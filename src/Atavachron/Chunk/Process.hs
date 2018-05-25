{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}


-- | Functions and types for processing chunks, e.g. hashing,
-- compression and encryption.

module Atavachron.Chunk.Process where

import Codec.Serialise

import Control.Arrow
import Control.Monad.State

import Data.Maybe
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy.Char8 as L8
import qualified Data.ByteString.Builder as Builder

import Data.Sequence (Seq)
import qualified Data.Sequence as Seq

import qualified Streaming.Prelude as S

import qualified Codec.Compression.LZ4 as LZ4
import qualified Crypto.Saltine.Class as Saltine
import Crypto.Saltine.Core.SecretBox as SecretBox
import Crypto.Saltine.Core.Auth as Auth

import Atavachron.Chunk.Builder
import Atavachron.Streaming (Stream')
import qualified Atavachron.Streaming as S

import GHC.Generics (Generic)


newtype StoreID = StoreID { unStoreID :: B.ByteString }
    deriving (Generic, Eq, Show, Read)

instance Serialise StoreID

newtype ChunkKey = ChunkKey { unChunkKey :: SecretBox.Key }

newtype StoreIDKey = StoreIDKey { unStoreIDKey :: Auth.Key }

instance Serialise ChunkKey where
    encode (ChunkKey k) = encode $ Saltine.encode k
    decode = maybe (fail "expected SecretBox.Key") (return . ChunkKey) . Saltine.decode =<< decode

instance Serialise StoreIDKey where
    encode (StoreIDKey k) = encode $ Saltine.encode k
    decode = maybe (fail "expected Auth.Key") (return . StoreIDKey) . Saltine.decode =<< decode


-- | Plaintext chunks with a store ID and a sequence of tags,
-- representing zero or more boundaries in the chunk.
data PlainChunk t = PlainChunk
  { pcStoreID :: !StoreID
  , pcOffsets :: !(TaggedOffsets t)
  , pcContent :: !B.ByteString
  } deriving Show


-- | A Cipher chunk contains an encrypted and authenticated secret box.
data CipherChunk t = CipherChunk
  { ccStoreID   :: !StoreID
  , ccOffsets   :: !(TaggedOffsets t)
  , ccNonce     :: !SecretBox.Nonce
  , ccSecretBox :: !B.ByteString
  }

instance Serialise SecretBox.Nonce where
    encode n = encode $ Saltine.encode n
    decode = maybe (fail "expected SecretBox.Nonce") return . Saltine.decode =<< decode

-- | Create a new random chunk key, used for encrypting chunks.
newChunkKey :: IO ChunkKey
newChunkKey = ChunkKey <$> SecretBox.newKey

-- | Create a new random store ID key, used for hashing plaintext chunks.
newStoreIDKey :: IO StoreIDKey
newStoreIDKey = StoreIDKey <$> Auth.newKey

-- | Encode a StoreID as a hexadecimal text string.
hexEncode :: StoreID -> Text
hexEncode = T.pack . L8.unpack . Builder.toLazyByteString . Builder.byteStringHex . unStoreID

-- | Calculate the storage IDs using a Hash message authentication code.
hash :: StoreIDKey -> RawChunk (TaggedOffsets t) B.ByteString -> PlainChunk t
hash (StoreIDKey key) (RawChunk offsets bs) =
    let !storageID = StoreID $ Saltine.encode $ Auth.auth key bs
    in PlainChunk storageID offsets bs


newtype VerifyFailed t = VerifyFailed { unVerifyFailed :: PlainChunk t }

-- | Use the Hash message authentication code to verify existing storage ID.
verify :: StoreIDKey -> PlainChunk t -> Either (VerifyFailed t) (PlainChunk t)
verify (StoreIDKey key) pc@PlainChunk{..} =
    let !hashResult = StoreID $ Saltine.encode $ Auth.auth key pcContent
    in if hashResult == pcStoreID
       then Right pc
       else Left (VerifyFailed pc)


encrypt :: MonadIO m => ChunkKey -> PlainChunk t -> m (CipherChunk t)
encrypt (ChunkKey key) PlainChunk{..} = do
    nonce <- liftIO newNonce
    return $ CipherChunk
        { ccStoreID   = pcStoreID
        , ccOffsets   = pcOffsets
        , ccNonce     = nonce
        , ccSecretBox = SecretBox.secretbox key nonce pcContent
        }


decrypt :: ChunkKey -> CipherChunk t -> Maybe (PlainChunk t)
decrypt (ChunkKey key) CipherChunk{..} =
    PlainChunk ccStoreID ccOffsets
        <$> SecretBox.secretboxOpen key ccNonce ccSecretBox


-- | Try LZ4 compression, which is extremely fast.
compress :: PlainChunk t -> PlainChunk t
compress c =
    c { pcContent = fromMaybe (pcContent c) $ LZ4.compress (pcContent c) }


decompress :: PlainChunk t -> PlainChunk t
decompress c =
    c { pcContent = fromMaybe (pcContent c) $ LZ4.decompress (pcContent c) }


-- | Group and aggregate an input stream of tagged-offset/payload
-- pairs, such that the output stream emits a distinct event per tag
-- with a second aggregation computed over offsets using the supplied
-- function @f@.
groupByTag
    :: forall a b t m r . (Eq t, Monad m)
    => (Seq (a, Int) -> b)               -- ^ aggregation function for offset/payload pairs
    -> Stream' (TaggedOffsets t, a) m r  -- ^ input is tagged offsets with a payload
    -> Stream' (t, b) m r                -- ^ output is one event per tag with aggregate
groupByTag f
  = S.map (second f)
  . S.aggregateByKey extractTags
  . S.mapAccum_ addZeroOffsetTags Nothing
  where
    extractTags :: (TaggedOffsets t, a) -> Seq (t, Seq (a, Int))
    extractTags (offsets, a) =
        flip fmap offsets $ \(t, offset) ->
            (t, Seq.singleton (a, offset))


-- | By default, we don't mark the start of a new chunk with the
-- previous seen tag and a zero offset.  This function adds them
-- explicitly and simplifies some operations, such as @rechunkToTags@
-- and @groupByTag@.
addZeroOffsetTags :: Maybe t -> (TaggedOffsets t, a) -> (Maybe t, (TaggedOffsets t, a))
addZeroOffsetTags !currentTag (!offsets, !a) =
    -- add last tag explicitly as zero offset, if necessary
    let !offsets'    = setZeroOffset currentTag offsets
        !currentTag' = getLastTag currentTag offsets
    in (currentTag', (offsets', a))
{-# INLINE addZeroOffsetTags #-}

-- NOTES:
-- 1. This will not recreate the same input chunk sizes originally fed to the CDC chunker.
-- 2. Subsequent offsets are used as end-markers, so if they are not provided
--    (e.g. a partial restore), the final raw chunk for each tag will need to be trimmed after
--    calling this function using size metadata in the concrete tag.
rechunkToTags
    :: forall t m r. (Eq t, Monad m)
    => Stream' (PlainChunk t) m r
    -> Stream' (RawChunk t B.ByteString) m r
rechunkToTags
    = S.concat
    . S.map step
    . S.mapAccum_ addZeroOffsetTags Nothing
    . S.map (pcOffsets &&& pcContent)
  where
    step (!offsets, !content)
        = fmap (doOffset content)
          $ Seq.zip offsets
          $ Seq.drop 1 (fmap snd offsets) Seq.|> B.length content

    doOffset :: B.ByteString
             -> ((t, Int), Int)
             -> RawChunk t B.ByteString
    doOffset !bs !((tag, offset), offset') =
        let (_, bs') = B.splitAt offset             bs
            (bs'',_) = B.splitAt (offset' - offset) bs'
        in RawChunk tag bs''
