{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

-- | Functions and types for encoding binary chunks, e.g. hashing,
-- compression and encryption.

module Atavachron.Chunk.Encode where

import Codec.Serialise

import Control.Arrow
import Control.Monad.IO.Class

import Data.Maybe
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.ByteString as B
import qualified Data.ByteString.Base16 as Base16

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

type Offset = Int

newtype StoreID = StoreID { unStoreID :: B.ByteString }
    deriving (Generic, Eq)

instance Serialise StoreID

instance Show StoreID where
    show = T.unpack . hexEncode

newtype ChunkKey = ChunkKey { unChunkKey :: SecretBox.Key }

newtype StoreIDKey = StoreIDKey { unStoreIDKey :: Auth.Key }

instance Serialise ChunkKey where
    encode (ChunkKey k) = encode $ Saltine.encode k
    decode = maybe (fail "expected SecretBox.Key") (return . ChunkKey) . Saltine.decode =<< decode

instance Serialise StoreIDKey where
    encode (StoreIDKey k) = encode $ Saltine.encode k
    decode = maybe (fail "expected Auth.Key") (return . StoreIDKey) . Saltine.decode =<< decode


-- | Chunk metadata with a store ID and a sequence of tags,
-- representing zero or more boundaries in the chunk.
data Chunk t c = Chunk
  { cStoreID      :: !StoreID
  , cOffsets      :: !(TaggedOffsets t)
  , cOriginalSize :: !(Maybe Int) -- not known on decode
  , cContent      :: !c
  }

-- | A CipherText object contains an encrypted and authenticated secret box.
data CipherText = CipherText
  { cNonce     :: !SecretBox.Nonce
  , cSecretBox :: !B.ByteString
  } deriving Generic

type PlainChunk t = Chunk t B.ByteString
type CipherChunk t = Chunk t CipherText

instance Serialise CipherText

instance Serialise SecretBox.Key where
    encode n = encode $ Saltine.encode n
    decode = maybe (fail "expected SecretBox.Key") return . Saltine.decode =<< decode

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
hexEncode = T.decodeUtf8 . Base16.encode . unStoreID

-- | Decode a hexadecimal text string into a StoreID.
hexDecode :: Text -> StoreID
hexDecode = StoreID . fst . Base16.decode . T.encodeUtf8

-- | Calculate the storage IDs using a Hash message authentication code.
hashChunk :: StoreIDKey -> RawChunk (TaggedOffsets t) B.ByteString -> PlainChunk t
hashChunk key (RawChunk offsets bs) = Chunk (hashBytes key bs) offsets (Just $ B.length bs) bs

hashBytes :: StoreIDKey -> B.ByteString -> StoreID
hashBytes (StoreIDKey key) bytes = StoreID $ Saltine.encode $ Auth.auth key bytes

newtype VerifyFailed t = VerifyFailed { unVerifyFailed :: PlainChunk t }

-- | Use the Hash message authentication code to verify existing storage ID.
verify :: StoreIDKey -> PlainChunk t -> Either (VerifyFailed t) (PlainChunk t)
verify (StoreIDKey key) pc@Chunk{..} =
    let !hashResult = StoreID $ Saltine.encode $ Auth.auth key cContent
    in if hashResult == cStoreID
       then Right pc
       else Left (VerifyFailed pc)


encryptChunk :: MonadIO m => ChunkKey -> PlainChunk t -> m (CipherChunk t)
encryptChunk key c = do
    ciphertext <- encryptBytes (unChunkKey key) (cContent c)
    return $ c { cContent = ciphertext }

encryptBytes :: MonadIO m => SecretBox.Key -> B.ByteString -> m CipherText
encryptBytes key content = do
    nonce <- liftIO newNonce
    return $ CipherText
        { cNonce     = nonce
        , cSecretBox = SecretBox.secretbox key nonce content
        }

decryptChunk :: ChunkKey -> CipherChunk t -> Maybe (PlainChunk t)
decryptChunk key c = do
    plaintext <- decryptBytes (unChunkKey key) (cContent c)
    return $ c { cContent = plaintext }

decryptBytes :: SecretBox.Key -> CipherText -> Maybe B.ByteString
decryptBytes key CipherText{..} =
    SecretBox.secretboxOpen key cNonce cSecretBox

-- | Try LZ4 compression, which is extremely fast.
compressChunk :: PlainChunk t -> PlainChunk t
compressChunk c = c { cContent = compress (cContent c) }

decompressChunk :: PlainChunk t -> PlainChunk t
decompressChunk c = c { cContent = decompress (cContent c) }

compress :: B.ByteString -> B.ByteString
compress bs = fromMaybe bs $ LZ4.compress bs

decompress :: B.ByteString -> B.ByteString
decompress bs = fromMaybe bs $ LZ4.decompress bs


-- | Group and aggregate an input stream of tagged-offset/payload
-- pairs, such that the output stream emits a distinct event per tag
-- with a second aggregation computed over offsets using the supplied
-- function @f@.
groupByTag
    :: forall a b t m r . (Eq t, Monad m)
    => (Seq (a, Offset) -> b)            -- ^ aggregation function for offset/payload pairs
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
    . S.map (cOffsets &&& cContent)
  where
    step (!offsets, !content)
        = fmap (doOffset content)
          $ Seq.zip offsets
          $ Seq.drop 1 (fmap snd offsets) Seq.|> B.length content

    doOffset :: B.ByteString
             -> ((t, Offset), Offset)
             -> RawChunk t B.ByteString
    doOffset !bs !((tag, offset), offset') =
        let (_, bs') = B.splitAt offset             bs
            (bs'',_) = B.splitAt (offset' - offset) bs'
        in RawChunk tag bs''
