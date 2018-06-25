{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TupleSections #-}

-- | Simple and efficient content-derived chunking (CDC) using
-- hashing by Cyclic Polynomials (also known as Buzhash).
--
-- Reference:
-- Daniel Lemire, Owen Kaser "Recursive n-gram hashing is pairwise
-- independent, at best", Computer Speech & Language, Volume 24, Issue
-- 4, October 2010, Pages 698-710. http://arxiv.org/abs/0705.4676
--
-- This rolling hash is used only to slice the input streams into chunks
-- at points defined by the content.
--
-- Default parameters: We use a sliding window of size 64 bytes.
-- Chunks range from 512 KB to 8 MB in size; aiming for a 1 MB chunk
-- size on average.

module Atavachron.Chunk.CDC
    ( CDCKey
    , CDCParams(..)
    , newCDCKey
    , defaultCDCParams
    , rechunkCDC
    , split
    , slowSplit
    , genHVs
    ) where

import Control.Monad.State

import Codec.Serialise

import Data.Bits
import Data.Word
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString.Unsafe as B

import Streaming.Prelude (yield)

import Data.Vector.Unboxed (Vector, (!))
import qualified Data.Vector.Unboxed as VU

import qualified Crypto.Saltine.Class as Saltine
import qualified Crypto.Saltine.Core.Hash as Hash

import Atavachron.Chunk.Builder
import Atavachron.Streaming

import GHC.Generics (Generic)


type Hash = Word64

newtype CDCKey = CDCKey Hash.ShorthashKey
    deriving (Eq)

newCDCKey :: IO CDCKey
newCDCKey = CDCKey <$> Hash.newShorthashKey

instance Serialise CDCKey where
    encode (CDCKey k) = encode $ Saltine.encode k
    decode = maybe (fail "expected ShorthashKey") (return . CDCKey) . Saltine.decode =<< decode

data CDCParams = CDCParams
  { cpLog2AvgSize :: !Int -- ^ log2 of desired average chunk size
  , cpLog2MinSize :: !Int -- ^ log2 of minimum chunk size
  , cpLog2MaxSize :: !Int -- ^ log2 of maximum chunk size
  , cpWinSize     :: !Int -- ^ window size in bytes, must be smaller than minSize
  } deriving (Generic)

instance Serialise CDCParams

defaultCDCParams :: CDCParams
defaultCDCParams = CDCParams
  { cpLog2AvgSize = 20 -- 1MB
  , cpLog2MinSize = 19 -- 512KB
  , cpLog2MaxSize = 23 -- 8MB
  , cpWinSize     = 64
  }

-- | Re-chunk the incoming tagged chunks using a rolling hash.
-- NOTES:
-- * We grow the supplied builder until we have maxSize
-- * maxSize must be greater than winSize
rechunkCDC
    :: (Eq t, Monad m)
    => CDCKey
    -> CDCParams
    -> Stream' (RawChunk t B.ByteString) m r                 -- ^ stream of tagged (small) bytestrings
    -> Stream' (RawChunk (TaggedOffsets t) B.ByteString) m r -- ^ stream of CDC chunks with boundary offsets
rechunkCDC cdcKey CDCParams{..} = loop mempty Nothing
  where
    loop !builder !currentTag !str = do
        (builder, currentTag, res) <- lift $ buildToSize builder maxSize currentTag str
        let !inChunk  = toByteString builder
            !tags     = bTaggedOffsets builder
        builder'  <- case split hvs cpWinSize minSize cpLog2AvgSize inChunk of
                        (outChunk, remChunk)
                            | B.null outChunk ->
                                  -- supplied bytestring was too small
                                  return $ fromByteString remChunk tags
                            | otherwise    -> do
                                  -- split offset metadata, if there is any
                                  let !(outTags, remTags) = splitTaggedOffsets (B.length outChunk) tags
                                  yield $ RawChunk outTags outChunk
                                  return $ fromByteString remChunk remTags
        case res of
            Left r      -> do -- yield last leftover, if necessary
                when (bSize builder' > 0 || length (bTaggedOffsets builder') > 0) $
                    yield $ RawChunk (bTaggedOffsets builder') (toByteString builder')
                return r
            Right str'  ->    -- continue
                loop builder' currentTag str'

    hvs     = genHVs cdcKey
    minSize = shiftL 1 cpLog2MinSize
    maxSize = shiftL 1 cpLog2MaxSize


-- | Attempt to split a ByteString into a chunk and a remainder.
-- NOTES:
-- * To get a strongly universal hash value, you have to ignore
--   the last or first (windowSize-1) bits.
-- * If the supplied chunk is too small, we return an empty bytestring
--   and the supplied bytestring as the remainder.
-- * If we fail to split the supplied bytestring, we return the
--   the whole lot and an empty remainder. Hence the supplied bytestring
--   should be our maximum chunk size.
split
    :: Vector Hash -- ^ Table of random hash values for each Word8
    -> Int         -- ^ window size
    -> Int         -- ^ minimum output chunk size in bytes
    -> Int         -- ^ log2 of desired average output chunk size
    -> B.ByteString
    -> (B.ByteString, B.ByteString)
split hvs winSize minSize log2AvgSize bs
    | winSize > minSize = error "Assertion failed: winSize > minSize"
    | B.length bs < minSize = (B.empty, bs)
    | otherwise =
      loop 0
         $ initHash hvs
         $ B.unsafeTake winSize bs
  where
    loop !start !_hash
        | start >= end = (bs, B.empty)
    loop !start !hash
        | let !i = start + winSize
        , (mask == (hash .&. mask) && i >= minSize) = B.splitAt i bs
    loop !start !hash =
       let !old8  = B.unsafeIndex bs start
           !new8  = B.unsafeIndex bs (start + winSize)
           !hash' = updateHash hvs winSize old8 new8 hash
       in loop (start + 1) hash'

    mask    = shiftL 1 log2AvgSize - 1
    end     = B.length bs - winSize

-- | Computes the same as 'split' using only 'initHash' (and is therefore very slow).
-- Used by the QuickCheck tests.
slowSplit
    :: Vector Word64
    -> Int         -- ^ window size
    -> Int         -- ^ minimum output chunk size in bytes
    -> Int         -- ^ log2 of desired average output chunk size
    -> B.ByteString
    -> (B.ByteString, B.ByteString)
slowSplit _hvs _winSize minSize _log2AvgSize bs
    | B.length bs < minSize = (mempty, bs)
slowSplit hvs winSize minSize log2AvgSize bs = loop 0
  where
    loop !start =
        let !bs'  = B.take winSize $ B.drop start bs
            !hash = initHash hvs bs'
            !i    = start + winSize
        in case () of
            _ | B.length bs' < winSize   -> (bs, B.empty)
              | mask == (mask .&. hash)
                && i >= minSize          -> B.splitAt i bs
              | otherwise                -> loop (start+1)

    mask = shiftL 1 log2AvgSize - 1

updateHash :: Vector Hash -> Int -> Word8 -> Word8 -> Hash -> Hash
updateHash !hvs !winSize !old8 !new8 !hash =
    rotateL hash 1 `xor` z `xor` lookupHV new8 hvs
  where
    z = rotateL (lookupHV old8 hvs) winSize
{-# INLINE updateHash #-}

-- | Initialise the hash using the supplied ByteString.
initHash :: Vector Hash -> B.ByteString -> Hash
initHash hvs = B.foldl eat 0
  where
    eat :: Hash -> Word8 -> Hash
    eat !hash !w8 = rotateL hash 1 `xor` lookupHV w8 hvs

lookupHV :: Word8 -> Vector Hash -> Hash
lookupHV k hvs = hvs ! fromEnum k
{-# INLINE lookupHV #-}

genHVs :: CDCKey -> Vector Hash
genHVs (CDCKey key) = VU.generate 256 f
  where
    f :: Int -> Word64
    f = toWord64 . Hash.shorthash key . B8.singleton . toEnum

    toWord64 :: B.ByteString -> Word64
    toWord64 = B.foldl' (\b a -> rotateL b 8 + fromIntegral a) 0
