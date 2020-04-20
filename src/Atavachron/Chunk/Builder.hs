{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE TupleSections #-}
{-# OPTIONS_GHC -Wno-name-shadowing #-}

-- | Builder is a wrapper around a ByteString builder containing
-- tagged offsets.

module Atavachron.Chunk.Builder
    ( TaggedOffsets
    , RawChunk(..)
    , Builder(..)
    , fromByteString
    , toByteString
    , splitTaggedOffsets
    , setZeroOffset
    , getLastTag
    , buildToSize
    ) where

import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Builder as BB

import Streaming.Prelude (next)
import qualified Streaming.Prelude as S

-- import Data.Semigroup (Semigroup (..))
import Data.Sequence (Seq)
import qualified Data.Sequence as Seq

import Atavachron.Streaming

type TaggedOffsets t = Seq (t, Int)

-- Represents a (tagged) fragment of data
data RawChunk t a = RawChunk { rcTag :: !t, rcData :: !a }
  deriving (Eq, Show)

data Builder t = Builder
  { bBuilder       :: !BB.Builder
  , bTaggedOffsets :: !(TaggedOffsets t)
  , bSize          :: !Int
  }

instance Semigroup (Builder t) where
  Builder bb1 ts1 s1 <> Builder bb2 ts2 s2 =
    Builder (bb1 <> bb2) (ts1 <> ts2) (s1 + s2)

instance Monoid (Builder t) where
    mempty = Builder mempty mempty 0
    bb1 `mappend` bb2 = bb1 <> bb2

toByteString :: Builder t -> B.ByteString
toByteString = L.toStrict . BB.toLazyByteString . bBuilder

fromByteString :: B.ByteString -> Seq (t, Int) -> Builder t
fromByteString bs taggedOffsets =
    Builder (BB.byteString bs) taggedOffsets (B.length bs)
{-# INLINE fromByteString #-}

addTaggedChunk :: Builder t -> t -> B.ByteString -> Builder t
addTaggedChunk bb tag bs = bb <> fromByteString bs (Seq.singleton (tag, bSize bb))
{-# INLINE addTaggedChunk #-}

addChunk :: Builder t -> B.ByteString -> Builder t
addChunk bb bs = bb <> fromByteString bs mempty
{-# INLINE addChunk #-}

-- | Split the tagged offset sequence at an arbitrary boundary.
-- For example:
-- [(a, 0)]          -- split 0 --> ([], [(a, 0)])
-- [(a, 0)]          -- split 5 --> ([(a, 0)], [])
-- [(a, 0), (b, 5)]  -- split 5 --> ([(a, 0)], [(b, 0)])
-- [(a, 0), (b, 10)] -- split 5 --> ([(a, 0)], [(b, 5)])
splitTaggedOffsets :: Int -> TaggedOffsets t -> (TaggedOffsets t, TaggedOffsets t)
splitTaggedOffsets i ts = (l, shift r)
  where
    (!l, !r) = Seq.breakl (\(_, offset) -> offset >= i) ts
    shift    = fmap (\(tag, j) -> (tag, j-i))

-- | add currentTag explicitly as first zero offset, if necessary
setZeroOffset :: Maybe t -> TaggedOffsets t -> TaggedOffsets t
setZeroOffset currentTag offsets =
    case Seq.viewl offsets of
        (_, 0) Seq.:< _ -> offsets
        _               -> zero <> offsets
  where
    zero = maybe mempty (Seq.singleton . (,0)) currentTag

-- | get the last tag used, if there is one.
getLastTag :: Maybe t -> TaggedOffsets t -> Maybe t
getLastTag currentTag offsets =
    case Seq.viewr offsets of
        _ Seq.:> (t, _) -> Just t
        _               -> currentTag

-- NOTE: when chunks are added with the same tag, no additional offset metadata is added.
buildToSize
    :: (Eq t, Monad m)
    => Builder t                             -- ^ starting builder
    -> Int                                   -- ^ the target size
    -> Maybe t                               -- ^ current tag, if there is one
    -> Stream' (RawChunk t B.ByteString) m r -- ^ stream of tagged chunks to consume
    -> m ( Builder t
         , Maybe t
         , Either r (Stream' (RawChunk t B.ByteString) m r)
         )
buildToSize bb targetSize currentTag str
  | bSize bb >= targetSize = error "buildToSize: Assertion failed!" -- nothing to do!
  | otherwise = loop bb currentTag str
    where
      loop !bb !currentTag !str = do
          res <- next str
          case res of
              Left r -> return (bb, currentTag, Left r)
              Right (RawChunk tag bs, str') ->
                  let add | Just tag == currentTag = addChunk bb
                          | otherwise = addTaggedChunk bb tag
                  in case () of
                      _ | bSize bb + B.length bs >= targetSize ->
                          let (!bs', !bs'') = B.splitAt (targetSize - bSize bb) bs
                          in return ( add bs'
                                    , Just tag
                                    , Right $ S.cons (RawChunk tag bs'') str')
                        | otherwise ->
                          loop (add bs) (Just tag) str'
