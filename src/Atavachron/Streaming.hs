{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE ViewPatterns #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

-- | Streaming utilities
--
module Atavachron.Streaming where

import Prelude hiding (concatMap)
import qualified Control.Arrow as Arr
import Control.Monad as M
import Control.Monad.Base
import Control.Monad.Trans.Resource

import Control.Concurrent.Async
import Control.Concurrent.QSem

import Data.Either
import Data.Function
import Data.Maybe
import Data.Monoid
import qualified Data.Sequence as Seq

import Streaming
import Streaming.Prelude (yield, next)
import qualified Streaming.Prelude as S

-- | We typically do not vary the streamed functor
type Stream' a m r = Stream (Of a) m r

-- | A stream transformation
type StreamF a b m r = Stream' a m r -> Stream' b m r

-- | Represents a bounded group of concurrent tasks
newtype TaskGroup = TaskGroup { getQSem :: QSem }

-- | Create a new task group with the supplied maximum number
-- of workers
mkTaskGroup :: Int -> IO TaskGroup
mkTaskGroup size = TaskGroup <$> newQSem size

-- | Create a new async task using the supplied task group. The
-- spawned task will be blocked if we have already reached the maximum
-- number of concurrent tasks for this task group.
asyncWithGroup :: TaskGroup -> IO a -> IO (Async a)
asyncWithGroup (TaskGroup q) m = async (waitQSem q >> m <* signalQSem q)

-- | A simple parMap that reads ahead creating async tasks.
--
-- NOTE: Since we force the effects using the supplied buffer size,
-- the interleaving between output events and effects will be changed.
-- This is especially important to consider when the governing monad
-- is itself used to hold another stream of values which are interleaved
-- with the supplied stream.
parMap
    :: MonadIO m
    => Int          -- ^ read-ahead buffer size
    -> TaskGroup    -- ^ available threads
    -> (a -> IO b)
    -> Stream' a m r
    -> Stream' b m r
parMap n g f
    = S.mapM (liftIO . wait)
    . evaluate n
    . S.mapM (liftIO . asyncWithGroup g . f)

-- | Evaluates the stream (forces the effects) ahead by @n@ items.
evaluate :: forall a m r. Monad m => Int -> Stream' a m r -> Stream' a m r
evaluate n = buffer mempty
  where
    buffer !buf str = do
      e <- lift $ next str
      case e of
        Left r -> do
          S.each buf -- yield remaining
          return r
        Right (a,rest) -> do
          case Seq.viewl buf of
            -- yield buffer head, only if buffer is full
            a' Seq.:< _ | Seq.length buf >= n -> do
              yield a'
              buffer (Seq.drop 1 buf Seq.|> a) rest
            _           ->
              buffer (buf Seq.|> a) rest

-- | maps over left values, leaving right values unchanged.
left
  :: Monad m
  => (Stream' a (Stream (Of c) m) r -> Stream' b (Stream (Of c) m) r)
  -> Stream' (Either a c) m r
  -> Stream' (Either b c) m r
left f = S.maps S.sumToEither
       . S.unseparate . f . S.separate
       . S.maps S.eitherToSum

-- | maps over right values, leaving left values unchanged.
right
  :: Monad m
  => (Stream' a (Stream (Of c) m) r -> Stream' b (Stream (Of c) m) r)
  -> Stream' (Either c a) m r
  -> Stream' (Either c b) m r
right f = S.map swap . left f . S.map swap
  where
    swap = either Right Left

-- | apply a partial transformation to a stream which already might contain errors.
bind
  :: Monad m
  => (Stream' a (Stream (Of e) m) r -> Stream' (Either e b) (Stream (Of e) m) r)
  -> Stream' (Either e a) m r
  -> Stream' (Either e b) m r
bind f = S.map join . right f


-- | filter out right values.
lefts
  :: Monad m
  => Stream' (Either a b) m r
  -> Stream' a m r
lefts = S.map (fromLeft (error "lefts: assertion failed"))
      . S.filter isLeft

-- | filter out left values.
rights
  :: Monad m
  => Stream' (Either a b) m r
  -> Stream' b m r
rights = S.map (fromRight (error "rights: assertion failed"))
       . S.filter isRight

-- | merge stream of two outputs into one output.
merge
  :: Monad m
  => Stream' (Either b b) m r -> Stream' b m r
merge = S.map (either id id)

-- | The interleaving of items after using e.g. @left@ depends on the
-- chunking when using non-synchronous streams.  This code buffers the
-- right items accordingly to ensure correct ordering.
--
-- NOTE: This direct implementation turned out to be much
-- faster than one based on S.concat and S.mapAccum.
reinterleaveRights
    :: (Ord c, Monad m)
    => (a -> c)
    -> (b -> c)
    -> Stream' (Either a b) m r
    -> Stream' (Either a b) m r
reinterleaveRights f g = loop Seq.empty
  where
    loop !items !str = do
        res <- lift $ next str
        case res of
            Left r -> do
                S.each $ fmap Right items
                return r
            Right (e'item, str') ->
                case e'item of
                    l@(Left item) -> do
                        let !predicate = (/=GT) . (flip compare $ f item) . g
                            (!pending, !items') = Seq.spanl predicate items
                        S.each $ fmap Right pending Seq.|> l
                        loop items' str'
                    Right nrItem ->
                        loop (items Seq.|> nrItem) str'


-- | Apply a function to every element in the stream, while threading
-- an accumulator/state.
mapAccum
    :: Monad m
    => (a -> b -> (a, c))
    -> a
    -> Stream (Of b) m r
    -> Stream (Of c) m (Of a r)
mapAccum f = loop
  where
    loop !accum !str = do
        res <- lift $ next str
        case res of
            Left r          -> return $ accum :> r
            Right (b, str') -> do
                let (accum', !c) = f accum b
                yield c
                loop accum' str'
{-# INLINE mapAccum #-}

-- | As mapAccum, but does return the final accumulation/state.
mapAccum_
    :: Monad m
    => (a -> b -> (a, c))
    -> a
    -> Stream (Of b) m r
    -> Stream (Of c) m r
mapAccum_ f a =
    liftM (\(_ :> r) -> r) . mapAccum f a
{-# INLINE mapAccum_ #-}

-- | Apply a monadic function to every element in the stream, while
-- threading an accumulator/state.
mapAccumM
    :: Monad m
    => (a -> b -> m (a, c))
    -> a
    -> Stream (Of b) m r
    -> Stream (Of c) m (Of a r)
mapAccumM f = loop
  where
    loop !accum !str = do
        res <- lift $ next str
        case res of
            Left r          -> return $ accum :> r
            Right (b, str') -> do
                (accum', !c) <- lift $ f accum b
                yield c
                loop accum' str'
{-# INLINE mapAccumM #-}

-- | As mapAccumM, but does return the final accumulation/state.
mapAccumM_
    :: Monad m
    => (a -> b -> m (a, c))
    -> a
    -> Stream (Of b) m r
    -> Stream (Of c) m r
mapAccumM_ f a =
    liftM (\(_ :> r) -> r) . mapAccumM f a
{-# INLINE mapAccumM_ #-}


-- | NOTE: This appears to be orders-of-magnitude faster than using the
-- more general @S.for@ provided by "streaming".
concatMap :: Monad m => (a -> Stream' b m x) -> Stream' a m r -> Stream' b m r
concatMap f = loop
  where
    loop !str = do
        res <- lift $ next str
        case res of
            Left r -> return r
            Right (x, str') -> f x >> loop str'
{-# INLINE concatMap #-}

-- | NOTE: This appears to be faster than @mapM_@ provided by "streaming".
mapM_ :: Monad m => (a -> m b) -> Stream' a m r -> m r
mapM_ f = loop
  where
    loop !str = do
        res <- next str
        case res of
            Left r -> return r
            Right (x, str') -> f x >> loop str'
{-# INLINE mapM_ #-}


-- | Extract out key/value pairs from the input stream, then
-- group by key and aggregate the values.
--
-- For each element in the input stream:
-- ... a1, a2, a3 ...
-- We extract key/value pairs using @f@:
-- ... [(k1, v1), (k2, v2)], [(k2, v3)], ...
-- Then we flatten:
-- ... (k1, v1), (k2, v2), (k2, v3), ...
-- Then we group by the key:
-- ... [(k1, v1)], [(k2, v2), (k2, v3)] ...
-- Then we aggregate the values:
-- ... (k1, v1), (k2, v2 <> v3) ...
--
-- NOTE: This implementation avoids the slow performing @S.concat@.
aggregateByKey
    :: (Monad m, Eq k, Monoid v, Functor f, Foldable f)
    => (a -> f (k, v))      -- ^ function to extract key/value pairs
    -> Stream' a m r        -- ^ input stream
    -> Stream' (k, v) m r   -- ^ output stream of key/value pairs
aggregateByKey f =
    S.map (Arr.first $ fromJust . getFirst)
  . S.mapped S.mconcat                           -- aggregate values
  . S.groupBy ((==) `on` fst)                    -- group by key
  . concatMap S.each                             -- flatten
  . S.map (fmap (Arr.first (First . Just)) . f)  -- extract key/value pairs

instance (MonadBase b m, Functor f) => MonadBase b (Stream f m) where
  liftBase  = effect . fmap return . liftBase
  {-#INLINE liftBase #-}

instance (MonadThrow m, Functor f) => MonadThrow (Stream f m) where
  throwM = lift . throwM
  {-#INLINE throwM #-}

instance (MonadResource m, Functor f) => MonadResource (Stream f m) where
    liftResourceT = lift . liftResourceT
