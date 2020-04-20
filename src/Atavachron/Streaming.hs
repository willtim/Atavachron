{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -fno-warn-orphans -fno-warn-name-shadowing #-}

-- | Streaming utilities.
--
module Atavachron.Streaming where

import Prelude hiding (concatMap)
import qualified Control.Arrow as Arr
import Control.Monad.Base
import Control.Monad.Trans.Resource

import Data.Either
import Data.Function
import Data.Maybe
import Data.Monoid
import qualified Data.Sequence as Seq
import Numeric.Natural

import Streaming
import Streaming.Prelude (yield, next)
import qualified Streaming.Prelude as S

import Atavachron.Executor (Executor)
import qualified Atavachron.Executor as Executor


-- | We typically do not vary the streamed functor
type Stream' a m r = Stream (Of a) m r


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

-- | merge stream of two outputs (@Either b b@) into one output.
mergeEither
  :: Monad m
  => Stream' (Either b b) m r -> Stream' b m r
mergeEither = S.map (either id id)

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

-- | As mapAccum, but does not return the final accumulation/state.
mapAccum_
    :: Monad m
    => (a -> b -> (a, c))
    -> a
    -> Stream (Of b) m r
    -> Stream (Of c) m r
mapAccum_ f a =
    fmap (\(_ :> r) -> r) . mapAccum f a
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
    fmap (\(_ :> r) -> r) . mapAccumM f a
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

forM :: Monad m => Stream' a m r -> (a -> m b) -> Stream' b m r
forM = flip S.mapM

-- | Decorate the input stream with a sequence number.
number :: (Monad m, Num i, Enum i) => Stream' a m r -> Stream' (i, a) m r
number = S.zip (S.iterate succ 0)

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
    => (a -> f (k, v))      -- ^ function to extract key/value pairs -- TODO move this function out?
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


-- | A simple parMap that reads ahead submitting concurrent tasks.
--
-- NOTE: Since we force the effects using the supplied buffer size,
-- the interleaving between output events and effects will be changed.
-- This is especially important to consider when the governing monad
-- is itself used to hold another stream of values which are interleaved
-- with the supplied stream.
parMap
    :: MonadIO m
    => Natural     -- ^ read-ahead buffer size
    -> Executor    -- ^ thread pool
    -> (a -> IO b)
    -> Stream' a m r
    -> Stream' b m r
parMap n e f
    = S.mapM (liftIO . Executor.futureWait)
    . evaluate n
    . S.mapM (liftIO . Executor.submit e . f)

-- | Evaluates the stream (forces the effects) ahead by @n@ items.
evaluate :: forall a m r. Monad m => Natural -> Stream' a m r -> Stream' a m r
evaluate n = buffer mempty
  where
    buffer !buf str = do
      e <- lift $ next str
      case e of
        Left r -> do
          S.each buf -- yield remaining
          return r
        Right (a,rest) ->
          case Seq.viewl buf of
            -- yield buffer head, only if buffer is full
            a' Seq.:< _ | Seq.length buf >= fromIntegral n -> do
              yield a'
              buffer (Seq.drop 1 buf Seq.|> a) rest
            _           ->
              buffer (buf Seq.|> a) rest
