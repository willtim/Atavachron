{-# LANGUAGE RecordWildCards #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

-- | A thread-pool using a blocking bounded queue.
--
module Atavachron.Executor
    ( Executor
    , Future(..)
    , new
    , shutdown
    , shutdownAndWait
    , submit_
    , submit
    , scheduleAtFixedRate
    ) where

import Control.Monad

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (Async)
import qualified Control.Concurrent.Async as Async

import Control.Concurrent.STM (atomically)
import Control.Concurrent.STM.TBQueue
import Control.Concurrent.MVar

import Numeric.Natural

import System.Mem (performGC)


-- | A pool of worker threads.
data Executor = Executor
    { exWorkers  :: [Async ()]
    , exTaskQ    :: TBQueue (Maybe (IO ()))
    }

newtype Future a = Future { futureWait :: IO a }

-- Create a linked async task.
-- Any exceptions thrown in the async will be re-thrown in the current thread
async :: IO a -> IO (Async a)
async m = do
    a <- Async.async m
    Async.link a
    return a

-- | Create a new Executor thread-pool.
new :: Natural -> Natural -> IO Executor
new groupSize queueSize = do
    taskQ <- atomically $ newTBQueue queueSize

    -- Create worker threads
    as <- forM [1..groupSize] $ \_ ->
              async $
                  let loop = do
                          t <- atomically (readTBQueue taskQ)
                          case t of
                              Nothing   -> return () -- die
                              Just task -> task >> performGC >> loop
                  in loop

    return $ Executor { exWorkers = as
                      , exTaskQ   = taskQ }

-- | Submit an IO effect for execution by the Executor.
submit_ :: Executor -> IO () -> IO ()
submit_ Executor{..} =
    atomically . writeTBQueue exTaskQ . Just -- blocks if queue is full

-- | Submit an effectful computation yielding a future for its value.
submit :: Executor -> IO a -> IO (Future a)
submit executor m = do
    var <- newEmptyMVar
    submit_ executor (m >>= putMVar var)
    return . Future $ takeMVar var

-- | Repeatedly run the task using the executor according to the supplied delay in milliseconds.
scheduleAtFixedRate :: Executor -> Int -> IO () -> IO ()
scheduleAtFixedRate executor delay_ms task =
    submit_ executor loop
  where
    loop = task >> threadDelay (delay_ms * 1000) >> submit_ executor loop

-- | Signal shutdown to workers, only blocks if the queue is full.
shutdown :: Executor -> IO ()
shutdown Executor{..} =
    -- Send finish messages
    atomically $
        mapM_ (const $ writeTBQueue exTaskQ Nothing) exWorkers

-- | Signal shutdown and block until all pending work is complete.
shutdownAndWait :: Executor -> IO ()
shutdownAndWait e@Executor{..} = do
    shutdown e
    mapM_ Async.wait exWorkers
