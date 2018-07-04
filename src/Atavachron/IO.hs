{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | This module contains general purpose IO routines. In particular,
-- performant IO routines for ByteStrings that use raw file paths.

module Atavachron.IO where

import Control.Concurrent (threadDelay)
import Control.Logging
import Control.Monad
import Control.Monad.Catch
import Control.Monad.IO.Class

import Data.Foldable
import Data.Monoid
import qualified Data.ByteString as B
import qualified Data.ByteString.Internal as BSI

import qualified Data.Text as T

import Data.Sequence (Seq)
import qualified Data.Sequence as Seq

import Streaming.Prelude (yield, Of(..))
import qualified Streaming.Prelude as S
import qualified Atavachron.Streaming as S

import System.Posix.Types
import System.Random (randomRIO)

import qualified System.Posix.IO.ByteString as IO
import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Ptr (plusPtr)
import Foreign.C.Error

import Atavachron.Streaming

-- | This uses POSIX read.
fdGetContents
    :: Fd
    -> Int -- ^ chunk size
    -> Stream' B.ByteString IO ()
fdGetContents fd sz = loop
  where
    loop = do
        chunk <- liftIO $ fdGet fd sz
        let !size = B.length chunk
        yield $ chunk
        unless (size < sz) loop

-- | This uses POSIX read. EOF is indicated by returning a smaller/empty ByteString.
fdGet :: Fd -> Int -> IO B.ByteString
fdGet fd sz = BSI.createAndTrim sz $ \buf ->
                   fromIntegral <$> IO.fdReadBuf fd buf (fromIntegral sz)

-- | This uses POSIX write.
fdPut :: Fd -> B.ByteString -> IO ()
fdPut fd (BSI.PS ps s l) =
     withForeignPtr ps $ \p ->
        throwErrnoIf_ (/= fromIntegral l) "fdPut" $
            IO.fdWriteBuf fd (p `plusPtr` s) (fromIntegral l)

buffer :: Monad m => Stream' B.ByteString m r -> Stream' B.ByteString m r
buffer str = do
    (bss, _) :> r <- S.catMaybes $ S.mapAccum step (mempty, 0) str
    yield $ fold bss -- remaining
    return r
  where
    size = 32768 -- 32K

    step :: (Seq B.ByteString, Int)
         -> B.ByteString
         -> ((Seq B.ByteString, Int), Maybe B.ByteString)
    step (bss, len) bs
        | len' > size = ((mempty, 0),  Just $ fold bss')
        | otherwise   = ((bss', len'), Nothing)
      where
        bss' = bss Seq.|> bs
        len' = len + B.length bs

-- | Retry, if necessary, an idempotent action that is prone to
-- failure.  Exponential backoff and randomisation, ensure that we are
-- a well-behaved client to a remote service.
retryWithExponentialBackoff
    :: forall a. Int
    -> IO (Either SomeException a)
    -> IO (Either SomeException a)
retryWithExponentialBackoff retries m
  | retries < 0 = error "retryWithExponentialBackoff: retries must be a positive integer"
  | otherwise   = loop retries
  where
    loop :: Int -> IO (Either SomeException a)
    loop n = do
      res <- m
      case res of
          -- failure
          Left ex
              | n > 0     -> do -- backoff before retrying/looping
                    r <- randomRIO (1 :: Double, 1 + randomisation)
                    let interval = r * initTimeout * multiplier ^ n -- seconds

                    warn' $ T.pack (show ex) <> ". Retrying..."

                    delay (floor $ interval * 1e6) -- argument is microseconds
                    loop (n - 1)
              | otherwise ->    -- give up
                    return $ Left ex
          -- success!
          Right x         -> return $ Right x

    initTimeout   = 0.5 -- seconds
    multiplier    = 1.5
    randomisation = 0.5

    delay :: Integer -> IO ()
    delay time = do
        let maxWait = min time $ toInteger (maxBound :: Int)
        threadDelay $ fromInteger maxWait
        when (maxWait /= time) $ delay (time - maxWait)
