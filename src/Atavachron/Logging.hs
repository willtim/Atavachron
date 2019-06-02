-- | A wrapper around the underlying "fast-logger" logging implementation.
-- This module also contains stderr progress routines that will interleave
-- with stdout logging when both are directed to the console.
--
-- NOTE: all output is flushed upon every logging statement.
--

module Atavachron.Logging (
    logTrace
  , logDebug
  , logInfo
  , logWarn
  , logFatal
  , panic
  , putProgress
  , resetStdErrCursor
  , retainProgress
  , setLogLevel
  , withStdoutLogging
  , withFileLogging
  , LogLevel(..)
  ) where

import Control.Concurrent
import Control.Monad
import Control.Monad.IO.Class
import Data.IORef
import Data.Text (Text)
import qualified Data.Text as T
import Data.Time
import Data.Time.Locale.Compat (defaultTimeLocale)
import System.IO
import System.IO.Unsafe
import System.Log.FastLogger

data LogLevel
    = LevelTrace
    | LevelDebug
    | LevelInfo
    | LevelWarn
    | LevelFatal
    deriving (Eq, Show, Read, Ord)

logLevel :: IORef LogLevel
{-# NOINLINE logLevel #-}
logLevel = unsafePerformIO $ newIORef LevelInfo

loggerSet :: IORef LoggerSet
{-# NOINLINE loggerSet #-}
loggerSet = unsafePerformIO $
    newIORef (error "Logging must reside inside withStdoutLogging or withFileLogging")

stderrLock :: MVar ()
{-# NOINLINE stderrLock #-}
stderrLock = unsafePerformIO $ newMVar ()

logTimeFormat :: String
logTimeFormat = "%Y-%m-%dT%H:%M:%S"

setLogLevel :: LogLevel -> IO ()
setLogLevel = atomicWriteIORef logLevel

doLog :: LogLevel -> Text -> IO ()
doLog lvl msg = do
    maxLvl <- readIORef logLevel
    when (lvl >= maxLvl) $ do
        resetStdErrCursor
        now <- getCurrentTime
        let stamp = formatTime defaultTimeLocale logTimeFormat now
        set <- readIORef loggerSet
        pushLogStr set
             $ toLogStr (stamp ++ " " ++ renderLevel lvl ++ " ")
            <> toLogStr msg
            <> toLogStr "\n"
        flushLog

logTrace :: MonadIO m => Text -> m ()
logTrace = liftIO . doLog LevelTrace

logDebug :: MonadIO m => Text -> m ()
logDebug = liftIO . doLog LevelDebug

logInfo :: MonadIO m => Text -> m ()
logInfo = liftIO . doLog LevelInfo

logWarn :: MonadIO m => Text -> m ()
logWarn = liftIO . doLog LevelWarn

logFatal :: MonadIO m => Text -> m()
logFatal = liftIO . doLog LevelFatal

panic :: Text -> a
panic str = error $ unsafePerformIO (logFatal str) `seq` T.unpack str

withStdoutLogging :: MonadIO m => m a -> m a
withStdoutLogging m = do
    liftIO $ newStdoutLoggerSet defaultBufSize >>= atomicWriteIORef loggerSet
    m

withFileLogging :: MonadIO m => FilePath -> m a -> m a
withFileLogging fp m = do
    liftIO $ newFileLoggerSet defaultBufSize fp >>= atomicWriteIORef loggerSet
    m

flushLog :: MonadIO m => m ()
flushLog = liftIO $
    readIORef loggerSet >>= flushLogStr

renderLevel :: LogLevel -> String
renderLevel LevelTrace = "[TRACE]"
renderLevel LevelDebug = "[DEBUG]"
renderLevel LevelInfo  = "[INFO]"
renderLevel LevelWarn  = "[WARN]"
renderLevel LevelFatal = "[FATAL]"

-- | Write a progress reporting string to standard error after first clearing the line.
-- NOTE: Atavachron.Logging will also clear the stderr line before logging to prevent
-- garbled output when interleaved with stdout.
putProgress :: MonadIO m => String -> m ()
putProgress s = liftIO $ withMVar stderrLock (\_ -> hPutStr stderr $ "\r\ESC[K" ++ s)

-- | clear the stderr progress line
resetStdErrCursor :: MonadIO m => m ()
resetStdErrCursor = liftIO $ withMVar stderrLock (\_ -> hPutStr stderr "\r\ESC[K")

-- | Print a newline to always leave the last stderr progress line visible at the end
retainProgress :: MonadIO m => m ()
retainProgress = liftIO $ withMVar stderrLock (\_ -> hPutStr stderr "\n")
