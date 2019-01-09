{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PackageImports #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

-- | Functions for reading/writing files from various sources.
--

module Atavachron.Files where

import Prelude hiding (concatMap)

import Codec.Serialise

import Control.Lens (over)
import Control.Logging
import Control.Monad
import Control.Monad.Morph (hoist)
import Control.Monad.Reader
import Control.Monad.State.Class
import Control.Monad.Trans.Resource

import Data.Binary.Get
import Data.Binary.Put
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy.Char8 as L8
import qualified Data.Text as T

import qualified Streaming.Prelude as S hiding (mapM_)

import qualified System.Posix.Files.ByteString as Files
import qualified System.Posix.IO.ByteString as IO
import qualified System.Posix.Directory.ByteString as Dir
import System.Posix.Types

import Atavachron.Env
import Atavachron.Path
import Atavachron.Chunk.Builder
import Atavachron.Tree
import Atavachron.Streaming (Stream')
import qualified Atavachron.IO as IO
import qualified Atavachron.Streaming as S


-- | Note: since this is executed in lock-step with the file-read
-- stream, it is important that the writes are non-blocking.
writeCacheFile
    :: forall entry m r. (Serialise entry, MonadResource m)
    => Path Abs File
    -> Stream' entry m r
    -> m r
writeCacheFile file str = do
    let rfp = getRawFilePath file
    (key, fd) <- allocate (IO.openFd rfp IO.WriteOnly (Just Files.stdFileMode) IO.defaultFileFlags { IO.trunc = True })
                          (IO.closeFd)

    r <- S.mapM_ (liftIO . IO.fdPut fd) $ IO.buffer $ S.map serialiseEntry str
    release key
    return r
  where
    serialiseEntry :: entry -> B.ByteString
    serialiseEntry !e =
        let !pickled = serialise e
            !size    = fromIntegral $ L8.length pickled
        in L8.toStrict $ runPut (putInt32le size) <> pickled

readCacheFile :: (Serialise entry, MonadResource m) => Path Abs File -> Stream' entry m ()
readCacheFile file = do
    let rfp = getRawFilePath file
    (key, fd) <- lift $ allocate (IO.openFd rfp IO.ReadOnly (Just Files.stdFileMode) IO.defaultFileFlags)
                                 (IO.closeFd)
    loop fd
    release key
  where
    loop fd = do
        bs <- liftIO $ IO.fdGet fd 4
        unless (B.null bs) $ do
           let size = fromIntegral $ runGet getInt32le $ L8.fromStrict bs
           e    <- deserialise . L8.fromStrict <$> liftIO (IO.fdGet fd size)
           S.yield e
           loop fd

data Handle = Handle
  { hFile :: FileItem
  , hKey  :: ReleaseKey
  , hFd   :: Fd
  }

-- | Save files in the supplied target directory using the tagged
-- stream of bytestring chunks.
saveFiles
    :: forall m. MonadResource m
    => Stream' (Either (RawChunk FileItem B.ByteString) OtherItem) m ()
    -> m ()
saveFiles str = do
    open <- S.foldM_ step (return Nothing) return str
    case open of
        Just h  -> closeFile h
        Nothing -> return ()
  where
    step :: Maybe Handle -> (Either (RawChunk FileItem B.ByteString) OtherItem) -> m (Maybe Handle)
    step !open !e'item =
        case e'item of
            Left (RawChunk file bs) -> do
                open' <- doFileChunk file bs open
                return $ Just open'
            Right other -> do
                maybe (return ()) closeFile open
                case other of
                    DirItem FileMeta{..} -> liftIO $ do
                        Dir.createDirectory (getRawFilePath filePath) fileMode
                        fp <- getFilePath filePath
                        debug' $ "Wrote directory '" <> T.pack fp <> "'"
                    LinkItem FileMeta{..} target -> liftIO $ do
                        Files.createSymbolicLink target (getRawFilePath filePath)
                        fp <- getFilePath filePath
                        debug' $ "Wrote symbolic link '" <> T.pack fp <> "'"
                return Nothing

    doFileChunk file bs = \case
        Just h@Handle{..}
            | file == hFile -> liftIO (IO.fdPut hFd bs) >> return h
            | otherwise     -> closeFile h >> startNewFile file bs
        Nothing -> startNewFile file bs

    startNewFile file bs = do
        let rfp = getRawFilePath $ filePath file
        (key, fd) <- allocate (IO.openFd rfp IO.WriteOnly (Just $ fileMode file) IO.defaultFileFlags { IO.exclusive = True })
                              (IO.closeFd)
        liftIO $ IO.fdPut fd bs
        return $ Handle file key fd

    closeFile Handle{..} = do
        liftIO $ Files.setFdOwnerAndGroup hFd (fileUID hFile)   (fileGID hFile)
        liftIO $ Files.setFdTimesHiRes    hFd (fileATime hFile) (fileMTime hFile)
        release hKey
        fp <- liftIO $ getFilePath $ filePath hFile
        debug' $ "Wrote file '" <> T.pack fp <> "'"



-- | Produce a stream of file chunks from a stream of filenames.
-- NOTE: The chunk size is 1024K.
readFiles
    :: forall m r. (MonadResource m, MonadState Progress m)
    => Stream' FileItem m r
    -> Stream' (RawChunk FileItem B.ByteString) m r
readFiles = S.concatMap $ \file -> do
    modify $ over prFiles succ
    S.map (RawChunk file) $ readFileChunks file
  where
    readFileChunks :: FileItem -> Stream' B.ByteString m ()
    readFileChunks FileMeta{..} = do
        let !rfp = getRawFilePath filePath

        fp <- liftIO $ getFilePath filePath
        debug' $ "Reading file '" <> T.pack fp <> "'"

        (key, fd) <- allocate (IO.openFd rfp IO.ReadOnly (Just Files.stdFileMode) IO.defaultFileFlags)
                              (IO.closeFd)
        hoist liftIO $ IO.fdGetContents fd $ (fromIntegral $ fileSize + 1) `min` 1048576 -- 1MB chunk size
        release key
