{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Functions for reading/writing files.
--

module Atavachron.Files where

import Prelude hiding (concatMap)

import Control.Monad
import Control.Monad.Morph (hoist)
import Control.Monad.Reader
import Control.Monad.Trans.Resource

import qualified Data.ByteString as B
import qualified Data.Text as T
import qualified Streaming.Prelude as S hiding (mapM_)

import qualified System.Posix.Files.ByteString as Files
import qualified System.Posix.IO.ByteString as IO
import qualified System.Posix.Directory.ByteString as Dir
import System.Posix.Types

import Atavachron.Env
import Atavachron.Logging
import Atavachron.Path
import Atavachron.Chunk.Builder
import Atavachron.Tree
import Atavachron.Streaming (Stream')
import qualified Atavachron.IO as IO
import qualified Atavachron.Streaming as S

data Handle = Handle
  { hFile :: FileItem
  , hKey  :: ReleaseKey
  , hFd   :: Fd
  }

-- | Save files in the supplied target directory using the tagged
-- stream of bytestring chunks.
saveFiles
    :: forall m. MonadResource m
    => Stream' (RawChunk FileItem B.ByteString) m ()
    -> m ()
saveFiles str = do
    open <- S.foldM_ step (return Nothing) return str
    case open of
        Just h  -> closeFile h
        Nothing -> return ()
  where
    step :: Maybe Handle -> RawChunk FileItem B.ByteString -> m (Maybe Handle)
    step !open (RawChunk file bs) = do
        open' <- doFileChunk file bs open
        return $ Just open'

    doFileChunk file bs = \case
        Just h@Handle{..}
            | file == hFile -> liftIO (IO.fdPut hFd bs) >> return h
            | otherwise     -> closeFile h >> startNewFile file bs
        Nothing -> startNewFile file bs

    startNewFile file bs = do
        let rfp = getRawFilePath $ filePath file
        (key, fd) <- allocate (IO.openFd rfp IO.WriteOnly (Just $ fileMode file) IO.defaultFileFlags { IO.exclusive = True })
                              IO.closeFd
        liftIO $ IO.fdPut fd bs
        return $ Handle file key fd

    closeFile Handle{..} = do
        liftIO $ Files.setFdOwnerAndGroup hFd (fileUID hFile)   (fileGID hFile)
        liftIO $ Files.setFdTimesHiRes    hFd (fileATime hFile) (fileMTime hFile)
        release hKey
        fp <- liftIO $ getFilePath $ filePath hFile
        logDebug $ "Wrote file '" <> T.pack fp <> "'"

-- | Save non-regular files (links and directories) in the supplied target directory.
saveOther
    :: forall m. MonadResource m
    => Stream' OtherItem m ()
    -> m ()
saveOther =
    S.mapM_ step
  where
    step :: OtherItem -> m ()
    step !other =
        case other of
            DirItem FileMeta{..} -> liftIO $ do
                let rfp = getRawFilePath filePath
                exists <- Files.fileExist rfp
                -- do not error if a directory already exists
                unless exists $ do
                    Dir.createDirectory rfp fileMode
                    fp <- getFilePath filePath
                    logDebug $ "Wrote directory '" <> T.pack fp <> "'"
            LinkItem FileMeta{..} target -> liftIO $ do
                Files.createSymbolicLink target (getRawFilePath filePath)
                fp <- getFilePath filePath
                logDebug $ "Wrote symbolic link '" <> T.pack fp <> "'"

-- | Produce a stream of file chunks from a stream of filenames.
-- NOTE: The chunk size is 1024K.
readFiles
    :: forall i m r. (MonadResource m, MonadReader Env m)
    => Stream' (i, FileItem) m r
    -> Stream' (RawChunk i B.ByteString) m r
readFiles = S.concatMap $ \(i, file) -> do
    ref <- lift $ asks envProgressRef
    void $ updateProgress ref (mempty{prFiles=1})
    S.map (RawChunk i) $ readFileChunks file
  where
    readFileChunks :: FileItem -> Stream' B.ByteString m ()
    readFileChunks FileMeta{..} = do
        let !rfp = getRawFilePath filePath

        fp <- liftIO $ getFilePath filePath
        logDebug $ "Reading file '" <> T.pack fp <> "'"

        (key, fd) <- allocate (IO.openFd rfp IO.ReadOnly (Just Files.stdFileMode) IO.defaultFileFlags)
                              IO.closeFd
        hoist liftIO $ IO.fdGetContents fd $ (fromIntegral $ fileSize + 1) `min` 1048576 -- 1MB chunk size
        release key
