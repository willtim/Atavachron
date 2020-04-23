{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ViewPatterns #-}

-- | Local filesystem Store, intended mainly for testing.
--
module Atavachron.Store.LocalFS where

import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.Resource

import Data.Time
import qualified Data.Text as T

import qualified Data.ByteString.Lazy as LB
import qualified System.Directory as Dir
import System.FilePath

import Atavachron.Path (Path, Abs, Dir, getFilePath)
import Atavachron.Store (Store(..))
import qualified Atavachron.Store as Store
import Atavachron.Streaming (Stream')
import qualified Streaming.Prelude as S


newLocalFS :: Path Abs Dir -> IO Store
newLocalFS root = do
    name <- T.pack . ("file://" <>) <$> getFilePath root
    return $ Store{..}

  where

    list :: Store.Path -> Stream' Store.Key (ResourceT IO) ()
    list path@(Store.Path p) = do
        liftIO $ ensureSubdir path
        dir <- liftIO $ T.pack . (</> T.unpack p) <$> getFilePath root
        listFilesRecursively (Store.Path dir)

    get :: Store.Key -> IO LB.ByteString
    get key = do
        fileName <- keyToFileName key
        LB.readFile fileName

    put :: Store.Key -> LB.ByteString -> IO ()
    put key bs = do
        ensureSubdir $ Store.kPath key
        fileName <- keyToFileName key
        LB.writeFile fileName bs

    hasKey :: Store.Key -> IO Bool
    hasKey key = do
        fileName <- keyToFileName key
        Dir.doesFileExist fileName

    move :: Store.Key -> Store.Key -> IO ()
    move src dest = do
        ensureSubdir $ Store.kPath dest
        srcFile  <- keyToFileName src
        destFile <- keyToFileName dest
        Dir.renameFile srcFile destFile

    delete :: [Store.Key] -> IO ()
    delete keys = do
        fileNames <- mapM keyToFileName keys
        mapM_ Dir.removeFile fileNames

    modTime :: Store.Key -> IO UTCTime
    modTime key = do
        fileName <- keyToFileName key
        Dir.getModificationTime fileName

    keyToFileName :: Store.Key -> IO FilePath
    keyToFileName (Store.Key (Store.Path prefix) k) =
        (</> T.unpack prefix </> T.unpack k) <$> getFilePath root

    ensureSubdir :: Store.Path -> IO ()
    ensureSubdir (Store.Path p) = do
        dir <- (</> T.unpack p) <$> getFilePath root
        Dir.createDirectoryIfMissing True dir


listFilesRecursively :: Store.Path -> Stream' Store.Key (ResourceT IO) ()
listFilesRecursively path@(Store.Path (T.unpack -> dir)) = do
    files <- liftIO $ Dir.listDirectory dir
    forM_ files $ \file -> do
        let dir' = dir </> file
        exists <- liftIO $ Dir.doesDirectoryExist dir'
        if exists
            then listFilesRecursively $ Store.Path (T.pack dir')
            else S.yield $ Store.Key path (T.pack file)
