{-# LANGUAGE RecordWildCards #-}

-- | Local filesystem Store, intended only for testing.
--
module Atavachron.Store.LocalFS where

import Control.Monad.IO.Class

import qualified Data.Text as T

import qualified Data.ByteString.Lazy as LB
import qualified System.Directory as Dir
import System.FilePath

import Atavachron.Path (Path, Abs, Dir, getFilePath)
import Atavachron.Store (Store(..))
import qualified Atavachron.Store as Store
import Atavachron.Streaming (Stream')
import qualified Streaming.Prelude as S


newLocalFS :: Path Abs Dir -> Store
newLocalFS root = Store{..}
  where

    list :: Store.Path -> Stream' Store.Key IO ()
    list path@(Store.Path name) = do
        liftIO $ ensureSubdir path
        dirName <- liftIO $ (</> T.unpack name) <$> getFilePath root
        entries <- liftIO $ Dir.listDirectory dirName
        S.each $ map (Store.Key path . T.pack) entries

    get :: Store.Key -> IO LB.ByteString
    get key = do
        ensureSubdir $ Store.kPath key
        fileName <- keyToFileName key
        LB.readFile fileName

    put :: Store.Key -> LB.ByteString -> IO ()
    put key bs = do
        ensureSubdir $ Store.kPath key
        fileName <- keyToFileName key
        LB.writeFile fileName bs

    hasKey :: Store.Key -> IO Bool
    hasKey key = do
        ensureSubdir $ Store.kPath key
        fileName <- keyToFileName key
        Dir.doesFileExist fileName

    keyToFileName :: Store.Key -> IO FilePath
    keyToFileName (Store.Key (Store.Path prefix) name) =
        (</> T.unpack prefix </> T.unpack name) <$> getFilePath root

    ensureSubdir :: Store.Path -> IO ()
    ensureSubdir (Store.Path name) = do
        dir <- (</> T.unpack name) <$> getFilePath root
        Dir.createDirectoryIfMissing True dir
