{-# LANGUAGE RecordWildCards #-}

-- | Local filesystem Store
--
module Atavachron.Store.LocalFS where

import Control.Monad
import Control.Monad.IO.Class

import qualified Data.Text as T

import qualified Data.ByteString.Lazy as LB
import qualified System.Directory as Dir

import Atavachron.Path (Path, Abs, Dir, getFilePath)
import Atavachron.Store (Store(..))
import qualified Atavachron.Store as Store
import Atavachron.Streaming

import System.FilePath

newLocalFS :: Path Abs Dir -> IO Store
newLocalFS root = do
    Dir.createDirectoryIfMissing True $ getFilePath root </> "chunks"
    return $ Store{..}

  where

    list :: Store.Path -> Stream' Store.Key IO ()
    list _path = undefined

    get :: Store.Key  -> IO (Maybe LB.ByteString)
    get (Store.Key (Store.Path prefix) name) = liftM Just $ -- TODO
        LB.readFile fileName
      where
        fileName = getFilePath root </> T.unpack prefix </> T.unpack name

    put :: Store.Key -> LB.ByteString -> IO ()
    put (Store.Key (Store.Path prefix) name) =
        liftIO . LB.writeFile fileName
      where
        fileName = getFilePath root </> T.unpack prefix </> T.unpack name
