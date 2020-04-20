{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Use SQLite for simple disk-backed collections.
module Atavachron.DB.Connection
  ( connect
  , disconnect
  , Connection(..)
  ) where

import qualified Data.Text as T
import           Database.SQLite3 (Database)
import qualified Database.SQLite3 as SQL

import           Atavachron.Path

newtype Connection = Connection { cDatabase :: Database }

connect :: Path Abs File -> IO Connection
connect path = do
    fpAsText <- T.pack <$> getFilePath path
    cDatabase <- SQL.open fpAsText

    SQL.exec cDatabase "PRAGMA journal_mode=WAL;"
    SQL.exec cDatabase "PRAGMA synchronous=NORMAL;"
    -- SQL.exec cDatabase "PRAGMA journal_mode=MEMORY;"
    -- SQL.exec cDatabase "PRAGMA synchronous=OFF;" -- a corrupt cache on power failure is no big issue
    SQL.exec cDatabase "PRAGMA cache_size=10000;"
    SQL.exec cDatabase "PRAGMA locking_mode=EXCLUSIVE;"

    return $ Connection{..}

disconnect :: Connection -> IO ()
disconnect Connection{..} =
    SQL.close cDatabase
