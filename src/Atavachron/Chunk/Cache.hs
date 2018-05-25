{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

-- | Use SQLite as a simple disk-backed Set
-- This allows is to scale up to huge numbers of chunks.

module Atavachron.Chunk.Cache
  ( connect
  , close
  , insert
  , member
  , Connection
  ) where

import           Prelude hiding (lookup)
import           Control.Monad
import qualified Data.Text as T
import           Data.Monoid
import           Database.SQLite3 (Database, Statement, SQLData(..))
import qualified Database.SQLite3 as SQL

import Atavachron.Chunk.Process

data Connection = Connection
  { connDatabase   :: Database
  , connInsertStmt :: Statement
  , connMemberStmt :: Statement
--  , connSizeStmt   :: Statement
  }

connect :: T.Text -> IO Connection
connect fileName = do
    connDatabase   <- SQL.open fileName

    SQL.exec connDatabase $
        "CREATE TABLE IF NOT EXISTS object (" <>
        "id   BLOB(32) PRIMARY KEY NOT NULL);"

    connInsertStmt <- SQL.prepare connDatabase "INSERT OR IGNORE INTO object (id) VALUES (?);"
    connMemberStmt <- SQL.prepare connDatabase "SELECT 1 FROM object WHERE id = ?;"
    -- connSizeStmt   <- SQL.prepare db "SELECT COUNT(*) FROM object;"
    return $ Connection{..}

close :: Connection -> IO ()
close Connection{..} = do
    SQL.finalize connInsertStmt
    SQL.finalize connMemberStmt
    SQL.close connDatabase

insert :: Connection -> StoreID -> IO ()
insert Connection{..} (StoreID key) = do
    SQL.bind connInsertStmt [SQLBlob key]
    void $ SQL.step connInsertStmt
    SQL.reset connInsertStmt

member :: Connection -> StoreID -> IO Bool
member Connection{..} (StoreID key) = do
    SQL.bind connMemberStmt [SQLBlob key]
    res <- SQL.step connMemberStmt
    case res of
        SQL.Row  -> do
            col <- SQL.column connMemberStmt 0
            SQL.reset connMemberStmt
            case col of
                SQLInteger{} -> return True
                _            -> return False
        SQL.Done -> do
            SQL.reset connMemberStmt
            return False
