{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

-- | Use SQLite as a simple disk-backed Set
-- This allows is to scale up to huge numbers of chunks.

module Atavachron.Chunk.Cache
  ( connect
  , close
  , insert
  , delete
  , member
  , notMember
  , size
  , values
  , clear
  , Connection
  ) where

import           Prelude hiding (lookup)
import           Control.Monad
import           Control.Monad.IO.Class
import           Data.Int
import qualified Data.Text as T
import           Database.SQLite3 (Database, Statement, SQLData(..))
import qualified Database.SQLite3 as SQL

import Streaming (Stream, Of(..))
import qualified Streaming.Prelude as S

import Atavachron.Chunk.Encode

data Connection = Connection
  { connDatabase   :: Database
  , connInsertStmt :: Statement
  , connDeleteStmt :: Statement
  , connMemberStmt :: Statement
  , connSizeStmt   :: Statement
  , connValuesStmt :: Statement
  , connClearStmt  :: Statement
  }

connect :: T.Text -> IO Connection
connect fileName = do
    connDatabase   <- SQL.open fileName

    SQL.exec connDatabase $
        "CREATE TABLE IF NOT EXISTS object (" <>
        "id   BLOB(32) PRIMARY KEY NOT NULL);"

    connInsertStmt <- SQL.prepare connDatabase "INSERT OR IGNORE INTO object (id) VALUES (?);"
    connDeleteStmt <- SQL.prepare connDatabase "DELETE FROM object WHERE id = ?;"
    connMemberStmt <- SQL.prepare connDatabase "SELECT 1 FROM object WHERE id = ?;"
    connSizeStmt   <- SQL.prepare connDatabase "SELECT COUNT(*) FROM object;"
    connValuesStmt <- SQL.prepare connDatabase "SELECT id FROM object;"
    connClearStmt  <- SQL.prepare connDatabase "DELETE FROM object;"
    return $ Connection{..}

close :: Connection -> IO ()
close Connection{..} = do
    SQL.finalize connInsertStmt
    SQL.finalize connDeleteStmt
    SQL.finalize connMemberStmt
    SQL.finalize connSizeStmt
    SQL.finalize connValuesStmt
    SQL.finalize connClearStmt
    SQL.close connDatabase

insert :: Connection -> StoreID -> IO ()
insert Connection{..} (StoreID key) = do
    SQL.bind connInsertStmt [SQLBlob key]
    void $ SQL.step connInsertStmt
    SQL.reset connInsertStmt

delete :: Connection -> StoreID -> IO ()
delete Connection{..} (StoreID key) = do
    SQL.bind connDeleteStmt [SQLBlob key]
    void $ SQL.step connDeleteStmt
    SQL.reset connDeleteStmt

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

notMember :: Connection -> StoreID -> IO Bool
notMember conn = fmap not . member conn

size :: Connection -> IO Int64
size Connection{..} = do
    SQL.bind connSizeStmt []
    res <- SQL.step connSizeStmt
    case res of
        SQL.Row  -> do
            col <- SQL.column connSizeStmt 0
            SQL.reset connSizeStmt
            case col of
                SQLInteger i -> return i
                _            -> return 0
        SQL.Done -> do
            SQL.reset connSizeStmt
            return 0

values :: Connection -> Stream (Of StoreID) IO ()
values Connection{..} = do
    liftIO $ SQL.bind connValuesStmt []
    go
    liftIO $ SQL.reset connValuesStmt
    return ()
  where
    go = do
        res <- liftIO $ SQL.step connValuesStmt
        case res of
            SQL.Row  -> do
                col <- liftIO $ SQL.column connValuesStmt 0
                case col of
                    SQLBlob key -> S.yield (StoreID key) >> go
                    _           -> return ()
            SQL.Done -> return ()

clear :: Connection -> IO ()
clear Connection{..} = do
    SQL.bind connClearStmt []
    void $ SQL.step connClearStmt
    SQL.reset connClearStmt
