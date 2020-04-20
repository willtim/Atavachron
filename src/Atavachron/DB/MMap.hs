{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -Wno-name-shadowing -Wno-simplifiable-class-constraints #-}

-- | Use SQLite as a simple disk-backed mutable Map.
-- This allows is to scale up to huge numbers of files and chunks.

module Atavachron.DB.MMap
  ( open
  , close
  , withMap
  , withEmptyMap
  , innerJoin
  , leftOuterJoin
  , insert
  , insertMany
  , delete
  , lookup
  , member
  , notMember
  , size
  , keys
  , toStream
  , elems
  , clear
  , MMap
  ) where

import           Prelude hiding (lookup)
import           Control.Monad
import           Control.Monad.Catch
import           Control.Monad.IO.Class
import           Control.Concurrent
import           Data.Int
import           Data.IORef
import           Data.Map (Map)
import qualified Data.Map as Map
import           Data.Proxy
import           Data.Text (Text)
import qualified Data.Text as T
import           Database.SQLite3 (Database, Statement, StepResult, SQLData(..))
import qualified Database.SQLite3 as SQL

import Streaming (Stream, Of(..))
import qualified Streaming.Prelude as S (yield)

import qualified Atavachron.Streaming as S
import Atavachron.DB.Field
import Atavachron.DB.Connection


-- NOTE: prepared statements cannot be used concurrently
-- so we create thread-local copies.
type StmtCache = IORef (Map ThreadId Statement)

data MMap k v = MMap
  { mDatabase  :: Database
  , mTableName :: Text
  , mInsertRef :: StmtCache
  , mDeleteRef :: StmtCache
  , mLookupRef :: StmtCache
  , mMemberRef :: StmtCache
  , mSizeRef   :: StmtCache
  , mKeysRef   :: StmtCache
  , mElemsRef  :: StmtCache
  , mValuesRef :: StmtCache
  , mClearRef  :: StmtCache
  }

withMap
    :: forall m k v r. (MonadIO m, MonadMask m, Field k, Field v)
    => Connection
    -> Text
    -> (MMap k v -> m r)
    -> m r
withMap conn name f = do
    m  <- liftIO $ open conn name
    f m `finally` liftIO (close m)

withEmptyMap
    :: forall m k v r. (MonadIO m, MonadMask m, Field k, Field v)
    => Connection
    -> Text
    -> (MMap k v -> m r)
    -> m r
withEmptyMap conn name f =
    withMap conn name $ \m -> liftIO (clear m) >> f m

open :: forall k v. (Field k, Field v) => Connection -> Text -> IO (MMap k v)
open (Connection mDatabase) mTableName = do
    SQL.exec mDatabase $
        "CREATE TABLE IF NOT EXISTS " <> mTableName <> " (" <>
        "k " <> sqlType (Proxy :: Proxy k) <> " PRIMARY KEY NOT NULL," <>
        "v " <> sqlType (Proxy :: Proxy v) <> " NOT NULL);"

    mInsertRef <- newIORef $! Map.empty
    mDeleteRef <- newIORef $! Map.empty
    mLookupRef <- newIORef $! Map.empty
    mMemberRef <- newIORef $! Map.empty
    mSizeRef   <- newIORef $! Map.empty
    mKeysRef   <- newIORef $! Map.empty
    mElemsRef  <- newIORef $! Map.empty
    mValuesRef <- newIORef $! Map.empty
    mClearRef  <- newIORef $! Map.empty
    return MMap{..}

-- NOTE: does not close the database connection, as it may be shared.
close :: MMap k v -> IO ()
close MMap{..} = do
    finaliseAll mInsertRef
    finaliseAll mDeleteRef
    finaliseAll mLookupRef
    finaliseAll mMemberRef
    finaliseAll mSizeRef
    finaliseAll mKeysRef
    finaliseAll mElemsRef
    finaliseAll mValuesRef
    finaliseAll mClearRef
  where
    finaliseAll ref = readIORef ref >>= mapM_ SQL.finalize . Map.elems

insert :: (Field k, Field v) => MMap k v -> k -> v -> IO ()
insert MMap{..} key val =
    withBind mDatabase sql mInsertRef [toField key, toField val] step_
  where
    sql = "INSERT OR REPLACE INTO " <> mTableName <> " (k,v) VALUES (?,?);"

-- NOTE: that we don't wrap this in a transaction as the stream may be interleaved with other
-- queries and/or updates to the same database.
insertMany :: (MonadIO m, Field k, Field v) => MMap k v -> Stream (Of (k,v)) m r -> m r
insertMany m@MMap{..} =
    S.mapM_ (liftIO . uncurry (insert m))

delete :: (Field k) => MMap k v -> k -> IO ()
delete MMap{..} key =
    withBind mDatabase sql mDeleteRef [toField key] step_
  where
    sql = "DELETE FROM " <> mTableName <> " WHERE k = ?;"

lookup :: (Field k, Field v) => MMap k v -> k -> IO (Maybe v)
lookup MMap{..} key =
    withBind mDatabase sql mLookupRef [toField key] $ \stmt -> do
        res <- step stmt
        case res of
            SQL.Row  -> fromField <$> SQL.column stmt 0
            SQL.Done -> return Nothing
  where
    sql = "SELECT v FROM " <> mTableName <> " WHERE k = ?;"

member :: (Field k) => MMap k v -> k -> IO Bool
member MMap{..} key =
    withBind mDatabase sql mMemberRef [toField key] $ \stmt -> do
        res <- step stmt
        case res of
            SQL.Row  -> do
                col <- SQL.column stmt 0
                case col of
                    SQLInteger{} -> return True
                    _            -> return False
            SQL.Done ->
                return False
  where
    sql = "SELECT 1 FROM " <> mTableName <> " WHERE k = ?;"

notMember :: (Field k) => MMap k v -> k -> IO Bool
notMember m = fmap not . member m

size :: MMap k v -> IO Int64
size MMap{..} =
    withBind mDatabase sql mSizeRef [] $ \stmt -> do
        res <- step stmt
        case res of
            SQL.Row  -> do
                col <- SQL.column stmt 0
                case col of
                    SQLInteger i -> return i
                    _            -> return 0
            SQL.Done ->
                return 0
  where
    sql = "SELECT COUNT(k) FROM " <> mTableName<>";"

keys :: (Field k) => MMap k v -> Stream (Of k) IO ()
keys MMap{..} =
    withBind mDatabase sql mKeysRef [] go
  where
    go stmt = do
        res <- liftIO $ step stmt
        case res of
            SQL.Row  -> do
                col <- liftIO $ SQL.column stmt 0
                case fromField col of
                    Just k -> S.yield k >> go stmt
                    _      -> return ()
            SQL.Done -> return ()

    sql = "SELECT k FROM " <> mTableName <> " ORDER BY k;"

elems :: (Field v) => MMap k v -> Stream (Of v) IO ()
elems MMap{..} =
    withBind mDatabase sql mElemsRef [] go
  where
    go stmt = do
        res <- liftIO $ step stmt
        case res of
            SQL.Row  -> do
                col <- liftIO $ SQL.column stmt 0
                case fromField col of
                    Just v -> S.yield v >> go stmt
                    _      -> return ()
            SQL.Done -> return ()

    sql = "SELECT v FROM " <> mTableName <> " ORDER BY k;"

toStream :: (Field k, Field v) => MMap k v -> Stream (Of (k,v)) IO ()
toStream MMap{..} =
    withBind mDatabase sql mValuesRef [] go
  where
    go stmt = do
        res <- liftIO $ step stmt
        case res of
            SQL.Row  -> do
                col_k <- liftIO $ SQL.column stmt 0
                col_v <- liftIO $ SQL.column stmt 1
                case (,) <$> fromField col_k <*> fromField col_v of
                    Just kv -> S.yield kv >> go stmt
                    _       -> return ()
            SQL.Done -> return ()

    sql = "SELECT k,v FROM " <> mTableName <> " ORDER BY k;"

clear :: MMap k v -> IO ()
clear MMap{..} =
    withBind mDatabase sql mClearRef [] step_
  where
    sql = "DELETE FROM " <> mTableName <> ";"

innerJoin
    :: (Field v, Field v')
    => Connection -> Text -> Text -> Stream (Of (v,v')) IO ()
innerJoin (Connection database) tb1 tb2 =
    withSql database sql [] go
  where
    sql = mconcat ["SELECT ", tb1, ".v,", tb2, ".v FROM ", tb1, " INNER JOIN ", tb2," ON "
                  , tb1, ".k", " = ", tb2, ".k ORDER BY ", tb1, ".k;" ]

    go stmt = do
        res <- liftIO $ step stmt
        case res of
            SQL.Row  -> do
                col_v  <- liftIO $ SQL.column stmt 0
                col_v' <- liftIO $ SQL.column stmt 1
                case (,) <$> fromField col_v <*> fromField col_v' of
                    Just vv' -> S.yield vv' >> go stmt
                    _        -> return ()
            SQL.Done -> return ()

leftOuterJoin
    :: (Field v, Field (Maybe v'))
    => Connection -> Text -> Text -> Stream (Of (v,Maybe v')) IO ()
leftOuterJoin (Connection database) tb1 tb2 =
    withSql database sql [] go
  where
    sql = mconcat ["SELECT ", tb1, ".v,", tb2, ".v FROM ", tb1, " LEFT OUTER JOIN ", tb2," ON "
                  , tb1, ".k", " = ", tb2, ".k ORDER BY ", tb1, ".k;" ]

    go stmt = do
        res <- liftIO $ step stmt
        case res of
            SQL.Row  -> do
                col_v  <- liftIO $ SQL.column stmt 0
                col_v' <- liftIO (SQL.column stmt 1)
                case (,) <$> fromField col_v <*> fromField col_v' of
                    Just vv' -> S.yield vv' >> go stmt
                    _        -> return ()
            SQL.Done -> return ()

withBind
    :: MonadIO m
    => Database
    -> Text
    -> IORef (Map ThreadId Statement)
    -> [SQLData]
    -> (Statement -> m r)
    -> m r
withBind database sql ref params f = do
    stmt <- liftIO $ getCachedStmt database sql ref
    liftIO $ SQL.bind stmt params
    res  <- f stmt
    liftIO $ SQL.reset stmt
    return res

getCachedStmt
    :: Database
    -> Text
    -> IORef (Map ThreadId Statement)
    -> IO Statement
getCachedStmt database sql ref = do
    m    <- readIORef ref
    tid  <- myThreadId
    case Map.lookup tid m of
        Just stmt -> return stmt
        Nothing   -> do
            stmt <- SQL.prepare database sql
            atomicModifyIORef' ref $ \m -> (Map.insert tid stmt m, ())
            return $! stmt

withSql :: MonadIO m => Database -> T.Text -> [SQLData] -> (Statement -> m r) -> m r
withSql database sql params f = do
    stmt <- liftIO $ SQL.prepare database sql
    liftIO $ SQL.bind stmt params
    res  <- f stmt
    liftIO $ SQL.finalize stmt
    return res

{-# INLINE step #-}
step :: Statement -> IO StepResult
-- Faster step for statements that do not callback to Haskell
-- functions (e.g. custom SQL functions).
step = SQL.stepNoCB

{-# INLINE step_ #-}
step_ :: Statement -> IO ()
step_ = void . step
