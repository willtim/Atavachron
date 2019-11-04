{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Garbage collection and chunk check/repair procedures.
--

module Atavachron.Garbage where

import Prelude hiding (concatMap)
import Control.Lens (over)
import Control.Monad
import Control.Monad.Catch
import Control.Monad.IO.Class
import Control.Monad.Morph
import Control.Monad.Reader.Class
import Control.Monad.State.Class
import Control.Monad.Trans.Resource
import qualified Data.List as List
import Data.Maybe (isNothing)
import Data.Time.Calendar (diffDays)
import qualified Data.Text as T
import Data.Time.Clock
import Data.Semigroup (Semigroup(..))
import qualified Streaming as S
import qualified Streaming.Prelude as S hiding (mapM_)

import Atavachron.Repository
import qualified Atavachron.Chunk.Cache as ChunkCache
import Atavachron.Chunk.Encode
import Atavachron.Logging
import Atavachron.Streaming (Stream')
import qualified Atavachron.Streaming as S
import Atavachron.Env
import Atavachron.Pipelines (collectChunks, withChunkCache)


-- | Find and collect garbage using the the entire set of snapshots
-- for the repository. The deleted snapshots parameter is optional, if
-- it is provided an incremental garbage collection is performed which
-- considers only the chunks from the deleted snapshots as potential
-- garbage; otherwise an exhaustive collection is performed.
-- NOTE: This is a destructive operation!
collectGarbage
    :: (MonadReader Env m, MonadState Progress m, MonadThrow m, MonadResource m)
    => Repository
    -> Maybe (Stream' (SnapshotName, Snapshot) m ())
    -> m ()
collectGarbage repo deletedSnapshots = do
   garbageStr <- maybe (findGarbageExhaustive repo)
                       (findGarbageIncremental repo)
                       deletedSnapshots

   when (isNothing deletedSnapshots) $
       logInfo "Performing an exhaustive garbage collection."

   garbage S.:> _ <-
       S.sum . flip S.mapM garbageStr $ \storeID -> do
           res <- liftIO $ garbageCollectChunk repo storeID
           case res of
               Left ex -> do
                   logWarn $ "Could not garbage collect chunk " <> T.pack (show storeID)
                       <> " : " <> T.pack (show ex)
                   return 0
               Right () -> return (1::Integer)

   logInfo $ "Garbage collected: " <> T.pack (show garbage) <> " chunks."

-- | Delete (expired) garbage.
-- The expiry time should be set to an interval greater than the longest likely backup time.
-- This will prevent any possible concurrent backups being affected.
-- NOTE: This is a destructive operation!
deleteGarbage
    :: (MonadReader Env m, MonadState Progress m, MonadThrow m, MonadResource m)
    => Repository
    -> m ()
deleteGarbage repo = do
   --- Run chunk repair first as a precaution against any (old, expired) referenced garbage chunks
   logInfo "Forcing a chunk repair..."
   chunkRepair repo

   t1  <- asks envStartTime
   ttl <- asks envGarbageExpiryDays

   logInfo $ "Finding and deleting expired garbage (expiry is " <> T.pack (show ttl) <> " days)..."
   deleted S.:> _ <-
       S.sum
       . S.mapM deleteGarbageChunks'
       . S.mapped S.toList
       . S.chunksOf 100 -- delete in batches of 100
       . S.concat       -- remove Nothings
       . flip S.mapM garbageChunks $ \storeID -> do
           e't0  <- liftIO $ getGarbageModTime repo storeID
           case e't0 of
               Left ex -> do
                   logWarn $ "Could not query modification time of garbage chunk " <> T.pack (show storeID)
                       <> " : " <> T.pack (show ex)
                   return Nothing
               Right t0 -> do
                   let diff = fromIntegral $ diffDays (utctDay t1) (utctDay t0)
                   if diff >= ttl -- expiry condition
                       then return $ Just storeID
                       else return Nothing

   logInfo $ "Garbage deleted: " <> T.pack (show deleted) <> " chunks."
  where
   garbageChunks = hoist liftResourceT $ listGarbageChunks repo

   -- delete chunks in batches
   deleteGarbageChunks' storeIDs = liftIO $ do
       res <- liftIO $ deleteGarbageChunks repo storeIDs
       case res of
           Left ex -> do
               logWarn $ "Could not delete garbage chunks: " <> T.pack (show storeIDs)
                   <> " : " <> T.pack (show ex)
               return 0
           Right () -> return (List.genericLength storeIDs ::Integer)



-- | Find garbage by considering every chunk in the repository and
-- using the entire set of snapshots. This /exhaustive collection/ is
-- expensive and more likely to interfere with concurrent backups,
-- requiring later repository repairs (newly uploaded chunks
-- erroneously marked as garbage). An exhaustive collection may be
-- required if a backup is abandoned before it completes, leaving
-- unreferenced chunks in the repository.
findGarbageExhaustive
    :: (MonadReader Env m, MonadState Progress m, MonadThrow m, MonadResource m)
    => Repository
    -> m (Stream' StoreID m ())
findGarbageExhaustive repo =
    chunkDifference repoChunks rootChunks
  where
    rootChunks = S.concatMap (collectChunks . snd) $ listSnapshotsPartial repo
    repoChunks = hoist liftResourceT $ listChunks repo

-- | A non-exhaustive (incremental) collection that considers only
-- chunks referenced by the supplied deleted snapshots and finds those
-- that are not in the repository's entire snapshot set.
findGarbageIncremental
    :: (MonadReader Env m, MonadState Progress m, MonadThrow m, MonadResource m)
    => Repository
    -> Stream' (SnapshotName, Snapshot) m ()
    -> m (Stream' StoreID m ())
findGarbageIncremental repo deletedSnapshots =
    chunkDifference deletedChunks rootChunks
  where
    rootChunks    = S.concatMap (collectChunks . snd) $ listSnapshotsPartial repo
    deletedChunks = S.concatMap (collectChunks . snd) deletedSnapshots

-- | Find the difference between two chunk streams using an on-disk chunk cache.
chunkDifference
    :: (MonadReader Env m, MonadState Progress m, MonadThrow m, MonadResource m)
    => Stream' StoreID m ()
    -> Stream' StoreID m ()
    -> m (Stream' StoreID m ())
chunkDifference leftChunks rightChunks = do
    assertCacheEmpty

    -- Add all left chunks (for consideration) to the cache.
    -- NOTE: this stream will potentially contain duplicate store IDs,
    -- which the chunk cache will de-duplicate for us.
    sinkChunks leftChunks ChunkCache.insert

    -- Remove all chunks referenced by the right chunks from cache.
    sinkChunks rightChunks ChunkCache.delete

    -- The cache now contains left chunks not referenced in the right chunks.
    return $ withChunkCache $ hoist liftIO . ChunkCache.values

sinkChunks
    :: (MonadState Progress m, MonadReader Env m, MonadResource m)
    => Stream' StoreID m ()
    -> (ChunkCache.Connection -> StoreID -> IO ())
    -> m ()
sinkChunks chunks op = do
    withChunkCache $ \conn ->
        S.mapM_ (liftIO . op conn)
            . showChunksReceived . S.copy
            $ chunks -- potentially duplicate chunks
    resetStdErrCursor

showChunksReceived
    :: (MonadState Progress m, MonadReader Env m, MonadIO m)
    => Stream' StoreID m r
    -> m r
showChunksReceived = S.mapM_ $ \_ -> do
    modify (over prChunks succ)
    gets _prChunks >>= \n ->
        when (n `mod` 100==0) $ putProgress $ "Chunks received: " ++ show n

-- | Check that the the on-disk chunk cache state is empty, as expected.
assertCacheEmpty
  :: (MonadReader Env m, MonadResource m)
  => m ()
assertCacheEmpty = do
    size <- withChunkCache $ liftIO . ChunkCache.size
    when (size /= 0) $ panic "Assertion failed! Chunk cache is not empty."

listSnapshotsPartial :: MonadResource m => Repository -> Stream' (SnapshotName, Snapshot) m ()
listSnapshotsPartial repo =
    S.map (fmap $ either err id) . hoist liftResourceT $ listSnapshots repo
  where
    err ex = panic $ "Failed to fetch snapshot: " <> T.pack (show ex)

-- | Perform a chunk check, reporting:
-- * number of chunks referenced by snapshots in the repository;
-- * any (unreferenced) chunks eligible for garbage collection;
-- * referenced garbage eligible for repair;
-- * chunks missing from the repository entirely;
-- * number of garbage chunks.
chunkCheck
    :: (MonadReader Env m, MonadState Progress m, MonadThrow m, MonadResource m)
    => Repository
    -> m ()
chunkCheck repo = do
    assertCacheEmpty

    -- Add chunks referenced by all snapshots in the repository to cache.
    logInfo "Listing chunks referenced by snapshots..."
    sinkChunks snapChunks ChunkCache.insert

    -- This is all referenced chunks from all repository snapshots
    -- (some may unfortunately have been moved to garbage, due to
    -- concurrent backups running during a prune).
    referenced  <- cacheSize

    -- List all chunks in repo and remove them from the cache.
    -- There are two sets we are interested in:
    -- 1) missing: chunks in cache, not in this stream
    -- 2) collectable: chunks in this stream, but not in cache
    logInfo "Listing all chunks in repository..."
    collectable S.:> _ <- count $ filterNotInChunkCache repoChunks
    missing <- cacheSize

    -- List all garbage chunks in repo.
    -- There are an additional three sets we are interested in:
    -- 1) referenced garbage: chunks in cache and in garbage stream
    -- 2) garbage (unreferenced): chunks not in cache, but in the stream
    -- 3) missing completely: chunks in cache, but not in stream
    logInfo "Listing all garbage chunks in repository..."
    unreferenced_garbage S.:> _ <- count $ filterNotInChunkCache garbageChunks
    missing_completely <- cacheSize

    let referenced_garbage = missing - missing_completely

    liftIO $ do
        logInfo $ "Referenced from snapshots:   " <> T.pack (show referenced)
        logInfo $ "Collectable as garbage:      " <> T.pack (show collectable)
        logInfo $ "Referenced garbage:          " <> T.pack (show referenced_garbage)
        logInfo $ "Unreferenced garbage:        " <> T.pack (show unreferenced_garbage)
        logInfo $ "Missing from repository (!): " <> T.pack (show missing_completely)

    when (referenced_garbage > 0) $
        logWarn $ T.pack $ unlines
            [ "The repository contains garbage that is currently referenced by one or more snapshots."
            , "This can occur as a result of running backups and prune concurrently."
            , "It can be fixed by the chunks '--repair' flag."
            , "Note that repair is automatically run first as part of the --delete-garbage operation."
            ]

    when (missing_completely > 0) $
        panic $ T.pack $ unlines
            [ "The repository appears corrupt in that there are missing chunks!"
            , "Please verify the latest snapshots!"
            ]
  where
    count = S.sum . S.map (const (1::Integer))

    cacheSize = withChunkCache $ liftIO . ChunkCache.size

    snapChunks = S.concatMap (collectChunks . snd) $ listSnapshotsPartial repo
    repoChunks = hoist liftResourceT $ listChunks repo
    garbageChunks = hoist liftResourceT $ listGarbageChunks repo

    -- Filters the supplied chunk stream such that it includes only
    -- store ids that are *not* in the chunk cache. StoreIds filtered
    -- out are also then removed from the chunk cache.
    filterNotInChunkCache chunksStr = do
        void $ withChunkCache $ \conn ->
            S.filterM (liftIO . isNotInCache conn)
            . showChunksReceived . S.copy
            $ chunksStr
        lift resetStdErrCursor
      where
        isNotInCache conn storeID = do
            isMember <- ChunkCache.member conn storeID
            if isMember
               then ChunkCache.delete conn storeID >> return False
               else return True

-- | Repair any referenced garbage chunks by promoting them back to the main chunks set.
-- This situation can occur as a result of running backups and prune concurrently.
-- Note that check/repair should always be done first as part of the --delete-garbage operation."
chunkRepair
    :: (MonadReader Env m, MonadState Progress m, MonadThrow m, MonadResource m)
    => Repository
    -> m ()
chunkRepair repo = do
    assertCacheEmpty

    -- Add chunks referenced by all snapshots in the repository to cache.
    logInfo "Listing chunks referenced by snapshots..."
    sinkChunks snapChunks ChunkCache.insert

    -- List all chunks in repo and remove them from the cache.
    -- The cache should then contain missing chunks.
    logInfo "Listing all chunks in repository..."
    sinkChunks repoChunks ChunkCache.delete

    withChunkCache $ \conn -> do
        let missingStr = hoist liftIO $ ChunkCache.values conn
        flip S.mapM_ missingStr $ \storeID -> do
           e'present <- liftIO $ hasGarbageChunk repo storeID
           case e'present of
               Left err ->
                   panic $ "Could not determine presence of garbage chunk " <> T.pack (show storeID)
                       <> " : " <> T.pack (show err)
               Right True -> do
                   logInfo $ "Restoring chunk from garbage: "  <> T.pack (show storeID)
                   res <- liftIO $ restoreGarbageChunk repo storeID
                   case res of
                       Left err -> logWarn $ "Could not restore garbage chunk " <> T.pack (show storeID)
                           <> " : " <> T.pack (show err)
                       Right () -> return ()
               Right False ->
                   panic $ "Chunk is missing from the repository: " <> T.pack (show storeID)
                       <> " : Please verify the latest backups!"
  where
    snapChunks = S.concatMap (collectChunks . snd) $ listSnapshotsPartial repo
    repoChunks = hoist liftResourceT $ listChunks repo
