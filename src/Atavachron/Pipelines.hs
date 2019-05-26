{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

-- | Stream pipelines for backup, restore, verify etc.
--

module Atavachron.Pipelines where

import Prelude hiding (concatMap)
import Control.Arrow ((+++), (&&&))
import Control.Lens (over)
import Control.Monad
import Control.Monad.Catch
import Control.Monad.IO.Class
import Control.Monad.Morph
import Control.Monad.Fail
import Control.Monad.Reader.Class
import Control.Monad.State.Class
import Control.Monad.Trans.Resource

import Data.Function (on)

import qualified Data.ByteString as B
import qualified Data.ByteString.Builder as Builder
import qualified Data.ByteString.Lazy as LB
import Data.Maybe (isNothing)
import Data.Time.Calendar (diffDays)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.IO as T
import Data.Time.Clock

import qualified Data.List as List

import Data.Semigroup (Semigroup(..))
import Data.Sequence (Seq)
import qualified Data.Sequence as Seq

import Streaming (Stream, Of(..))
import Streaming.Prelude (yield)
import qualified Streaming as S
import qualified Streaming.Prelude as S hiding (mapM_)
import qualified Streaming.Internal as S (concats)

import qualified System.Posix.User as User
import qualified System.Directory as Dir

import Network.HostName (getHostName)
import qualified Network.URI.Encode as URI

import Atavachron.Repository
import Atavachron.Chunk.Builder
import qualified Atavachron.Chunk.Cache as ChunkCache
import qualified Atavachron.Chunk.CDC as CDC
import Atavachron.Chunk.Encode

import Atavachron.IO
import Atavachron.Logging
import Atavachron.Path

import Atavachron.Streaming (Stream', StreamF)
import qualified Atavachron.Streaming as S

import Atavachron.Env
import Atavachron.Tree
import Atavachron.Files


------------------------------------------------------------
-- Pipelines

-- | Full and incremental backup.
backupPipeline
    :: (MonadReader Env m, MonadState Progress m, MonadResource m, MonadFail m)
    => Bool
    -> Path Abs Dir
    -> m Snapshot
backupPipeline forceFullScan
    = makeSnapshot
    . uploadPipeline
    . serialiseTree
    . overFileItems fst
          ( writeFilesCache
          . overChangedFiles (uploadPipeline . readFiles)
          . diff fst id previousFiles
          )
    . filterItems id
    . recurseDir
  where
    previousFiles | forceFullScan = mempty
                  | otherwise     = readFilesCache

-- | Verify files and their chunks.
verifyPipeline
    :: (MonadReader Env m, MonadState Progress m, MonadThrow m, MonadIO m)
    => Snapshot
    -> Stream' (FileItem, VerifyResult) m ()
verifyPipeline
  = summariseErrors
  . downloadPipeline
  . S.lefts
  . filterItems fst
  . snapshotTree

-- | Restore (using the FilePredicate in the environment).
restoreFiles
    :: (MonadReader Env m, MonadState Progress m, MonadThrow m, MonadResource m)
    => Snapshot
    -> m ()
restoreFiles
  = saveFiles
  . overFileItems rcTag
        ( trimChunks
        . rechunkToTags
        . handleErrors
        . downloadPipeline
        )
  . filterItems fst
  . snapshotTree

-- | The complete upload pipeline: CDC chunking, encryption,
-- compression and upload to remote location.
uploadPipeline
  :: (MonadReader Env m, MonadState Progress m, MonadResource m, Eq t, Show t)
  => Stream' (RawChunk t B.ByteString) m r
  -> Stream' (t, ChunkList) m r
uploadPipeline
    = packChunkLists
    . progressMonitor . S.copy
    . S.mergeEither
    . S.left (storeChunks . encodeChunks)
    . dedupChunks
    . hashChunks
    . rechunkCDC

-- | A download pipeline that does not abort on errors.
downloadPipeline
    :: (MonadReader Env m, MonadState Progress m, MonadThrow m, MonadIO m)
    => Stream' (FileItem, ChunkList) m ()
    -> Stream' (Either (Error FileItem) (PlainChunk FileItem)) m ()
downloadPipeline
    = progressMonitor . S.copy
    . S.bind verifyChunks
    . S.bind decodeChunks
    . retrieveChunks
    . unpackChunkLists

-- | Download and deserialise the tree.
snapshotTree
    :: (MonadReader Env m, MonadState Progress m, MonadThrow m, MonadIO m)
    => Snapshot
    -> Stream' (Either (FileItem, ChunkList) OtherItem) m ()
snapshotTree
    = deserialiseTree
    . rechunkToTags
    . abortOnError decodeChunks
    . abortOnError retrieveChunks
    . unpackChunkLists
    . snapshotChunkLists

-- | Collect all chunks for the supplied snapshot, including those
-- comprising the actual snapshot tree itself.
collectChunks
    :: (MonadReader Env m, MonadState Progress m, MonadThrow m, MonadIO m)
    => Snapshot
    -> Stream' StoreID m ()
collectChunks snapshot
    = (S.map snd . unpackChunkLists . S.lefts $ snapshotTree snapshot) <>
      (S.map snd . unpackChunkLists $ snapshotChunkLists snapshot)

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
    logInfo $ "Listing chunks referenced by snapshots..."
    sinkChunks snapChunks ChunkCache.insert

    -- This is all referenced chunks from all repository snapshots
    -- (some may unfortunately have been moved to garbage, due to
    -- concurrent backups running during a prune).
    referenced  <- cacheSize

    -- List all chunks in repo and remove them from the cache.
    -- There are two sets we are interested in:
    -- 1) missing: chunks in cache, not in this stream
    -- 2) collectable: chunks in this stream, but not in cache
    logInfo $ "Listing all chunks in repository..."
    collectable S.:> _ <- count $ filterNotInChunkCache repoChunks
    missing <- cacheSize

    -- List all garbage chunks in repo.
    -- There are an additional three sets we are interested in:
    -- 1) referenced garbage: chunks in cache and in garbage stream
    -- 2) garbage (unreferenced): chunks not in cache, but in the stream
    -- 3) missing completely: chunks in cache, but not in stream
    logInfo $ "Listing all garbage chunks in repository..."
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
        withChunkCache $ \conn ->
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
    logInfo $ "Listing chunks referenced by snapshots..."
    sinkChunks snapChunks ChunkCache.insert

    -- List all chunks in repo and remove them from the cache.
    -- The cache should then contain missing chunks.
    logInfo $ "Listing all chunks in repository..."
    sinkChunks repoChunks ChunkCache.delete

    withChunkCache $ \conn -> do
        let missingStr = hoist liftIO $ ChunkCache.values conn
        flip S.mapM_ missingStr $ \storeID -> do
           e'present <- liftIO $ hasGarbageChunk repo storeID
           case e'present of
               Left err -> do
                   panic $ "Could not determine presence of garbage chunk " <> T.pack (show storeID)
                       <> " : " <> T.pack (show err)
               Right True -> do
                   logInfo $ "Restoring chunk from garbage: "  <> T.pack (show storeID)
                   res <- liftIO $ restoreGarbageChunk repo storeID
                   case res of
                       Left err -> logWarn $ "Could not restore garbage chunk " <> T.pack (show storeID)
                           <> " : " <> T.pack (show err)
                       Right () -> return ()
               Right False -> do
                   panic $ "Chunk is missing from the repository: " <> T.pack (show storeID)
                       <> " : Please verify the latest backups!"
  where
    snapChunks = S.concatMap (collectChunks . snd) $ listSnapshotsPartial repo
    repoChunks = hoist liftResourceT $ listChunks repo

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
       logInfo $ "Performing an exhaustive garbage collection."

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
   logInfo $ "Forcing a chunk repair..."
   chunkRepair repo

   t1  <- asks envStartTime
   ttl <- asks envGarbageExpiryDays

   logInfo $ "Finding and deleting expired garbage (expiry is " <> T.pack (show ttl) <> " days)..."
   deleted S.:> _ <-
       S.sum . flip S.mapM garbageChunks $ \storeID -> do
           e't0  <- liftIO $ getGarbageModTime repo storeID
           case e't0 of
               Left ex -> do
                   logWarn $ "Could not query modification time of garbage chunk " <> T.pack (show storeID)
                       <> " : " <> T.pack (show ex)
                   return 0
               Right t0 -> do
                   let diff = fromIntegral $ diffDays (utctDay t1) (utctDay t0)
                   if (diff >= ttl) -- expiry condition
                       then do
                           res <- liftIO $ deleteGarbageChunk repo storeID
                           case res of
                               Left ex -> do
                                   logWarn $ "Could not delete garbage chunk " <> T.pack (show storeID)
                                       <> " : " <> T.pack (show ex)
                                   return 0
                               Right () -> return (1::Integer)
                       else return 0

   logInfo $ "Garbage deleted: " <> T.pack (show deleted) <> " chunks."
  where
   garbageChunks = hoist liftResourceT $ listGarbageChunks repo

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
findGarbageIncremental repo deletedSnapshots = do
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
        when (n `mod` 100==0) $ putProgress $ "Chunks received: " ++ (show n)

-- | Check that the the on-disk chunk cache state is empty, as expected.
assertCacheEmpty
  :: (MonadReader Env m, MonadResource m)
  => m ()
assertCacheEmpty = do
    size <- withChunkCache $ liftIO . ChunkCache.size
    when (size /= 0) $ panic $ "Assertion failed! Chunk cache is not empty."

------------------------------------------------------------
-- Supporting stream transformers and utilities.


-- | Real-time progress console output.
-- NOTE: we write progress to stderr to get automatic flushing
-- and to make it possible to use pipes over stdout if needed.
progressMonitor
    :: (MonadState Progress m, MonadReader Env m, MonadIO m)
    => Stream' a m r
    -> m r
progressMonitor = S.mapM_ $ \_ -> do
    Progress{..} <- get
    startT       <- asks envStartTime
    nowT         <- liftIO getCurrentTime
    putProgress $ unwords $ List.intersperse " | "
        [ "Files: "         ++ show _prFiles
        , "Chunks: "        ++ show _prChunks
        , "In: "            ++ show (_prInputSize  `div` megabyte) ++ " MB"
        , "Out (dedup): "   ++ show (_prDedupSize  `div` megabyte) ++ " MB"
        , "Out (stored):  " ++ show (_prStoredSize `div` megabyte) ++ " MB"
        , "Rate: "          ++ show (rate _prInputSize (nowT `diffUTCTime` startT)) ++ " MB/s"
        , "Errors: "        ++ show (_prErrors)
        ]
  where
    rate bytes ndt = round $ (toRational $ bytes `div` megabyte) / (toRational ndt) :: Int
    megabyte = 1024*1024

overFileItems
    :: Monad m
    => (b -> FileItem)
    -> StreamF a b (Stream (Of OtherItem) m) r
    -> StreamF (Either a OtherItem) (Either b OtherItem) m r
overFileItems getFileItem f =
    S.reinterleaveRights fileElems otherElems . S.left f
  where
    fileElems = pathElems . filePath . getFileItem
    otherElems (DirItem item)    = pathElems (filePath item)
    otherElems (LinkItem item _) = pathElems (filePath item)

-- | Report errors during restore and log affected files.
-- Perhaps in the future we can record broken files and missing chunks
-- then we can give them special names in saveFiles? For now, just abort.
handleErrors
    :: (MonadReader Env m, MonadState Progress m)
    => Stream' (Either (Error FileItem) (PlainChunk FileItem)) m r
    -> Stream' (PlainChunk FileItem) m r
handleErrors = S.mapM $ \case
    Left Error{..} ->
        panic $ "Error during restore: "
            <> T.pack (show errKind)
            <> maybe mempty (T.pack . show) errCause
    Right chunk    -> return chunk

-- | Apply the supplied stream transform @f@ to the inserts
-- and changes only.
overChangedFiles
    :: forall m c r. Monad m
    => StreamF FileItem (FileItem, c) (Stream (Of (FileItem, c)) m) r
    -> Stream' (Diff (FileItem, c) FileItem) m r
    -> Stream' (FileItem, c) m r
overChangedFiles f
   = S.mergeEither
   . S.reinterleaveRights fileElems fileElems
   . S.left f
   . fromDiffs
  where
    fileElems = pathElems . filePath . fst

    -- Left values need the full chunk, encode, upload pipeline;
    -- Right values are unchanged and already have a chunk list.
    fromDiffs = S.catMaybes . S.map g
      where
        g :: Diff (FileItem, c) FileItem
          -> Maybe (Either FileItem (FileItem, c))
        g (Keep x)   = Just $ Right x
        g (Insert y) = Just $ Left y
        g (Change y) = Just $ Left y
        g (Delete _) = Nothing

-- | Hash the supplied stream of chunks using multiple cores.
hashChunks
  :: (MonadReader Env m, MonadIO m)
  => Stream' (RawChunk (TaggedOffsets t) B.ByteString) m r
  -> Stream' (PlainChunk t) m r
hashChunks str = do
    Env{..} <- lift ask
    let Manifest{..} = repoManifest envRepository
    flip (S.parMap envTaskBufferSize envTaskGroup) str $ \c ->
        liftIO $ return $! hashChunk mStoreIDKey c

-- | Separate out duplicate chunks.
-- Unseen chunks are passed on the left, known duplicates on the right.
-- Uses an on-disk persistent chunks cache for de-duplication.
dedupChunks
  :: (MonadReader Env m, MonadState Progress m, MonadResource m)
  => Stream' (PlainChunk t) m r
  -> Stream' (Either (PlainChunk t) (TaggedOffsets t, StoreID)) m r
dedupChunks str = do
    withChunkCache $ \conn ->
        flip S.mapM str $ \c@Chunk{..} -> do
            -- collect some statistics
            let chunkSize = fromIntegral $ B.length cContent
            modify $ over prChunks    succ
                   . over prInputSize (+ chunkSize)
            -- query chunk cache
            isDuplicate <- liftIO $ ChunkCache.member conn cStoreID
            if isDuplicate
               then -- duplicate
                   return $ Right (cOffsets, cStoreID)
               else
                   return $ Left c

-- | Compress and encrypt the supplied stream of chunks using multiple cores.
encodeChunks
  :: (MonadReader Env m, MonadState Progress m, MonadIO m)
  => Stream' (PlainChunk t) m r
  -> Stream' (CipherChunk t) m r
encodeChunks str = do
    Env{..} <- lift ask
    let Manifest{..} = repoManifest envRepository
    flip (S.parMap envTaskBufferSize envTaskGroup) str $
        liftIO . encryptChunk mChunkKey
               . compressChunk

-- | Store (upload) a stream of chunks using multiple cores.
storeChunks
    :: forall m t r. (Show t, MonadState Progress m, MonadReader Env m, MonadResource m)
    => Stream' (CipherChunk t) m r
    -> Stream' (TaggedOffsets t, StoreID) m r
storeChunks str = do
    Env{..} <- lift ask
    withChunkCache $ \conn ->
        S.mapM_ (liftIO . ChunkCache.insert conn . snd)
              . S.copy
              . S.mapM measure
              . S.parMap envTaskBufferSize envTaskGroup (storeChunk envRetries envRepository)
              $ str
  where
    measure :: (CipherChunk t, Bool) -> m (TaggedOffsets t, StoreID)
    measure (cc@Chunk{..}, isDuplicate) = do
        unless isDuplicate $ do
            measureStoredSize cc
            modify $ over prDedupSize (+ maybe 0 fromIntegral cOriginalSize)
        return (cOffsets, cStoreID)

-- | Store a ciphertext chunk (likely an upload to a remote repo).
-- NOTE: this throws a fatal error if it cannot successfully upload a
-- chunk after exceeding the retry limit.
storeChunk :: Int -> Repository -> CipherChunk t -> IO (CipherChunk t, Bool)
storeChunk retries repo cc@Chunk{..} = do
    res <- retryWithExponentialBackoff retries $ do
        logDebug $ "Storing chunk " <> T.pack (show cStoreID)
        putChunk repo cStoreID cContent
    case res of
        Left (ex :: SomeException) ->
            panic $ "Failed to store chunk : " <> T.pack (show ex) -- fatal abort
        Right isDuplicate ->
            return (cc, isDuplicate)

-- | Retrieve (download) a stream of chunks using multiple cores.
retrieveChunks
    :: forall m t r. (MonadState Progress m, MonadReader Env m, MonadIO m, Show t)
    => Stream' (TaggedOffsets t, StoreID) m r
    -> Stream' (Either (Error t) (CipherChunk t)) m r  -- ^ assume downloading may fail
retrieveChunks str = do
    Env{..} <- lift ask
    S.mapM measure
        $ S.parMap envTaskBufferSize envTaskGroup (retrieveChunk envRetries envRepository) str
  where
    measure :: (Either (Error t) (CipherChunk t)) -> m (Either (Error t) (CipherChunk t))
    measure (Right cc) = measureStoredSize cc >> return (Right cc)
    measure (Left e)   = return $ Left e

-- | Retrieve a ciphertext chunk (likely a download from a remote repo).
-- NOTE: we do not log errors here, instead we return them for aggregation elsewhere.
retrieveChunk
    :: Show t
    => Int
    -> Repository
    -> (TaggedOffsets t, StoreID)
    -> IO (Either (Error t) (CipherChunk t))
retrieveChunk retries repo (offsets, storeID) = do
    logDebug $ "Retrieving chunk " <> T.pack (show storeID)
    res <- retryWithExponentialBackoff retries $ getChunk repo storeID
    return $ (mkError +++ mkCipherChunk) $ res
  where
    mkCipherChunk = Chunk storeID offsets Nothing
    mkError = Error RetrieveError offsets storeID . Just

rechunkCDC
  :: (MonadReader Env m, Eq t)
  => Stream' (RawChunk t B.ByteString) m r
  -> Stream' (RawChunk (TaggedOffsets t) B.ByteString) m r
rechunkCDC str = asks (repoManifest . envRepository) >>= \Manifest{..} ->
    CDC.rechunkCDC mCDCKey mCDCParams str

writeFilesCache
  :: (MonadReader Env m, MonadResource m)
  => Stream' (FileItem, ChunkList) m r
  -> Stream' (FileItem, ChunkList) m r
writeFilesCache str = do
    cacheFile <- genFilesCacheName "files.tmp" >>= resolveCacheFileName
    sourceDir <- asks envDirectory
    writeCacheFile cacheFile
        . relativePaths sourceDir
        . S.map (uncurry FileCacheEntry)
        $ S.copy str

readFilesCache
  :: (MonadReader Env m, MonadResource m)
  => Stream' (FileItem, ChunkList) m ()
readFilesCache = do
    cacheFile <- genFilesCacheName "files" >>= resolveCacheFileName
    sourceDir <- asks envDirectory
    S.map (ceFileItem &&& ceChunkList)
        . absolutePaths sourceDir
        $ readCacheFile cacheFile

readLastSnapshotKey
    :: (MonadReader Env m, MonadIO m)
    => m (Maybe SnapshotName)
readLastSnapshotKey = do
    fp <- genFilesCacheName "last_snapshot" >>= resolveCacheFileName'
    liftIO $ do
        exists <- Dir.doesFileExist fp
        if exists
            then Just <$> T.readFile fp
            else return Nothing

writeLastSnapshotKey
    :: (MonadReader Env m, MonadIO m)
    => SnapshotName
    -> m()
writeLastSnapshotKey key = do
    fp <- genFilesCacheName "last_snapshot" >>= resolveCacheFileName'
    liftIO $ T.writeFile fp key

-- Generate a file cache file name from the absolute path of the source directory.
genFilesCacheName :: MonadReader Env m => RawName -> m RawName
genFilesCacheName prefix = do
    name  <- Builder.toLazyByteString . Builder.byteStringHex . getRawFilePath
                 <$> asks envDirectory
    return $ prefix <> "." <> LB.toStrict name

-- NOTE: We only commit updates to the file cache if the entire backup completes.
commitFilesCache :: (MonadIO m, MonadCatch m, MonadReader Env m) => m ()
commitFilesCache = do
    cacheFile  <- genFilesCacheName "files.tmp" >>= resolveCacheFileName'
    cacheFile' <- genFilesCacheName "files" >>= resolveCacheFileName'
    res <- try $ liftIO $ Dir.renameFile cacheFile cacheFile'
    case res of
        Left (ex :: SomeException) -> panic $ "Failed to update cache file: " <> (T.pack $ show ex)
        Right () -> return ()

resolveTempFileName
    :: (MonadReader Env m, MonadIO m)
    => RawName
    -> m (Path Abs File)
resolveTempFileName name = ask >>= \Env{..} -> liftIO $ do
    Dir.createDirectoryIfMissing True =<< getFilePath envTempPath
    return $ makeFilePath envTempPath name

resolveTempFileName'
    :: (MonadReader Env m, MonadIO m)
    => RawName
    -> m FilePath
resolveTempFileName' name =
    resolveTempFileName name >>= liftIO . getFilePath

cleanUpTempFiles :: (MonadReader Env m, MonadIO m) => m ()
cleanUpTempFiles = ask >>= \Env{..} -> liftIO $ do
    Dir.removeDirectoryRecursive =<< getFilePath envTempPath

resolveCacheFileName
    :: (MonadReader Env m, MonadIO m)
    => RawName
    -> m (Path Abs File)
resolveCacheFileName name = ask >>= \Env{..} ->
    liftIO $ mkCacheFileName envCachePath (repoURL envRepository) name

resolveCacheFileName'
    :: (MonadReader Env m, MonadIO m)
    => RawName
    -> m FilePath
resolveCacheFileName' name =
    resolveCacheFileName name >>= liftIO . getFilePath

mkCacheFileName :: Path Abs Dir -> Text -> RawName -> IO (Path Abs File)
mkCacheFileName cachePath repoURL name = do
    let dir = pushDir cachePath (T.encodeUtf8 $ URI.encodeText repoURL)
    Dir.createDirectoryIfMissing True =<< getFilePath dir
    return $ makeFilePath dir name

-- | Group tagged-offsets and store IDs into distinct ChunkList per tag.
packChunkLists
    :: forall m t r. (Eq t, Monad m)
    => Stream' (TaggedOffsets t, StoreID) m r
    -> Stream' (t, ChunkList) m r
packChunkLists = groupByTag mkChunkList
  where
    mkChunkList :: Seq (StoreID, Offset) -> ChunkList
    mkChunkList s = case Seq.viewl s of
        (storeID, offset) Seq.:< rest -> ChunkList (storeID Seq.<| fmap fst rest) offset
        _ -> ChunkList mempty 0

makeSnapshot
    :: (MonadReader Env m, MonadFail m, MonadIO m)
    => Stream' (Tree, ChunkList) m r
    -> m Snapshot
makeSnapshot str = do
    ((Tree, chunkList):_) :> _ <- S.toList str
    hostDir <- asks envDirectory
    startT  <- asks envStartTime
    repo    <- asks envRepository
    binary  <- asks envBackupBinary
    liftIO $ do
        mBinaryID <- if binary
            then storeProgramBinary repo
            else return Nothing
        user    <- T.pack <$> User.getLoginName
        host    <- T.pack <$> getHostName
        uid     <- User.getRealUserID
        gid     <- User.getRealGroupID
        finishT <- getCurrentTime
        return $ Snapshot
            { sUserName   = user
            , sHostName   = host
            , sHostDir    = hostDir
            , sUID        = uid
            , sGID        = gid
            , sStartTime  = startT
            , sFinishTime = finishT
            , sTree       = chunkList
            , sExeBinary  = mBinaryID
            }

storeProgramBinary :: Repository -> IO (Maybe StoreID)
storeProgramBinary repo = do
    e'res <- putProgramBinary repo
    case e'res of
        Left err -> do
            logWarn $ "Failed to store program binary: " <> T.pack (show err)
            return Nothing
        Right storeID ->
            return $ Just storeID

snapshotChunkLists
    :: Monad m
    => Snapshot
    -> Stream' (Tree, ChunkList) m ()
snapshotChunkLists = yield . (Tree,) . sTree

unpackChunkLists
    :: forall m t r. (Eq t, Monad m)
    => Stream' (t, ChunkList) m r
    -> Stream' (TaggedOffsets t, StoreID) m r
unpackChunkLists
  = S.map swap
  . S.aggregateByKey extractStoreIDs
  where
    extractStoreIDs :: (t, ChunkList) -> Seq (StoreID, Seq (t, Offset))
    extractStoreIDs (t, ChunkList ids offset) = case Seq.viewl ids of
        storeID Seq.:< rest ->
            (storeID, Seq.singleton (t, offset)) Seq.<| fmap (,mempty) rest
        _ -> mempty

    swap (a, b) = (b, a)

-- | Subsequent offsets are used as end-markers, so if they are not provided
-- (e.g. a partial restore), the final raw chunk for each tag will need to be trimmed.
trimChunks
    :: forall m r . MonadIO m
    => Stream' (RawChunk FileItem B.ByteString) m r
    -> Stream' (RawChunk FileItem B.ByteString) m r
trimChunks
    = S.concats
    . S.maps doFileItem
    . S.groupBy ((==) `on` rcTag)
  where
    doFileItem = flip S.mapAccum_ 0 $ \accumSize (RawChunk item bs) ->
      let accumSize' = accumSize + fromIntegral (B.length bs)
      in if fileSize item < accumSize'
         then (0, RawChunk item $ B.take (fromIntegral $ fileSize item - accumSize) bs)
         else (accumSize', RawChunk item bs)

-- | Decode chunks using multiple cores.
decodeChunks
    :: (MonadReader Env m, MonadState Progress m, MonadIO m)
    => Stream' (CipherChunk t) m r
    -> Stream' (Either (Error t) (PlainChunk t)) m r   -- ^ assume decoding may fail
decodeChunks str = do
    Env{..} <- lift ask
    let Manifest{..} = repoManifest envRepository
    flip (S.parMap envTaskBufferSize envTaskGroup) str $ \cc ->
        return
            . maybe (Left $ toError cc) Right
            . fmap decompressChunk
            . decryptChunk mChunkKey
            $ cc
  where
    toError Chunk{..} = Error DecryptError cOffsets cStoreID Nothing

-- | We cannot survive errors retrieving the snapshot metadata.
abortOnError
    :: (MonadThrow m, Exception e)
    => (Stream' a m r -> Stream' (Either e b) m r)
    -> Stream' a m r
    -> Stream' b m r
abortOnError f str =
    S.mapM (either throwM return) $ f str -- TODO log also

verifyChunks
    :: (MonadState Progress m, MonadReader Env m)
    => Stream' (PlainChunk t) m r
    -> Stream' (Either (Error t) (PlainChunk t)) m r
verifyChunks str = do
    Manifest{..} <- lift . asks $ repoManifest . envRepository
    S.mapM (measure . (toError +++ id) . verify mStoreIDKey) str
  where
    toError :: VerifyFailed t -> Error t
    toError (VerifyFailed Chunk{..}) =
        Error VerifyError cOffsets cStoreID Nothing

    -- to support the progress monitor
    measure e'chunk = do
        modify $ over prFiles     (+ fromIntegral (either (const 0) offsets e'chunk))
               . over prChunks    (+ 1)
               . over prInputSize (+ fromIntegral (either (const 0) sizeOf e'chunk))
               . over prErrors    (+ either (const 1) (const 0) e'chunk)
        return e'chunk

    offsets c = length (cOffsets c)
    sizeOf  c = B.length (cContent c)

newtype VerifyResult = VerifyResult
    { vrErrors :: Seq (Error FileItem)
    } deriving (Show, Semigroup, Monoid)

summariseErrors
    :: (MonadState Progress m, MonadIO m)
    => Stream' (Either (Error FileItem) (PlainChunk FileItem)) m r
    -> Stream' (FileItem, VerifyResult) m r
summariseErrors
  = groupByTag (foldMap fst)
  . S.mergeEither
  . S.map (fromError +++ fromChunk)
  where
    fromError e = (errOffsets e, VerifyResult $ Seq.singleton e)
    fromChunk c = (cOffsets c,   VerifyResult Seq.empty)

-- | Measure the stored size (encrypted and compressed size) of CipherChunk
measureStoredSize
    :: MonadState Progress m
    => CipherChunk t
    -> m ()
measureStoredSize Chunk{..} = do
    let encodedSize = fromIntegral $ B.length (cSecretBox cContent)
    modify $ over prStoredSize (+ encodedSize)

-- | Run an action using a temporary chunk cache
withChunkCache
  :: (MonadReader Env m, MonadResource m)
  => (ChunkCache.Connection -> m r)
  -> m r
withChunkCache f = do
    cacheFile   <- T.pack <$> resolveTempFileName' "chunks"
    (key, conn) <- allocate (ChunkCache.connect cacheFile) ChunkCache.close
    r <- f conn
    release key
    return r

listSnapshotsPartial :: MonadResource m => Repository -> Stream' (SnapshotName, Snapshot) m ()
listSnapshotsPartial repo =
    S.map (fmap $ either err id) . hoist liftResourceT $ listSnapshots repo
  where
    err ex = panic $ "Failed to fetch snapshot: " <> T.pack (show ex)
