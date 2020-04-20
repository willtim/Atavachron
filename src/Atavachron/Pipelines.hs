{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE BangPatterns #-}
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
{-# OPTIONS_GHC -Wno-name-shadowing #-}

-- | Stream pipelines for backup, restore, verify etc.
--

module Atavachron.Pipelines where

import Prelude hiding (concatMap)

import Codec.Serialise

import Control.Arrow ((+++),first,second)
import Control.Monad
import Control.Monad.Catch
import Control.Monad.IO.Class
import Control.Monad.Morph
import Control.Monad.Reader.Class
import Control.Monad.Trans.Resource

import Data.Function (on)

import qualified Data.ByteString as B
import qualified Data.ByteString.Short as SB
import Data.Int
import Data.IORef
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.IO as T
import Data.Time.Clock

import qualified Data.List as List

import Data.Sequence (Seq)
import qualified Data.Sequence as Seq

import Streaming (Stream, Of(..))
import Streaming.Prelude (yield)
import qualified Streaming as S
import qualified Streaming.Prelude as S hiding (mapM_)

import qualified System.Posix.User as User
import qualified System.Directory as Dir

import Numeric.Natural
import Network.HostName (getHostName)
import qualified Network.URI.Encode as URI

import Atavachron.Repository
import Atavachron.Chunk.Builder
import qualified Atavachron.Chunk.CDC as CDC
import Atavachron.Chunk.Encode

import Atavachron.IO
import Atavachron.Logging
import Atavachron.Path

import Atavachron.Streaming (Stream')
import qualified Atavachron.Streaming as S

import Atavachron.Env
import qualified Atavachron.Executor as Executor
import Atavachron.Tree
import Atavachron.Files

import qualified Atavachron.DB.MMap as MMap
import qualified Atavachron.DB.MSet as MSet
import Atavachron.DB.MMap (MMap)
import Atavachron.DB.MSet (MSet)
import Atavachron.DB as DB


type ItemID  = Int64
type ChunkID = Int64

type ItemsMap      = MMap ItemID (TreeEntry () Rel)
type FileChunksMap = MMap ItemID ChunkList
type TasksMap t    = MMap ChunkID (TaggedOffsets t, StoreID)
type ChunksSet     = MSet StoreID
type RestoreMap    = MMap ItemID (FileChunks Rel)

------------------------------------------------------------
-- Pipelines

-- | Full and incremental backup.
backupPipeline
    :: forall m. (MonadReader Env m, MonadMask m, MonadResource m)
    => Path Abs Dir
    -> Bool
    -> m Snapshot
backupPipeline path forceFullScan = do
    when forceFullScan $ logInfo "Forcing full scan."
    cachePath <- genFilePath "cache.db"
    -- NOTE: connecting takes an exclusive lock on the cache file.
    (_, conn) <- allocate (DB.connect cachePath) DB.disconnect

    -- Wrap the entire backup process in a transaction.
    DB.withTx conn $ do

      previousFiles <- readPreviousFiles conn forceFullScan

      MMap.withEmptyMap conn "items" $ \(itemsMap    :: ItemsMap) ->
        MMap.withEmptyMap conn "files" $ \(fileChunksMap :: FileChunksMap) -> do

        -- upload chunks as we recurse the filesystem
        logInfo "Uploading file chunks ..."

        sourceDir <- asks envDirectory

        -- NOTE: we save all items into a disk cache so that we can upload
        -- chunks in any order.
        uploadedStr <- uploadPipeline conn                -- upload chunks and then stream
                     . readFiles                          -- read changed files from local filesystem
                     . MMap.insertMany fileChunksMap      -- write unchanged file chunks
                     . partitionDiff                      -- outer stream is unchanged, inner is changed
                     . diff fst snd previousFiles         -- diff with previous files cache
                     . filterItems (mapM extractFileItem) -- select only the file items from stream
                     . MMap.insertMany itemsMap           -- save items into cache
                     . relativePaths second snd sourceDir -- relativise paths before saving to disk
                     . S.copy
                     . S.number                           -- assign sequence number
                     . filterWithEnvPredicate             -- filter out unwanted items
                     $ recurseDir path                    -- recurse local filesystem

        -- by this point upload is complete
        MMap.insertMany fileChunksMap uploadedStr

        -- writing metadata
        logInfo "Uploading snapshot metadata ..."

        -- stream through the entries again to make the snapshot
        treeStr    <- uploadPipeline conn
                    . serialiseTree
                    . hoist liftIO
                    . S.mapM (lookupChunkList fileChunksMap)
                    $ MMap.toStream itemsMap

        retainProgress
        makeSnapshot treeStr

-- | Verify files and their chunks.
verifyPipeline
    :: (MonadReader Env m, MonadThrow m, MonadIO m, MonadResource m)
    => Snapshot
    -> Stream' (FileItem, VerifyResult) m ()
verifyPipeline
  = summariseErrors
  . downloadPipeline
  . filterItems extractChunkLists
  . filterWithEnvPredicate
  . snapshotTree

-- | Restore (using the FilePredicate in the environment).
restorePipeline
    :: (MonadReader Env m, MonadThrow m, MonadMask m, MonadResource m)
    => Snapshot
    -> m ()
restorePipeline snapshot = do
    cachePath <- genFilePath "cache.db"
    sourceDir <- asks envDirectory
    -- NOTE: connecting takes an exclusive lock on the cache file.
    (_, conn) <- allocate (DB.connect cachePath) DB.disconnect

    DB.withTx conn $

      MMap.withEmptyMap conn "restore_tmp" $ \(restoreMap :: RestoreMap) -> do

        logInfo "Downloading snapshot ..."

        -- write file chunk lists into table
        MMap.insertMany restoreMap
          . relativePaths second snd sourceDir
          . S.number
          . saveOther               -- restore directories and links
          . partitionEntries
          . filterWithEnvPredicate
          . snapshotTree
          $ snapshot

        logInfo "Downloading files ..."

        -- download files (in parallel)
        saveFiles
          . trimChunks
          . rechunkToTags
          . handleErrors
          . downloadPipeline
          . hoist liftIO
          . S.map unFileChunks
          . absolutePaths id id sourceDir
          $ MMap.elems restoreMap

        -- clean-up cache
        liftIO $ MMap.clear restoreMap

-- | The complete upload pipeline: CDC chunking, encryption,
-- compression and upload to a remote location.
uploadPipeline
  :: forall m t. (MonadReader Env m, MonadResource m, MonadMask m, Serialise t, Eq t, Show t)
  => DB.Connection
  -> Stream' (RawChunk t B.ByteString) m ()
  -> m (Stream' (t, ChunkList) m ())
uploadPipeline conn str = do
    env@Env{..} <- ask
    let Manifest{..} = repoManifest envRepository

    MSet.withEmptySet conn "chunks" $ \chunksSet ->  -- TODO re-use if switch set?
        MMap.withEmptyMap conn "tasks" $ \(tasksMap :: TasksMap t) -> do

            workers  <- liftIO $ Executor.new envTaskGroup envTaskBufferSize

            -- render progress every 500ms
            liftIO $ Executor.scheduleAtFixedRate workers 500 (progressMonitor env)

            let upload = liftIO
                       . Executor.submit_ workers
                       . uploadTask env tasksMap chunksSet

            -- Drain stream and create up to envTaskBufferSize pending tasks
            _ <-    S.mapM_ upload -- blocks if queue is full
                  . S.number
                  . rechunkCDC env
                  $ str

            logDebug "Waiting for workers ..."

            liftIO $ Executor.shutdownAndWait workers

            logDebug "Packing chunk lists ..."

            -- we need to stream through the results in-order and run
            -- it though "packChunkLists" to get the stream of ChunkLists.
            return $
                packChunkLists . S.map snd
                               $ allocate (liftIO $ MMap.open conn "tasks") (liftIO . MMap.close)
                                     >>= \(_, m :: TasksMap t) -> hoist liftIO $ MMap.toStream m
  where
    uploadTask
        :: Env
        -> TasksMap t
        -> ChunksSet
        -> (ChunkID, RawChunk (TaggedOffsets t) B.ByteString)
        -> IO ()
    uploadTask env@Env{..} tasksMap chunksSet (i, rawChunk) = do
        let Manifest{..}  = repoManifest envRepository
        let !pc@Chunk{..} = hashChunk mStoreIDKey rawChunk
        -- query chunk set
        isDup <- liftIO $ MSet.member chunksSet cStoreID
        if isDup
            then
                void $ updateProgress envProgressRef (mempty{prChunks=1
                                                            ,prInputSize=sizeOf pc})
            else do
                cc    <- encodeChunk env pc
                isDup <- storeChunk envRetries envRepository cc
                void $ updateProgress envProgressRef  (measure isDup cc)
        MMap.insert tasksMap i (cOffsets, cStoreID)

    measure :: Bool -> CipherChunk t -> Progress
    measure isDup cc@Chunk{..} =
        mempty { prChunks     = 1
               , prInputSize  = maybe 0 fromIntegral cOriginalSize
               , prStoredSize = if isDup then 0 else storedSize cc
               , prDedupSize  = if isDup then 0 else maybe 0 fromIntegral cOriginalSize
               }

    sizeOf  = fromIntegral . B.length . cContent


-- | A download pipeline that does not abort on errors.
downloadPipeline
    :: (MonadReader Env m, MonadThrow m, MonadIO m, MonadResource m, Eq t, Show t)
    => Stream' (t, ChunkList) m ()
    -> Stream' (Either (Error t) (PlainChunk t)) m ()
downloadPipeline str = do
    env@Env{..}  <- lift ask
    (_, workers) <- allocate (liftIO $ Executor.new envTaskGroup envTaskBufferSize)
                             (liftIO . Executor.shutdown)

    -- render progress every 500ms
    liftIO $ Executor.scheduleAtFixedRate workers 500 (progressMonitor env)

    S.parMap envTaskBufferSize workers (downloadTask env)
        . unpackChunkLists
        $ str
  where
    downloadTask Env{..} k = do
        let m = repoManifest envRepository
        e'cc <- retrieveChunk envRetries envRepository k
        let !e'pc = e'cc >>= decodeChunk m >>= verifyChunk m
        void $ updateProgress envProgressRef $ mempty
            { prFiles     = either (const 0) offsets e'cc
            , prChunks    = 1
            , prInputSize = either (const 0) sizeOf e'pc
            , prErrors    = either (const 1) (const 0) e'cc
            }
        return e'pc

    offsets = fromIntegral . length . cOffsets
    sizeOf  = fromIntegral . B.length . cContent

-- | Decode a chunk, which may fail.
decodeChunk
    :: Manifest
    -> CipherChunk t
    -> Either (Error t) (PlainChunk t) -- ^ assume decoding may fail
decodeChunk Manifest{..} cc =
    maybe (Left $ toError cc) (Right . decompressChunk)
        . decryptChunk mChunkKey
       $! cc
  where
    toError Chunk{..} = Error DecryptError cOffsets cStoreID Nothing

-- | Download and deserialise the tree.
snapshotTree
    :: (MonadReader Env m, MonadThrow m, MonadIO m, MonadResource m)
    => Snapshot
    -> Stream' (TreeEntry ChunkList Abs) m ()
snapshotTree
    = deserialiseTree
    . rechunkToTags
    . abortOnError downloadPipeline
    . snapshotChunkLists

-- | Collect all chunks for the supplied snapshot, including those
-- comprising the actual snapshot tree itself.
collectChunks
    :: (MonadReader Env m, MonadThrow m, MonadIO m, MonadResource m)
    => Snapshot
    -> Stream' StoreID m ()
collectChunks snapshot
    = (S.map snd . unpackChunkLists . filterItems extractChunkLists $ snapshotTree snapshot) <>
      (S.map snd . unpackChunkLists $ snapshotChunkLists snapshot)

-- | Real-time progress console output.
-- NOTE: we write progress to stderr to get automatic flushing
-- and to make it possible to use pipes over stdout if needed.
progressMonitor
    :: Env
    -> IO ()
progressMonitor Env{..} = do
    let startT   = envStartTime
    nowT         <- getCurrentTime
    Progress{..} <- readIORef envProgressRef
    putProgress $ unwords $ List.intersperse " | "
        [ "Files: "         ++ show prFiles
        , "Chunks: "        ++ show prChunks
        , "In: "            ++ show (prInputSize  `div` megabyte) ++ " MB"
        , "Out (dedup): "   ++ show (prDedupSize  `div` megabyte) ++ " MB"
        , "Out (stored):  " ++ show (prStoredSize `div` megabyte) ++ " MB"
        , "Rate: "          ++ show (rate prInputSize (nowT `diffUTCTime` startT)) ++ " MB/s"
        , "Errors: "        ++ show prErrors
        ]
  where
    rate bytes ndt = round $ toRational (bytes `div` megabyte) / toRational ndt :: Int
    megabyte = 1024*1024

readPreviousFiles :: (MonadIO m, MonadReader Env m) => Connection -> Bool -> m (Stream' (FileItem, ChunkList) m ())
readPreviousFiles conn forceFullScan
    | forceFullScan = return mempty
    | otherwise     = do
          sourceDir <- asks envDirectory
          liftIO $ do
              DB.renameTable conn "items" "items_prev"
              DB.renameTable conn "files" "files_prev"
          -- NOTE: we must stream through all items in order to convert
          -- out of relative paths (relative to the prior entry).
          let str :: Stream' (TreeEntry () Rel, Maybe ChunkList) IO ()
                   = MMap.leftOuterJoin conn "items_prev" "files_prev"
          return . hoist liftIO
                 . S.catMaybes
                 . S.map extractChunkLists'
                 . absolutePaths first fst sourceDir
                 $ str
  where
    extractChunkLists' (FileEntry item (), Just chunks) = Just (item, chunks)
    extractChunkLists' _ = Nothing

lookupChunkList
    :: FileChunksMap
    -> (ItemID, TreeEntry () Rel)
    -> IO (TreeEntry ChunkList Rel)
lookupChunkList fileChunksMap (i, entry) =
    case entry of
        FileEntry item ()     -> do
            mcl <- MMap.lookup fileChunksMap i
            case mcl of
                Nothing -> error "Upload task failed!" -- TODO how to propogate errors?
                Just cl -> return $ FileEntry item cl
        DirEntry item         -> return $ DirEntry item
        LinkEntry item target -> return $ LinkEntry item target

filterItems
    :: Monad m
    => (a -> Maybe b)
    -> Stream' a m r
    -> Stream' b m r
filterItems f = S.catMaybes . S.map f

-- | Report errors during restore and log affected files.
-- Perhaps in the future we can record broken files and missing chunks
-- then we can give them special names in saveFiles? For now, just abort.
handleErrors
    :: (MonadReader Env m)
    => Stream' (Either (Error FileItem) (PlainChunk FileItem)) m r
    -> Stream' (PlainChunk FileItem) m r
handleErrors = S.mapM $ \case
    Left Error{..} ->
        panic $ "Error during restore: "
            <> T.pack (show errKind)
            <> maybe mempty (T.pack . show) errCause
    Right chunk    -> return chunk

-- | Partition diff results into unchanged and changed files.
-- The outer stream is the unchanged files, the inner the changed.
partitionDiff
    :: forall m c r. Monad m
    => Stream' (Diff (FileItem, c) (ItemID, FileItem)) m r
    -> Stream (Of (ItemID, c)) (Stream (Of (ItemID, FileItem)) m) r
partitionDiff
   = S.separate
   . S.maps S.eitherToSum
   . fromDiffs
  where
    -- Left values need the full chunk, encode, upload pipeline;
    -- Right values are unchanged and already have a chunk list.
    fromDiffs = S.catMaybes . S.map g
      where
        g :: Diff (FileItem, c) (Int64, FileItem)
          -> Maybe (Either (ItemID, c) (ItemID, FileItem))
        g (Keep (_,c) (i,_)) = Just $ Left (i, c)
        g (Insert y)         = Just $ Right y
        g (Change _ y)       = Just $ Right y
        g (Delete _)         = Nothing

-- | Partition tree entries into "other" items (non-regular files) and file items.
-- The outer stream is other items, the inner is file items.
partitionEntries
    :: forall m r. Monad m
    => Stream' (TreeEntry ChunkList Abs) m r
    -> Stream (Of OtherItem) (Stream (Of (FileChunks Abs)) m) r
partitionEntries
   = S.separate
   . S.maps S.eitherToSum
   . S.map fromTreeEntry
  where
    fromTreeEntry (FileEntry item chunks) = Right $ FileChunks (item, chunks)
    fromTreeEntry (LinkEntry item target) = Left $ LinkItem item target
    fromTreeEntry (DirEntry item)         = Left $ DirItem item

-- | Compress and encrypt the supplied chunk.
encodeChunk
  :: Env
  -> PlainChunk t
  -> IO (CipherChunk t)
encodeChunk Env{..}
    = encryptChunk mChunkKey
    . compressChunk
  where
    Manifest{..} = repoManifest envRepository

-- | Store a ciphertext chunk (likely an upload to a remote repo).
-- NOTE: this throws a fatal error if it cannot successfully upload a
-- chunk after exceeding the retry limit.
storeChunk :: MonadIO m => Natural -> Repository -> CipherChunk t -> m Bool
storeChunk retries repo Chunk{..} = liftIO $ do
    res <- retryWithExponentialBackoff retries $ do
        logDebug $ "Storing chunk " <> T.pack (show cStoreID)
        putChunk repo cStoreID cContent
    case res of
        Left (ex :: SomeException) ->
            panic $ "Failed to store chunk : " <> T.pack (show ex) -- fatal abort
        Right isDuplicate ->
            return isDuplicate

-- | Retrieve a ciphertext chunk (likely a download from a remote repo).
-- NOTE: we do not log errors here, instead we return them for aggregation elsewhere.
retrieveChunk
    :: Show t
    => Natural
    -> Repository
    -> (TaggedOffsets t, StoreID)
    -> IO (Either (Error t) (CipherChunk t))
retrieveChunk retries repo (offsets, storeID) = do
    logDebug $ "Retrieving chunk " <> T.pack (show storeID)
    res <- retryWithExponentialBackoff retries $ getChunk repo storeID
    return $ (mkError +++ mkCipherChunk) res
  where
    mkCipherChunk = Chunk storeID offsets Nothing
    mkError = Error RetrieveError offsets storeID . Just

rechunkCDC
  :: (Monad m, Eq t)
  => Env
  -> Stream' (RawChunk t B.ByteString) m r
  -> Stream' (RawChunk (TaggedOffsets t) B.ByteString) m r
rechunkCDC Env{..} = CDC.rechunkCDC mCDCKey mCDCParams
  where
    Manifest{..} = repoManifest envRepository

filterWithEnvPredicate
    :: forall m c r. (MonadReader Env m, MonadIO m)
    => Stream' (TreeEntry c Abs) m r
    -> Stream' (TreeEntry c Abs) m r
filterWithEnvPredicate str = do
    predicate <- asks envFilePredicate
    baseDir   <- asks envDirectory
    filterWithPredicate predicate baseDir str

readLastSnapshotKey
    :: (MonadReader Env m, MonadIO m)
    => m (Maybe SnapshotName)
readLastSnapshotKey = do
    fp <- genFilePath "last_snapshot" >>= liftIO . getFilePath
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
    fp <- genFilePath "last_snapshot" >>= liftIO . getFilePath
    liftIO $ T.writeFile fp key

-- | Generate a file path using repo url, source uri and the provided name.
-- Creates the parent directories if they do not exist.
genFilePath :: (MonadReader Env m, MonadIO m) => RawName -> m (Path Abs File)
genFilePath name = do
    cachePath <- asks envCachePath
    srcPath   <- fmap T.pack . liftIO . getFilePath =<< asks envDirectory
    repoURL'  <- asks (repoURL . envRepository)

    -- directory structure is <encoded-repo-url>/<encoded-source-url>
    let dir = foldl pushDir cachePath [ encode repoURL', encode srcPath ]
    -- ensure directory exxists
    liftIO $ Dir.createDirectoryIfMissing True =<< getFilePath dir
    return $ makeFilePath dir name
  where
    encode = SB.toShort . T.encodeUtf8 . URI.encodeText

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
    :: (MonadReader Env m, MonadIO m)
    => Stream' (Tree, ChunkList) m r
    -> m Snapshot
makeSnapshot str = do
    res :> _ <- S.toList str
    let chunkList =
            case res of
                (Tree, chunkList):_ -> chunkList
                _ -> error "Assertion failed: no tree"

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

-- | We cannot survive errors retrieving the snapshot metadata.
abortOnError
    :: (MonadThrow m, Exception e)
    => (Stream' a m r -> Stream' (Either e b) m r)
    -> Stream' a m r
    -> Stream' b m r
abortOnError f str =
    S.mapM (either throwM return) $ f str -- TODO log also

verifyChunk
    :: Manifest
    -> PlainChunk t
    -> Either (Error t) (PlainChunk t)
verifyChunk Manifest{..} =
    (toError +++ id) . verify mStoreIDKey
  where
    toError :: VerifyFailed t -> Error t
    toError (VerifyFailed Chunk{..}) =
        Error VerifyError cOffsets cStoreID Nothing

newtype VerifyResult = VerifyResult
    { vrErrors :: Seq (Error FileItem)
    } deriving (Show, Semigroup, Monoid)

summariseErrors
    :: (MonadIO m)
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
storedSize :: CipherChunk t -> Int64
storedSize Chunk{..} = fromIntegral $ B.length (cSecretBox cContent)
