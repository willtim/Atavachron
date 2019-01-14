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
import Control.Arrow ((+++),(&&&))
import Control.Lens (over)
import Control.Logging
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

import System.IO
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

-- | Perform a chunk check, showing any garbage (unreferenced) chunks
-- or missing chunks (!).
chunkCheck
    :: (MonadReader Env m, MonadState Progress m, MonadThrow m, MonadResource m)
    => Repository
    -> m ()
chunkCheck repo = do
    -- Add all chunks referenced by snapshots to cache.
    -- NOTE: this stream will potentially contain duplicate store IDs,
    -- which the chunk cache will de-duplicate for us.
    log' $ "Downloading snapshot chunks..."
    sinkIntoChunkCache ChunkCache.insert
        . showChunksReceived . S.copy
        $ snapChunks -- has duplicate chunks
    liftIO $ hPutStr stderr "\ESC[K"

    referenced <- withChunkCache (liftIO . ChunkCache.size)

    -- List all chunks in repo.
    -- There are two sets we are interested in:
    -- 1) missing: chunks in cache, not in this stream
    -- 2) garbage: chunks in this stream, but not in cache
    log' $ "Listing all chunks in repository..."
    garbage S.:> _ <-
          S.sum
        . S.map (const (1::Integer))
        . filterUsingChunkCache isGarbage
        . showChunksReceived . S.copy
        $ repoChunks
    liftIO $ hPutStr stderr "\ESC[K"

    missing <- withChunkCache (liftIO . ChunkCache.size)

    liftIO $ do
        putStrLn $ "Referenced: " ++ show referenced
        putStrLn $ "Garbage:    " ++ show garbage
        putStrLn $ "Missing:    " ++ show missing
  where

    snaps = hoist liftResourceT $ listSnapshots repo

    snapChunks = S.concatMap (either err collectChunks . snd) snaps
      where
        err ex = errorL' $ "Failed to fetch snapshot: " <> T.pack (show ex)

    repoChunks = hoist liftResourceT $ listChunks repo

    isGarbage conn storeId = do
        isMember <- ChunkCache.member conn storeId
        if isMember
           then ChunkCache.delete conn storeId >> return False
           else return True

    showChunksReceived = S.mapM_ $ \_ -> do
        modify (over prChunks succ)
        gets _prChunks >>= \n ->
            when (n `mod` 100==0) $ putProgress $ "Chunks received: " ++ (show n)


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

-- | Write a progress reporting string to standard error after first clearing the line.
-- NOTE: ("\ESC[K" ++ s ++ "\r") interleaves better with debug logging, although the cursor
-- sits at the beginning of the line.
putProgress :: MonadIO m => String -> m ()
putProgress s = liftIO $ hPutStr stderr $ "\ESC[K" ++ s ++ "\r"


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
        errorL' $ "Error during restore: "
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
        debug' $ "Storing chunk " <> T.pack (show cStoreID)
        putChunk repo cStoreID cContent
    case res of
        Left (ex :: SomeException) ->
            errorL' $ "Failed to store chunk : " <> T.pack (show ex) -- fatal abort
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
    debug' $ "Retrieving chunk " <> T.pack (show storeID)
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
    cacheFile <- resolveTempFileName "files.tmp"
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
    cacheFile  <- resolveTempFileName' "files.tmp"
    cacheFile' <- genFilesCacheName "files" >>= resolveCacheFileName'
    res <- try $ liftIO $ Dir.renameFile cacheFile cacheFile'
    case res of
        Left (ex :: SomeException) -> errorL' $ "Failed to update cache file: " <> (T.pack $ show ex)
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
            warn' $ "Failed to store program binary: " <> T.pack (show err)
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

sinkIntoChunkCache
  :: (MonadReader Env m, MonadResource m)
  => (ChunkCache.Connection -> a -> IO ())
  -> Stream' a m r
  -> m r
sinkIntoChunkCache f str =
    withChunkCache $ \conn ->
        S.mapM_ (liftIO . f conn) str

filterUsingChunkCache
  :: (MonadReader Env m, MonadResource m)
  => (ChunkCache.Connection -> a -> IO Bool)
  -> Stream' a m r
  -> Stream' a m r
filterUsingChunkCache f str =
    withChunkCache $ \conn ->
        S.filterM (liftIO . f conn) str
