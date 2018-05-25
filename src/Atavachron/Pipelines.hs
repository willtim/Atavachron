{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
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
import Control.Concurrent (threadDelay)
import Control.Lens (over)
import Control.Logging
import Control.Monad
import Control.Monad.Catch
import Control.Monad.Reader.Class
import Control.Monad.Reader
import Control.Monad.State
import Control.Monad.Trans.Resource

import Data.Either
import Data.Function (on)
import Data.Monoid

import qualified Data.ByteString as B
import qualified Data.Text as T
import qualified Data.Text.Encoding as E
import Data.Time.Clock

import qualified Data.List as List

import Data.Sequence (Seq)
import qualified Data.Sequence as Seq

import Codec.Serialise

import Streaming (Of(..))
import Streaming.Prelude (yield)
import qualified Streaming as S
import qualified Streaming.Prelude as S hiding (mapM_)
import qualified Streaming.Internal as S (concats)

import qualified System.Posix.User as User
import qualified System.Directory as Dir

import System.IO
import System.Random (randomRIO)

import Network.HostName (getHostName)

import qualified Atavachron.Chunk.CDC as CDC
import Atavachron.Chunk.Builder
import Atavachron.Chunk.Process
import qualified Atavachron.Chunk.Cache as ChunkCache
import Atavachron.Path
import qualified Atavachron.Store as Store

import Atavachron.Streaming (Stream')
import qualified Atavachron.Streaming as S

import Atavachron.Types
import Atavachron.Tree
import Atavachron.Files


------------------------------------------------------------
-- Pipelines


-- | Full and incremental backup.
backupPipeline
    :: (MonadReader (Env Backup) m, MonadState Progress m, MonadResource m)
    => Path Abs Dir
    -> m Snapshot
backupPipeline
    = makeSnapshot
    . uploadPipeline
    . serialiseTree
    . S.left ( writeFilesCache
             . S.merge
             . S.left (uploadPipeline . readFiles)
             . fromDiffs
             . diff readFilesCache
             )
    . recurseDir


-- | Verify files and their chunks.
verifyPipeline
    :: (MonadReader (Env Restore) m, MonadState Progress m, MonadThrow m, MonadIO m)
    => Snapshot
    -> Stream' (FileItem, VerifyResult) m ()
verifyPipeline
  = summariseErrors
  . downloadPipeline
  . S.lefts
  . snapshotTree


-- | Restore (using FileMeta predicates).
restoreFiles
    :: (MonadReader (Env Restore) m, MonadState Progress m, MonadThrow m, MonadResource m)
    => Snapshot
    -> m ()
restoreFiles
  = saveFiles
  . S.left ( trimEndChunks
           . rechunkToTags
           . handleErrors
           . downloadPipeline
           )
  . filterItems
  . snapshotTree


-- | The complete upload pipeline: CDC chunking, encryption,
-- compression and upload to remote location.
uploadPipeline
  :: (MonadReader (Env p) m, MonadState Progress m, MonadResource m, Eq t, Show t)
  => Stream' (RawChunk t B.ByteString) m r
  -> Stream' (t, ChunkList) m r
uploadPipeline
    = packChunkLists
    . progressMonitor . S.copy
    . S.merge
    . S.left (uploadChunks . encodeChunks)
    . dedupChunks
    . hashChunks
    . rechunkCDC


-- | A download pipeline that does not abort on errors
downloadPipeline
    :: (MonadReader (Env p) m, MonadState Progress m, MonadThrow m, MonadIO m)
    => Stream' (FileItem, ChunkList) m ()
    -> Stream' (Either (Error FileItem) (PlainChunk FileItem)) m ()
downloadPipeline
    = S.bind verifyChunks
    . S.bind decodeChunks
    . downloadChunks
    . unpackChunkLists


snapshotTree
    :: (MonadReader (Env Restore) m, MonadState Progress m, MonadThrow m, MonadIO m)
    => Snapshot
    -> Stream' (Either (FileItem, ChunkList) OtherItem) m ()
snapshotTree
    = deserialiseTree
    . rechunkToTags
    . abortOnError decodeChunks
    . abortOnError downloadChunks
    . unpackChunkLists
    . snapshotChunkLists


------------------------------------------------------------
-- Supporting stream transformers


-- | Apply the FileMeta predicate to the supplied tree metadata and
-- filter out files/directories for restore.
filterItems
    :: (MonadReader (Env Restore) m)
    => Stream' (Either (FileItem, ChunkList) OtherItem) m r
    -> Stream' (Either (FileItem, ChunkList) OtherItem) m r
filterItems str = do
    p <- asks $ rPredicate . envParams
    flip S.filter str $ \case
        Left (item, _)          -> applyPredicate p item
        Right (LinkItem item _) -> applyPredicate p item
        Right (DirItem item)    -> applyPredicate p item


-- | TODO Report errors during restore and record affected files.
-- Perhaps we can record broken files and missing chunks
-- then we can give them special names in saveFiles? For now, just abort.
handleErrors
    :: (MonadReader (Env Restore) m, MonadState Progress m)
    => Stream' (Either (Error FileItem) (PlainChunk FileItem)) m r
    -> Stream' (PlainChunk FileItem) m r
handleErrors = S.mapM $ \case
    Left Error{..} -> errorL' $ "Error during restore: "
        <> T.pack (show errKind)
        <> maybe mempty (T.pack . show) errCause
    Right chunk    -> return chunk


-- NOTE: we write progress to stderr to get automatic flushing
-- and to make it possible to use pipes over stdout if needed.
progressMonitor
    :: (MonadState Progress m, MonadIO m)
    => Stream' a m r
    -> m r
progressMonitor = S.mapM_ $ \_ -> do
    Progress{..} <- get
    putProgress $ unwords $ List.intersperse " | "
        [ "Files: "  ++ show _prFiles
        , "Chunks: " ++ show _prChunks
        , "Input: "  ++ show (_prInputSize `div` megabyte) ++ " MB"
        , "Output: " ++ show (_prCompressedSize `div` megabyte) ++ " MB"
        ]
  where
    putProgress s = liftIO $ hPutStr stderr $ "\r\ESC[K" ++ s
    megabyte = 1024*1024


-- | Scrutinise the diffs and dispatch either to the left or right.
-- Left values need the full chunk, encode, upload pipeline;
-- Right values are unchanged and already have a chunk list.
fromDiffs
    :: Monad m
    => Stream' (Diff (FileItem, ChunkList) FileItem) m r
    -> Stream' (Either FileItem (FileItem, ChunkList)) m r
fromDiffs = S.catMaybes . S.map f
  where
    f :: Diff (FileItem, ChunkList) FileItem
      -> Maybe (Either FileItem (FileItem, ChunkList))
    f (Keep x)   = Just $ Right x
    f (Insert y) = Just $ Left y
    f (Change y) = Just $ Left y
    f (Delete _) = Nothing


-- | Hash the supplied stream of chunks using multiple cores.
hashChunks
  :: (MonadReader (Env p) m, MonadIO m)
  => Stream' (RawChunk (TaggedOffsets t) B.ByteString) m r
  -> Stream' (PlainChunk t) m r
hashChunks str = do
    Env{..} <- lift ask
    let Manifest{..} = repoManifest envRepository
    flip (S.parMap envTaskWindowSize envTaskGroup) str $ \c ->
       liftIO $ return $! hash mStoreIDKey c


-- | Separate out duplicate chunks.
-- Unseen chunks are passed on the left, known duplicates on the right.
-- Uses an on-disk persistent chunks cache for de-duplication.
dedupChunks
  :: (MonadReader (Env p) m, MonadState Progress m, MonadResource m)
  => Stream' (PlainChunk t) m r
  -> Stream' (Either (PlainChunk t) (TaggedOffsets t, StoreID)) m r
dedupChunks str = do
    cacheFile   <- (T.pack . getFilePath) <$> resolveCacheFileName "chunks"
    (key, conn) <- allocate (ChunkCache.connect cacheFile) ChunkCache.close
    r <- flip S.mapM str $ \c@PlainChunk{..} -> do
        -- collect some statistics
        let chunkSize = fromIntegral $ B.length pcContent
        modify $ over prChunks    succ
               . over prInputSize (+ chunkSize)
        -- query chunk cache
        isDuplicate <- liftIO $ ChunkCache.member conn pcStoreID
        if isDuplicate
           then do -- duplicate
               return $ Right (pcOffsets, pcStoreID)
           else do
               modify $ over prDedupSize (+ chunkSize)
               return $ Left c
    release key
    return r


-- | Compress and encrypt the supplied stream of chunks using multiple cores.
encodeChunks
  :: (MonadReader (Env p) m, MonadState Progress m, MonadIO m)
  => Stream' (PlainChunk t) m r
  -> Stream' (CipherChunk t) m r
encodeChunks str = do
    Env{..} <- lift ask
    let Manifest{..} = repoManifest envRepository
    S.mapM measure .
        (S.parMap envTaskWindowSize envTaskGroup $
            liftIO . encrypt mChunkKey
                   . compress) $ str

  where
    -- measure encrypted and compressed size
    measure c@CipherChunk{..} = do
        let chunkSize = fromIntegral $ B.length ccSecretBox
        modify $ over prCompressedSize (+ chunkSize)
        return c


-- | Upload stream of chunks using multiple cores.
uploadChunks
    :: (Show t, MonadReader (Env p) m, MonadResource m)
    => Stream' (CipherChunk t) m r
    -> Stream' (TaggedOffsets t, StoreID) m r
uploadChunks str = do
    Env{..} <- lift ask
    let Repository{..} = envRepository
    cacheFile   <- (T.pack . getFilePath) <$> resolveCacheFileName "chunks"
    (key, conn) <- allocate (ChunkCache.connect cacheFile) ChunkCache.close
    str' <- S.mapM_ (liftIO . ChunkCache.insert conn . snd)
          . S.copy
          . S.parMap envTaskWindowSize envTaskGroup (uploadChunk envRetries repoStore)
          $ str
    release key
    return str'


-- NOTE: this throws a fatal error if it cannot successfully upload a
-- chunk after exceeding the retry limit.
uploadChunk :: Int -> Store.Store -> CipherChunk t -> IO (TaggedOffsets t, StoreID)
uploadChunk retries store CipherChunk{..} = do
    res <- retryWithExponentialBackoff retries $ do
        debug' $ "Uploading chunk " <> T.pack (show ccStoreID)
        Store.put store (Store.Key (Store.Path "chunks") (hexEncode ccStoreID))
            $ serialise (ccNonce, ccSecretBox)
    when (isLeft res) $ do
        let Left (ex :: SomeException) = res
        errorL' $ "Upload failed!! : " <> T.pack (show ex) -- fatal abort
    return (ccOffsets, ccStoreID)


data MissingChunkError = MissingChunkError StoreID
    deriving Show

instance Exception MissingChunkError

-- NOTE: we do not log errors here, instead we return them for aggregation elsewhere.
downloadChunk :: Show t => Int -> Store.Store -> (TaggedOffsets t, StoreID) -> IO (Either (Error t) (CipherChunk t))
downloadChunk retries store (offsets, storeID) = do
    debug' $ "Downloading chunk " <> T.pack (show storeID)
    blob <- fmap join . retryWithExponentialBackoff retries $
                maybe (Left $ MissingChunkError storeID) Right
                    <$> Store.get store (Store.Key (Store.Path "chunks") (hexEncode storeID))
    return $ (mkError +++ mkCipherChunk) ((mapLeft toException . deserialiseOrFail) =<< mapLeft toException blob)
  where
    mkCipherChunk (nonce, secretBox) = CipherChunk storeID offsets nonce secretBox
    mapLeft f = either (Left . f) Right
    mkError = Error DownloadError offsets storeID . Just


rechunkCDC
  :: (MonadReader (Env p) m, Eq t)
  => Stream' (RawChunk t B.ByteString) m r
  -> Stream' (RawChunk (TaggedOffsets t) B.ByteString) m r
rechunkCDC str = asks (repoManifest . envRepository) >>= \Manifest{..} ->
    CDC.rechunkCDC mCDCKey mCDCParams str


writeFilesCache
  :: (MonadReader (Env Backup) m, MonadResource m)
  => Stream' (FileItem, ChunkList) m r
  -> Stream' (FileItem, ChunkList) m r
writeFilesCache str = do
    cacheFile <- resolveCacheFileName "files.tmp"
    sourceDir <- asks $ bSourceDir . envParams
    writeCacheFile cacheFile
        . relativePaths sourceDir
        . S.map (uncurry FileCacheEntry)
        $ S.copy str


readFilesCache
  :: (MonadReader (Env Backup) m, MonadResource m)
  => Stream' (FileItem, ChunkList) m ()
readFilesCache = do
    cacheFile <- resolveCacheFileName "files"
    sourceDir <- asks $ bSourceDir . envParams
    S.map (ceFileItem &&& ceChunkList)
        . absolutePaths sourceDir
        $ readCacheFile cacheFile


resolveCacheFileName
    :: (MonadReader (Env p) m, MonadIO m)
    => RawName
    -> m (Path Abs File)
resolveCacheFileName name = ask >>= \Env{..} -> do
    let dir = pushDir envCachePath (E.encodeUtf8 $ repoID envRepository)
    liftIO $ Dir.createDirectoryIfMissing True $ getFilePath dir
    return $ makeFilePath dir name


-- | Group tagged-offsets and store IDs into distinct ChunkList per tag.
packChunkLists
    :: forall t m r. (Eq t, Monad m)
    => Stream' (TaggedOffsets t, StoreID) m r
    -> Stream' (t, ChunkList) m r
packChunkLists = groupByTag ChunkList


makeSnapshot
    :: (MonadReader (Env Backup) m, MonadIO m)
    => Stream' (Tree, ChunkList) m r
    -> m Snapshot
makeSnapshot str = do
    ((Tree, chunkList):_) :> _ <- S.toList str
    hostDir <- asks $ bSourceDir . envParams
    startT  <- asks envStartTime
    liftIO $ do
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
            }

snapshotChunkLists
    :: Monad m
    => Snapshot
    -> Stream' (Tree, ChunkList) m ()
snapshotChunkLists = yield . (Tree,) . sTree


unpackChunkLists
    :: forall t m r . (Eq t, Monad m)
    => Stream' (t, ChunkList) m r
    -> Stream' (TaggedOffsets t, StoreID) m r
unpackChunkLists
  = S.map swap
  . S.aggregateByKey extractStoreIDs
  where
    extractStoreIDs :: (t, ChunkList) -> Seq (StoreID, Seq (t, Offset))
    extractStoreIDs (t, ChunkList s) =
        fmap (\(storeID, offset) -> (storeID, Seq.singleton (t, offset))) s

    swap (a, b) = (b, a)


trimEndChunks
    :: forall m r . MonadIO m
    => Stream' (RawChunk FileItem B.ByteString) m r
    -> Stream' (RawChunk FileItem B.ByteString) m r
trimEndChunks
    = S.concats
    . S.maps doFileItem
    . S.groupBy ((==) `on` rcTag)
  where
    doFileItem = flip S.mapAccum_ 0 $ \accumSize (RawChunk item bs) ->
      let accumSize' = accumSize + fromIntegral (B.length bs)
      in if fileSize item < accumSize'
         then (0, RawChunk item $ B.take (fromIntegral $ fileSize item - accumSize) bs)
         else (accumSize', RawChunk item bs)


downloadChunks
    :: (MonadReader (Env p) m, MonadIO m, Show t)
    => Stream' (TaggedOffsets t, StoreID) m r
    -> Stream' (Either (Error t) (CipherChunk t)) m r  -- ^ assume downloading may fail
downloadChunks str = do
    Env{..} <- lift ask
    let Repository{..} = envRepository
    S.parMap envTaskWindowSize envTaskGroup (downloadChunk envRetries repoStore) str


decodeChunks
    :: (MonadReader (Env p) m, MonadState Progress m, MonadIO m)
    => Stream' (CipherChunk t) m r
    -> Stream' (Either (Error t) (PlainChunk t)) m r   -- ^ assume decoding may fail
decodeChunks str = do
    Env{..} <- lift ask
    let Manifest{..} = repoManifest envRepository
    (S.parMap envTaskWindowSize envTaskGroup $ \cc ->
        return
            . maybe (Left $ toError cc) Right
            . fmap decompress
            . decrypt mChunkKey
            $ cc) str
  where
    toError CipherChunk{..} = Error DecryptError ccOffsets ccStoreID Nothing


-- | We cannot survive errors retrieving the snapshot metadata.
abortOnError
    :: (MonadThrow m, Exception e)
    => (Stream' a m r -> Stream' (Either e b) m r)
    -> Stream' a m r
    -> Stream' b m r
abortOnError f str =
    S.mapM (either throwM return) $ f str -- TODO log also


-- Note: stores bad chunk errors and their IDs
data VerifyResult = VerifyResult
    { vrChunks :: Sum Int
    , vrErrors :: Seq (Error FileItem)
    } deriving Show

instance Monoid VerifyResult where
    mempty = VerifyResult 0 Seq.empty
    VerifyResult cs es `mappend` VerifyResult cs' es' =
        VerifyResult (cs `mappend` cs') (es `mappend` es')

verifyChunks
    :: MonadReader (Env p) m
    => Stream' (PlainChunk t) m r
    -> Stream' (Either (Error t) (PlainChunk t)) m r
verifyChunks str = do
    Manifest{..} <- lift . asks $ repoManifest . envRepository
    S.map ((toError +++ id) . verify mStoreIDKey) str
  where
    toError :: VerifyFailed t -> Error t
    toError (VerifyFailed PlainChunk{..}) =
        Error VerifyError pcOffsets pcStoreID Nothing

summariseErrors
    :: Monad m
    => Stream' (Either (Error FileItem) (PlainChunk FileItem)) m r
    -> Stream' (FileItem, VerifyResult) m r
summariseErrors
  = groupByTag (foldMap fst)
  . S.merge
  . S.map (fromError +++ fromChunk)
  where
    fromError e = (errOffsets e, VerifyResult (Sum 1) $ Seq.singleton e)
    fromChunk c = (pcOffsets c, VerifyResult (Sum 1) Seq.empty)


-- | Retry, if necessary, an idempotent action that is prone to
-- failure.  Exponential backoff and randomisation, ensure that we are
-- a well-behaved client to a remote service.
retryWithExponentialBackoff
    :: forall e a. Exception e
    => Int
    -> IO a
    -> IO (Either e a)
retryWithExponentialBackoff retries m
  | retries < 0 = error "retryWithExponentialBackoff: retries must be a positive integer"
  | otherwise   = loop retries
  where
    loop :: Int -> IO (Either e a)
    loop n = do
      res <- try m
      case res of
          -- failure
          Left ex
              | n > 0     -> do -- backoff before retrying/looping
                    r <- randomRIO (1 :: Double, 1 + randomisation)
                    let interval = r * initTimeout * multiplier ^ n -- seconds

                    warn' $ T.pack (show ex) <> ". Retrying..."

                    delay (floor $ interval * 1e6) -- argument is microseconds
                    loop (n - 1)
              | otherwise ->    -- give up
                    return $ Left ex
          -- success!
          Right x         -> return $ Right x

    initTimeout   = 0.5 -- seconds
    multiplier    = 1.5
    randomisation = 0.5

    delay :: Integer -> IO ()
    delay time = do
        let maxWait = min time $ toInteger (maxBound :: Int)
        threadDelay $ fromInteger maxWait
        when (maxWait /= time) $ delay (time - maxWait)
