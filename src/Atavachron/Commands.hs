{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DisambiguateRecordFields #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

-- |  High-level commands exposed to the command-line interface.
--
module Atavachron.Commands where

import Prelude hiding (concatMap)

import Codec.Serialise

import Control.Exception
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Reader
import Control.Monad.State.Strict
import Control.Monad.Trans.Resource

import qualified Data.ByteString.Lazy as LB
import qualified Data.List as List
import qualified Data.Map as Map
import Data.Maybe
import Data.Ord
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as T
import qualified Data.Text.Encoding as T
import Data.Time.Clock

import qualified System.IO as IO

import GHC.Conc (numCapabilities)

import Text.Printf

import qualified Streaming.Prelude as S (each, map, toList_)
import System.FilePath.Glob
import System.Posix.Process (getProcessID)
import Network.HostName (getHostName)
import qualified System.Directory as Dir
import qualified System.Posix.Files as Files

import Atavachron.Chunk.Encode (hexEncode)
import Atavachron.Config
import Atavachron.Env
import Atavachron.Logging
import Atavachron.Path
import Atavachron.Pipelines
import Atavachron.Garbage
import Atavachron.Repository ( Repository(..), Snapshot(..)
                             , SnapshotName, CachedCredentials)
import Atavachron.Store (Store)
import Atavachron.Streaming (mkTaskGroup)
import Atavachron.Tree (FileMeta(..), Diff(..), filterItems)
import qualified Atavachron.Prune as Prune
import qualified Atavachron.Repository as Repository
import qualified Atavachron.Store as Store
import qualified Atavachron.Store.LocalFS as Store
import qualified Atavachron.Store.S3 as Store
import qualified Atavachron.Streaming as S
import qualified Atavachron.Tree as Tree


type FileGlob = Text

data Command
  = CInit      InitOptions
  | CBackup    BackupOptions
  | CVerify    VerifyOptions
  | CRestore   RestoreOptions
  | CSnapshots SnapshotOptions
  | CList      ListOptions
  | CDiff      DiffOptions
  | CKeys      KeyOptions
  | CPrune     PruneOptions
  | CChunks    ChunkOptions
  | CConfig    ConfigOptions
--  | Help

data InitOptions
    = InitOptions
      { repoURL     :: URL
      }
    | InitOptionsProfile
      { profileName :: Text
      }

data BackupOptions
    = BackupOptions
      { repoURL       :: URL
      , sourceDir     :: Text
      , fileGlobs     :: FileGlobs
      , forceFullScan :: Bool
      }
    | BackupOptionsProfile
      { profileName   :: Text
      , fileGlobs     :: FileGlobs
      , forceFullScan :: Bool
      }

data VerifyOptions
    = VerifyOptions
      { repoURL     :: URL
      , snapshotID  :: SnapshotName
      , fileGlobs   :: FileGlobs
      }
    | VerifyOptionsProfile
      { profileName :: Text
      , snapshotID  :: SnapshotName
      , fileGlobs   :: FileGlobs
      }

data RestoreOptions
    = RestoreOptions
      { repoURL     :: URL
      , snapshotID  :: SnapshotName
      , targetDir   :: Text
      , fileGlobs   :: FileGlobs
      }
    | RestoreOptionsProfile
      { profileName :: Text
      , snapshotID  :: SnapshotName
      , targetDir   :: Text
      , fileGlobs   :: FileGlobs
      }

data SnapshotOptions
    = SnapshotOptions
      { repoURL     :: URL
      , sourceDir   :: Maybe Text
      }
    | SnapshotOptionsProfile
      { profileName :: Text
      }

data ListOptions
    = ListOptions
      { repoURL     :: URL
      , snapshotID  :: SnapshotName
      , fileGlobs   :: FileGlobs
      }
    | ListOptionsProfile
      { profileName :: Text
      , snapshotID  :: SnapshotName
      , fileGlobs   :: FileGlobs
      }

data DiffOptions
    = DiffOptions
      { repoURL     :: URL
      , snapshotID1 :: SnapshotName
      , snapshotID2 :: SnapshotName
      }
    | DiffOptionsProfile
      { profileName :: Text
      , snapshotID1 :: SnapshotName
      , snapshotID2 :: SnapshotName
      }

data KeyOptions
    = KeyOptions
      { repoURL     :: URL
      , argument    :: KeysArgument
      }
    | KeyOptionsProfile
      { profileName :: Text
      , argument    :: KeysArgument
      }

data PruneOptions
    = PruneOptions
      { repoURL     :: URL
      , sourceDir   :: Maybe Text
      , settings    :: PruneSettings
      , dryRun      :: Bool
      }
    | PruneOptionsProfile
      { profileName :: Text
      , dryRun      :: Bool
      }

data ChunkOptions
    = ChunkOptions
      { repoURL     :: URL
      , argument    :: ChunksArgument
      }
    | ChunkOptionsProfile
      { profileName :: Text
      , argument    :: ChunksArgument
      }

data KeysArgument
    = ListKeys
    | AddKey Text

data ChunksArgument
    = CheckChunks
    | RepairChunks
    | ExhaustiveGC
    | DeleteGarbage

data ConfigOptions
    = ValidateConfig
    | GenerateConfig

data FileGlobs = FileGlobs
    { includeGlobs :: [FileGlob]
    , excludeGlobs :: [FileGlob]
    }

noFileGlobs :: FileGlobs
noFileGlobs = FileGlobs [] []

runCommand :: Maybe FilePath -> Command -> IO ()
runCommand mfp cmd = do
    let mcfg = tryLoadingConfigFile mfp
    case cmd of
        CInit options       -> mcfg >>= initialise options
        CBackup options     -> mcfg >>= backup options
        CVerify options     -> mcfg >>= verify options
        CRestore options    -> mcfg >>= restore options
        CSnapshots options  -> mcfg >>= snapshots options
        CList options       -> mcfg >>= list options
        CDiff options       -> mcfg >>= diff options
        CKeys options       -> mcfg >>= keys options
        CPrune options      -> mcfg >>= prune options
        CChunks options     -> mcfg >>= chunks options
        CConfig options     -> config options mfp

------------------------------------------------------------

initialise :: InitOptions -> Maybe Config -> IO ()
initialise InitOptions{..} _ = do
    store <- parseURL' repoURL
    pass  <- newPassword
    cc    <- Repository.initRepository store pass
    let url = urlText repoURL
    saveCredentials url cc
    logInfo $ "Repository created at " <> url
initialise InitOptionsProfile{..} mcfg = do
    p <- getProfile profileName mcfg
    initialise (InitOptions{repoURL = profileLocation p}) mcfg

backup :: BackupOptions -> Maybe Config -> IO ()
backup BackupOptions{..} mcfg = do
    sourceDir <- parseAbsDir' sourceDir
    store     <- parseURL'    repoURL
    repo      <- authenticate store
    runBackup mcfg repo sourceDir fileGlobs forceFullScan
backup BackupOptionsProfile{..} mcfg = do
    p <- getProfile profileName mcfg
    backup (BackupOptions{repoURL = profileLocation p
                         ,sourceDir = profileSource p
                         ,fileGlobs = addGlobsFromProfile p fileGlobs
                         ,forceFullScan
                         }) mcfg

verify :: VerifyOptions -> Maybe Config -> IO ()
verify VerifyOptions{..} mcfg = do
    store     <- parseURL'    repoURL
    repo      <- authenticate store
    snap      <- getSnapshot  repo snapshotID
    runVerify mcfg repo snap fileGlobs
verify VerifyOptionsProfile{..} mcfg = do
    p <- getProfile profileName mcfg
    verify (VerifyOptions{repoURL = profileLocation p
                         ,snapshotID
                         ,fileGlobs = addGlobsFromProfile p fileGlobs
                         }) mcfg

restore :: RestoreOptions -> Maybe Config -> IO ()
restore RestoreOptions{..} mcfg = do
    store     <- parseURL'    repoURL
    targetDir <- parseAbsDir' targetDir
    repo      <- authenticate store
    snap      <- getSnapshot  repo snapshotID
    runRestore mcfg repo snap targetDir fileGlobs
restore RestoreOptionsProfile{..} mcfg = do
    p <- getProfile profileName mcfg
    restore (RestoreOptions{repoURL = profileLocation p
                           ,snapshotID
                           ,targetDir
                           ,fileGlobs = addGlobsFromProfile p fileGlobs
                           }) mcfg

snapshots :: SnapshotOptions -> Maybe Config -> IO ()
snapshots SnapshotOptions{..} _ = do
    ms <- maybe (return Nothing) (fmap Just . parseSource) sourceDir
    listSnapshots repoURL ms
snapshots SnapshotOptionsProfile{..} mcfg = do
    p <- getProfile profileName mcfg
    s <- parseSource (profileSource p)
    listSnapshots (profileLocation p) (Just s)

list :: ListOptions -> Maybe Config -> IO ()
list ListOptions{..} mcfg =
    listFiles mcfg repoURL snapshotID fileGlobs
list ListOptionsProfile{..} mcfg = do
    p <- getProfile profileName mcfg
    listFiles mcfg (profileLocation p) snapshotID (addGlobsFromProfile p fileGlobs)

diff :: DiffOptions -> Maybe Config -> IO ()
diff DiffOptions{..} mcfg = do
    store     <- parseURL'    repoURL
    repo      <- authenticate store
    env       <- makeEnv mcfg repo rootDir noFileGlobs
    snap1     <- getSnapshot repo snapshotID1
    snap2     <- getSnapshot repo snapshotID2
    runResourceT
        . flip evalStateT initialProgress
        . flip runReaderT env
        . S.mapM_ (liftIO . printDiff)
        $ Tree.diff fst fst (S.lefts $ snapshotTree snap1)
                            (S.lefts $ snapshotTree snap2)
  where
    printDiff = \case
        Keep   _        -> return () -- don't print
        Insert (item,_) -> getFilePath (filePath item) >>= putStrLn . ("+ "<>)
        Change (item,_) -> getFilePath (filePath item) >>= putStrLn . ("c "<>)
        Delete (item,_) -> getFilePath (filePath item) >>= putStrLn . ("- "<>)
diff DiffOptionsProfile{..} mcfg = do
    p <- getProfile profileName mcfg
    diff (DiffOptions{repoURL = profileLocation p
                     ,snapshotID1
                     ,snapshotID2
                     }) mcfg

keys :: KeyOptions -> Maybe Config -> IO ()
keys KeyOptions{..} _ =
    case argument of
        ListKeys    -> listAccessKeys repoURL
        AddKey name -> addAccessKey repoURL name
keys KeyOptionsProfile{..} mcfg = do
    p <- getProfile profileName mcfg
    keys (KeyOptions{repoURL = profileLocation p
                    ,argument
                    }) mcfg

prune :: PruneOptions -> Maybe Config -> IO ()
prune PruneOptions{..} mcfg = do
    store <- parseURL'    repoURL
    repo  <- authenticate store
    ms <- maybe (return Nothing) (fmap Just . parseSource) sourceDir
    case settings of
        PruneSettings Nothing Nothing Nothing Nothing | isNothing ms ->
            panic $ "Refusing to prune all snapshots for the entire repository. "
                <> "See \"prune help\" for additional settings."
        _ -> runPrune mcfg repo ms settings dryRun
prune PruneOptionsProfile{..} mcfg = do
    p <- getProfile profileName mcfg
    case profilePruning p of
        Disabled -> panic $ "Pruning disabled for profile: " <> profileName
        Enabled settings -> prune (PruneOptions{repoURL = profileLocation p
                                               ,sourceDir = Just (profileSource p)
                                               ,settings
                                               ,dryRun
                                               }) mcfg

chunks :: ChunkOptions -> Maybe Config -> IO ()
chunks ChunkOptions{..} mcfg =
    case argument of
        CheckChunks   -> runCheckChunks mcfg repoURL
        RepairChunks  -> runRepairChunks mcfg repoURL
        ExhaustiveGC  -> runExhaustiveGC mcfg repoURL
        DeleteGarbage -> runDeleteGarbage mcfg repoURL
chunks ChunkOptionsProfile{..} mcfg = do
    p <- getProfile profileName mcfg
    chunks (ChunkOptions{repoURL = profileLocation p
                        ,argument
                        }) mcfg

config :: ConfigOptions -> Maybe FilePath -> IO ()
config GenerateConfig mfp = writeDefaultConfigFile mfp
config ValidateConfig mfp = do
    res <- tryLoadingConfigFile mfp
    case res of
        Just{}  -> printf "Configuration loaded successfully."
        Nothing -> printf "No configuration file found."

listSnapshots :: URL -> Maybe (Text, Path Abs Dir) -> IO ()
listSnapshots repoURL source = do
    case source of
        Just (host, path) ->
            putStrLn $ "Listing snapshots for //" <> T.unpack host <> show path <> "..."
        Nothing ->
            putStrLn "Listing all snapshots in the repository..."

    store     <- parseURL'    repoURL
    repo      <- authenticate store
    let stream = maybe (Repository.listSnapshots repo)
                       (Repository.listSnapshotsForSource repo)
                       source

    -- We assume that we can retain the snapshot list in memory and order
    -- them by host, host dir, user and date (most recent first)
    snapshots <- runResourceT
        . fmap (List.sortOn $ compareKey . snd)
        . S.toList_
        . S.map (\(key, e'snap) ->
            case e'snap of
                Left ex    ->
                    panic $ "Failed to fetch snapshot: " <> T.pack (show ex)
                Right snap -> (key, snap))
        $ stream

    if (null snapshots)
       then putStrLn "No snapshots found."
       else flip mapM_ snapshots $ \(key, snap) ->
                liftIO (printSnapshotRow key snap)
  where
    compareKey Snapshot{..} = (sHostName, sHostDir, sUserName, Down sStartTime)

listFiles :: Maybe Config -> URL -> SnapshotName -> FileGlobs -> IO ()
listFiles mcfg repoURL partialKey globs = do
    store <- parseURL'    repoURL
    repo  <- authenticate store
    env   <- makeEnv mcfg repo rootDir globs
    snap  <- liftIO $ getSnapshot repo partialKey
    runResourceT
        . flip evalStateT initialProgress
        . flip runReaderT env
        . S.mapM_ (liftIO . printFile)
        . S.lefts
        . filterItems fst
        $ snapshotTree snap
  where
    printFile (item, {-Repository.ChunkList chunks-} _) = do
        -- print out as a relative path, i.e. without the leading '/'.
        fp <- getFilePath (relativise rootDir $ filePath item)
        putStrLn fp
        -- forM_ chunks $ T.putStrLn . hexEncode

listAccessKeys :: URL -> IO ()
listAccessKeys repoURL = do
    store <- parseURL'    repoURL
    repo  <- authenticate store
    runResourceT
        . S.mapM_ (liftIO . T.putStrLn . fst)
        $ Repository.listAccessKeys (repoStore repo)

addAccessKey :: URL -> Text -> IO ()
addAccessKey repoURL name = do
    store <- parseURL' repoURL
    T.putStrLn "Checking existing credentials."
    repo  <- authenticate store
    T.putStrLn "Please provide the additional credentials."
    pass  <- newPassword
    void $ Repository.newAccessKey (repoStore repo) (repoManifestKey repo) name pass
    T.putStrLn "Access key created."

runBackup :: Maybe Config -> Repository -> Path Abs Dir -> FileGlobs -> Bool -> IO ()
runBackup mcfg repo sourceDir globs forceFullScan' = do
    env      <- makeEnv mcfg repo sourceDir globs
    -- read the last snapshot written and check that it still exists, if it doesn't
    -- force a full scan, since we cannot use the files cache.
    mLastKey      <- runReaderT readLastSnapshotKey env
    forceFullScan <-
        case mLastKey of
            Just key | not forceFullScan' -> do
                haveLastSnap <- Repository.doesSnapshotExist repo key
                when (not haveLastSnap) $
                    logWarn $ "Could not find previous snapshot '" <> key <> "', forcing full scan."
                return $ not haveLastSnap
            Nothing  | not forceFullScan' -> return True -- full scan if no previous key
            _ -> return True

    snapshot <-
        runResourceT
          . flip evalStateT initialProgress
          . flip runReaderT env
          $ backupPipeline forceFullScan sourceDir

    res <- Repository.putSnapshot repo snapshot
    case res of
        Left ex   -> panic $ "Failed to write snapshot: " <> T.pack (show ex)
        Right key -> do
            retainProgress
            logInfo $ "Wrote snapshot " <> T.take 8 key
            flip runReaderT env $
                commitFilesCache >> writeLastSnapshotKey key >> cleanUpTempFiles
    logInfo "Backup complete."


runVerify :: Maybe Config -> Repository -> Snapshot -> FileGlobs -> IO ()
runVerify mcfg repo snapshot globs = do
    env <- makeEnv mcfg repo rootDir globs
    runResourceT
        . flip evalStateT initialProgress
        . flip runReaderT env
        . S.mapM_ logFailed -- for now, just log files with errors
        $ verifyPipeline snapshot
    retainProgress
    logInfo "Verification complete."
  where
    logFailed (item, VerifyResult errors) =
        unless (null errors) $ do
            path <- liftIO $ getFilePath (filePath item)
            logWarn $ "File has errors: " <> (T.pack path)

runRestore :: Maybe Config -> Repository -> Snapshot -> Path Abs Dir -> FileGlobs -> IO ()
runRestore mcfg repo snapshot targetDir globs = do
    env <- makeEnv mcfg repo targetDir globs
    runResourceT
        . flip evalStateT initialProgress
        . flip runReaderT env
        $ restoreFiles snapshot
    retainProgress
    logInfo "Restore complete."

runCheckChunks :: Maybe Config -> URL -> IO ()
runCheckChunks mcfg repoURL = do
    runChunksProc mcfg repoURL chunkCheck
    logInfo "Chunks check complete."

runRepairChunks :: Maybe Config -> URL -> IO ()
runRepairChunks mcfg repoURL = do
    runChunksProc mcfg repoURL chunkRepair
    logInfo "Chunks repair complete."

runExhaustiveGC :: Maybe Config -> URL -> IO ()
runExhaustiveGC mcfg repoURL = do
    runChunksProc mcfg repoURL $ \repo -> collectGarbage repo Nothing
    logInfo "Exhaustive GC complete."

runDeleteGarbage :: Maybe Config -> URL -> IO ()
runDeleteGarbage mcfg repoURL = do
    runChunksProc mcfg repoURL deleteGarbage
    logInfo "Garbage deletion complete."

runChunksProc
  :: Maybe Config
  -> URL
  -> (Repository -> ReaderT Env (StateT Progress (ResourceT IO)) ())
  -> IO ()
runChunksProc mcfg repoURL m = do
    store <- parseURL'    repoURL
    repo  <- authenticate store
    env   <- makeEnv mcfg repo rootDir noFileGlobs
    runResourceT
        . flip evalStateT initialProgress
        . flip runReaderT env
        $ m repo >> cleanUpTempFiles

runPrune
    :: Maybe Config
    -> Repository
    -> Maybe (Text, Path Abs Dir)
    -> PruneSettings
    -> Bool
    -> IO ()
runPrune mcfg repo source settings dryRun = do
    env <- makeEnv mcfg repo rootDir noFileGlobs
    let stream = maybe (Repository.listSnapshots repo)
                       (Repository.listSnapshotsForSource repo)
                       source
    snapshotsAll <- runResourceT
        . S.toList_
        . S.map (\(key, e'snap) ->
            case e'snap of
                Left ex    ->
                    panic $ "Failed to fetch snapshot: " <> T.pack (show ex)
                Right snap -> (key, snap))
        $ stream

    -- group by (host, path) and apply prune seperately to each distinct group
    let snapshotGroups
            = Map.toList
            . Map.map (Map.fromList)
            $ foldr (uncurry $ Map.insertWith (++)) mempty
                [ ((sHostName, sHostDir), [(k, s)])
                | (k, s@Snapshot{..}) <- snapshotsAll]

    forM_ snapshotGroups $ \((host, hostDir), snapshots) -> do

        path <- getFilePath hostDir
        putStrLn $ prefix <> "Pruning snapshots for //" <> T.unpack host <> path <> " ..."

        let snapshots_pruned = Prune.pruneSnapshots settings snapshots
            deletions = snapshots `Map.difference` snapshots_pruned

        unless (Map.null deletions) $ do
            putStrLn $ "Deletions" <> if dryRun then " (proposed):" else ":"
            mapM_ (uncurry printSnapshotRow) $ Map.toList deletions

        when (not dryRun) $ do
            -- Delete snapshots
            forM_ (Map.elems deletions) $ \snapshot -> do
                e <- Repository.deleteSnapshot repo snapshot
                case e of
                    Left ex  -> logWarn $ "Could not delete snapshot " <> T.pack (show snapshot)
                                    <> " : " <> T.pack (show ex)
                    Right () -> return ()

            -- Incremental Garbage collection
            runResourceT
                . flip evalStateT initialProgress
                . flip runReaderT env
                $ collectGarbage repo (Just . toStream $ deletions)
                    >> cleanUpTempFiles

    logInfo $ "Prune complete."
  where
    prefix | dryRun    = "DRY RUN: "
           | otherwise = ""

    toStream = S.each . Map.toList

authenticate :: Store -> IO Repository
authenticate store = do
    -- check for cached credentials
    m'cc <- loadCredentials (Store.name store)
    case m'cc of
        Nothing -> newCredentials store
        Just cc -> Repository.authenticate' store cc

newCredentials :: Store -> IO Repository
newCredentials store = do
    pass       <- askPassword
    (repo, cc) <- Repository.authenticate store pass
    repo <$ saveCredentials (Store.name store) cc

loadCredentials :: Text -> IO (Maybe CachedCredentials)
loadCredentials urlText = do
    cachePath <- getCachePath
    filePath  <- mkCacheFileName cachePath urlText "credentials" >>= getFilePath
    exists    <- Files.fileExist filePath
    if exists
        then do logDebug $ "Using cached credentials."
                Just . deserialise <$> LB.readFile filePath
        else    return Nothing

saveCredentials :: Text -> CachedCredentials -> IO ()
saveCredentials urlText cc = do
    cachePath <- getCachePath
    filePath  <- mkCacheFileName cachePath urlText "credentials" >>= getFilePath
    LB.writeFile filePath (serialise cc)
    Files.setFileMode filePath (Files.ownerReadMode `Files.unionFileModes` Files.ownerWriteMode)
    T.putStrLn $ "Credentials cached at " <> T.pack filePath

makeEnv :: Maybe Config -> Repository -> Path Abs Dir -> FileGlobs -> IO Env
makeEnv mcfg repo localDir globs = do
    logDebug $ "Available cores: " <> T.pack (show numCapabilities)
    startT      <- getCurrentTime

    -- default to a conservative size to minimise memory usage.
    let taskBufferSize = maybe numCapabilities fromIntegral
                       $ getOverride configTaskBufferSize

    taskGroup   <- mkTaskGroup
                 . maybe numCapabilities fromIntegral
                 $ getOverride configTaskThreads

    cachePath   <- maybe getCachePath parseAbsDir'
                 $ getOverride configCachePath

    tempPath    <- getTempDir

    let garbageExpiryDays
                 = maybe 30 fromIntegral
                 $ getOverride configGarbageExpiryDays

    return Env
         { envRepository        = repo
         , envStartTime         = startT
         , envTaskBufferSize    = taskBufferSize
         , envTaskGroup         = taskGroup
         , envRetries           = maybe 5 fromIntegral $ configMaxRetries <$> mcfg
         , envCachePath         = cachePath
         , envTempPath          = tempPath
         , envFilePredicate     = parseGlobs globs
         , envDirectory         = localDir
         , envBackupBinary      = fromMaybe False $ configBackupBinary <$> mcfg
         , envGarbageExpiryDays = garbageExpiryDays
         }
  where
      getOverride :: (Config -> Overridable a) -> Maybe a
      getOverride f = mcfg >>= overridableToMaybe . f

-- | For now, default to XDG standard
getCachePath :: IO (Path Abs Dir)
getCachePath =
    fromMaybe (panic "Cannot parse XDG directory") . parseAbsDir
        <$> Dir.getXdgDirectory Dir.XdgCache "atavachron"

-- | Parses the source path and augments it with the current hostname.
parseSource :: Text -> IO (Text, Path Abs Dir)
parseSource sourceDir = (,)
    <$> (T.pack <$> getHostName)
    <*> (parseAbsDir' sourceDir)

-- | Logs and throws, if it cannot parse the path.
parseAbsDir' :: Text -> IO (Path Abs Dir)
parseAbsDir' t =
    case parseAbsDir (T.unpack t) of
        Nothing   -> panic $ "Cannot parse absolute path: " <> t
        Just path -> return path

-- | Return a temporary directory inside the appropriate place which
-- incorporates the process name.
getTempDir :: IO (Path Abs Dir)
getTempDir = do
    fp  <- Dir.getTemporaryDirectory
    case parseAbsDir fp of
        Nothing   -> panic $ "Cannot parse system-provided temporary path: " <> (T.pack fp)
        Just path -> do
            pid <- T.encodeUtf8 . T.pack . show <$> getProcessID
            let tmpDir = foldl pushDir path [T.encodeUtf8 "atavachron", pid]
            fp  <- getFilePath tmpDir
            logDebug $ "Using temporary directory: " <> T.pack fp
            return tmpDir

-- | Logs and throws, if it cannot retrieve the snapshot
getSnapshot :: Repository -> Text -> IO Snapshot
getSnapshot repo partialKey = do
    e'snap <- Repository.getSnapshot repo partialKey
    case e'snap of
        Left ex    -> panic $ "Could not retrieve snapshot: " <> T.pack (show ex)
        Right snap -> return snap

-- | Print a snapshot to stdout as a fixed-width row.
printSnapshotRow :: SnapshotName -> Snapshot -> IO ()
printSnapshotRow key Snapshot{..} = do
    hostDir <- getFilePath sHostDir
    printf "%s | %-8.8s | %-8.8s | %-32.32s | %-16.16s | %-16.16s | %s \n"
           (T.unpack $ T.take 8 key)
           (T.unpack sUserName)
           (T.unpack sHostName)
           hostDir
           (show sStartTime)
           (show sFinishTime)
           (maybe "" (T.unpack . T.take 8 . hexEncode) sExeBinary)

parseGlobs :: FileGlobs -> FilePredicate
parseGlobs FileGlobs{..} = FilePredicate $ \path ->
    (&&) <$> applyPredicate includePred path <*> (not <$> applyPredicate excludePred path)
  where
    includePred | null includeGlobs = allFiles
                | otherwise         = disjunction $ map parseGlob includeGlobs
    excludePred | null excludeGlobs = noFiles
                | otherwise         = disjunction $ map parseGlob excludeGlobs

parseGlob :: Text -> FilePredicate
parseGlob g = FilePredicate $ \path ->
        match patt <$> getFilePath path
  where
    patt | "/" `T.isPrefixOf` g = panic $ "File path glob pattern must be relative not absolute: " <> g
         | otherwise = simplify $ compile $ T.unpack g

-- TODO move URL parsing logic to each individual store?
parseURL :: URL -> IO (Either Text Store)
parseURL URL{..} =
    case "://" `T.breakOn` urlText of
        ("file",  T.unpack . T.drop 3 -> rest) -> do
            m'path <- return $ parseAbsDir rest
            return $ case m'path of
                Nothing   -> Left $ "Cannot parse file URL: " <> urlText
                Just path -> Right $ Store.newLocalFS urlText path
        ("s3", T.drop 3 -> rest) -> do
            case Store.parseS3URL rest of
                Nothing   ->
                    return $ Left $ "Cannot parse S3 URL: " <> urlText
                Just (region, bucketName) ->
                    Right <$> Store.newS3Store urlText region bucketName
        _ -> return $ Left $ "Cannot parse URL: " <> urlText

-- a version that blows up
parseURL' :: URL -> IO Store
parseURL' repoURL =
    either (panic . ("Cannot parse URL: "<>)) id <$> parseURL repoURL

newPassword :: IO Text
newPassword = do
    T.putStr "Enter password: "
    pass1 <- getPassword
    T.putStr "Re-enter password: "
    pass2 <- getPassword
    if pass1 /= pass2
       then T.putStrLn "Passwords do not match!" >> newPassword
       else return pass1

askPassword :: IO Text
askPassword = T.putStr "Enter password: " >> getPassword

getPassword :: IO Text
getPassword = do
    IO.hFlush IO.stdout
    bracket_ (IO.hSetEcho IO.stdin False)
             (IO.hSetEcho IO.stdin True >> IO.putChar '\n')
             T.getLine

-- | Fail if we need a profile and we can't find one.
getProfile :: Text -> Maybe Config -> IO Profile
getProfile name mcfg
    | Just profile <- mcfg >>= findProfileByName name = do
          logInfo $ "Using profile: " <> name
          return profile
    | otherwise =
          panic $ "Cannot find profile: " <> name

addGlobsFromProfile :: Profile -> FileGlobs -> FileGlobs
addGlobsFromProfile Profile{..} (FileGlobs inc exc) =
    FileGlobs (inc ++ profileInclude) (exc ++ profileExclude)
