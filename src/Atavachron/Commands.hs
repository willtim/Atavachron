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
import Control.Logging
import Control.Monad
import Control.Monad.Reader
import Control.Monad.State.Strict
import Control.Monad.Trans.Resource

import qualified Data.ByteString.Lazy as LB
import Data.Maybe
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as T
import Data.Time.Clock

import qualified System.IO as IO

import GHC.Conc (numCapabilities)

import Text.Printf

import Streaming.Prelude as S (filter)
import System.FilePath.Glob
import qualified System.Directory as Dir
import qualified System.Posix.Files as Files

import Atavachron.Chunk.Encode (hexEncode)
import Atavachron.Config
import Atavachron.Env
import Atavachron.Path
import Atavachron.Pipelines
import Atavachron.Repository (Repository(..), Snapshot(..), SnapshotName, CachedCredentials)
import Atavachron.Store (Store)
import Atavachron.Streaming (mkTaskGroup)
import Atavachron.Tree (FileMeta(..), Diff(..))
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

data KeysArgument
    = ListKeys
    | AddKey Text

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
        CConfig options     -> config options mfp

------------------------------------------------------------

initialise :: InitOptions -> Maybe Config -> IO ()
initialise InitOptions{..} _ = do
    store <- parseURL' repoURL
    pass  <- newPassword
    cc    <- Repository.initRepository store pass
    let url = urlText repoURL
    saveCredentials url cc
    T.putStrLn $ "Repository created at " <> url
initialise InitOptionsProfile{..} mcfg = do
    p <- getProfile profileName mcfg
    initialise (InitOptions{repoURL = profileLocation p}) mcfg

backup :: BackupOptions -> Maybe Config -> IO ()
backup BackupOptions{..} mcfg = do
    sourceDir <- parseAbsDir' sourceDir
    store     <- parseURL'    repoURL
    repo      <- authenticate store
    runBackup mcfg repo sourceDir fileGlobs forceFullScan
    T.putStrLn $ "Backup complete."
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
    T.putStrLn $ "Verification complete."
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
    T.putStrLn $ "Restore complete."
restore RestoreOptionsProfile{..} mcfg = do
    p <- getProfile profileName mcfg
    restore (RestoreOptions{repoURL = profileLocation p
                           ,snapshotID
                           ,targetDir
                           ,fileGlobs = addGlobsFromProfile p fileGlobs
                           }) mcfg

snapshots :: SnapshotOptions -> Maybe Config -> IO ()
snapshots SnapshotOptions{..} _ = do
    s <- maybe (return Nothing) (fmap Just . parseAbsDir') sourceDir
    listSnapshots repoURL s
snapshots SnapshotOptionsProfile{..} mcfg = do
    p <- getProfile profileName mcfg
    s <- parseAbsDir' (profileSource p)
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

config :: ConfigOptions -> Maybe FilePath -> IO ()
config GenerateConfig mfp = writeDefaultConfigFile mfp
config ValidateConfig mfp = do
    res <- tryLoadingConfigFile mfp
    case res of
        Just{}  -> printf "Configuration loaded successfully."
        Nothing -> printf "No configuration file found."

listSnapshots :: URL -> Maybe (Path Abs Dir) -> IO ()
listSnapshots repoURL sourceDir = do
    store     <- parseURL'    repoURL
    repo      <- authenticate store
    let stream = S.filter snapshotsFilter
               $ Repository.listSnapshots repo
    flip S.mapM_ stream $ \(key, e'snap) ->
        case e'snap of
            Left ex            ->
                errorL' $ "Failed to fetch snapshot: " <> T.pack (show ex)
            Right Snapshot{..} -> do
                hostDir <- getFilePath sHostDir
                printf "%s | %-8.8s | %-8.8s | %-32.32s | %-16.16s | %-16.16s | %s \n"
                       (T.unpack $ T.take 8 key)
                       (T.unpack sUserName)
                       (T.unpack sHostName)
                       hostDir
                       (show sStartTime)
                       (show sFinishTime)
                       (maybe "" (T.unpack . T.take 8 . hexEncode) sExeBinary)
  where
    -- TODO we should offer the ability to filter by more than just sourceDir,
    -- for example: hostname and username
    snapshotsFilter (_, Right s) = Just (sHostDir s) == sourceDir
    snapshotsFilter _ = True -- always want to report errors

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
    S.mapM_ (T.putStrLn . fst) $ Repository.listAccessKeys (repoStore repo)

addAccessKey :: URL -> Text -> IO ()
addAccessKey repoURL name = do
    store <- parseURL' repoURL
    T.putStrLn "Checking existing credentials."
    repo  <- authenticate store
    T.putStrLn "Please provide the additional credentials."
    pass  <- newPassword
    cc    <- Repository.newAccessKey (repoStore repo) (repoManifestKey repo) name pass
    saveCredentials (urlText repoURL) cc

runBackup :: Maybe Config -> Repository -> Path Abs Dir -> FileGlobs -> Bool -> IO ()
runBackup mcfg repo sourceDir globs forceFullScan = do
    env      <- makeEnv mcfg repo sourceDir globs
    snapshot <-
        runResourceT
          . flip evalStateT initialProgress
          . flip runReaderT env
          $ backupPipeline forceFullScan sourceDir

    res <- Repository.putSnapshot repo snapshot
    case res of
        Left ex   -> errorL' $ "Failed to write snapshot: " <> T.pack (show ex)
        Right key -> do
            T.hPutStrLn IO.stderr $ "\nWrote snapshot " <> T.take 8 key
            runReaderT commitFilesCache env

runVerify :: Maybe Config -> Repository -> Snapshot -> FileGlobs -> IO ()
runVerify mcfg repo snapshot globs = do
    env <- makeEnv mcfg repo rootDir globs
    runResourceT
        . flip evalStateT initialProgress
        . flip runReaderT env
        . S.mapM_ logFailed -- for now, just log files with errors
        $ verifyPipeline snapshot
  where
    logFailed (item, VerifyResult errors) =
        unless (null errors) $ do
            path <- liftIO $ getFilePath (filePath item)
            warn' $ "File has errors: " <> (T.pack path)

runRestore :: Maybe Config -> Repository -> Snapshot -> Path Abs Dir -> FileGlobs -> IO ()
runRestore mcfg repo snapshot targetDir globs = do
    env <- makeEnv mcfg repo targetDir globs
    runResourceT
        . flip evalStateT initialProgress
        . flip runReaderT env
        $ restoreFiles snapshot

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
        then do debug' $ "Using cached credentials."
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
    debug' $ "Available cores: " <> T.pack (show numCapabilities)
    startT      <- getCurrentTime

    -- default to a conservative size to minimise memory usage.
    let taskBufferSize = maybe numCapabilities fromIntegral
                       $ getOverride configTaskBufferSize

    taskGroup   <- mkTaskGroup
                 . maybe numCapabilities fromIntegral
                 $ getOverride configTaskThreads

    cachePath   <- maybe getCachePath parseAbsDir'
                 $ getOverride configCachePath
    return Env
         { envRepository     = repo
         , envStartTime      = startT
         , envTaskBufferSize = taskBufferSize
         , envTaskGroup      = taskGroup
         , envRetries        = maybe 5 fromIntegral $ configMaxRetries <$> mcfg
         , envCachePath      = cachePath
         , envFilePredicate  = parseGlobs globs
         , envDirectory      = localDir
         , envBackupBinary   = fromMaybe False $ configBackupBinary <$> mcfg
         }
  where
      getOverride :: (Config -> Overridable a) -> Maybe a
      getOverride f = mcfg >>= overridableToMaybe . f

-- | For now, default to XDG standard
getCachePath :: IO (Path Abs Dir)
getCachePath =
    fromMaybe (errorL' "Cannot parse XDG directory") . parseAbsDir
        <$> Dir.getXdgDirectory Dir.XdgCache "atavachron"

-- | Logs and throws, if it cannot parse the path.
parseAbsDir' :: Text -> IO (Path Abs Dir)
parseAbsDir' t =
    case parseAbsDir (T.unpack t) of
        Nothing   -> errorL' $ "Cannot parse absolute path: " <> t
        Just path -> return path

-- | Logs and throws, if it cannot retrieve the snapshot
getSnapshot :: Repository -> Text -> IO Snapshot
getSnapshot repo partialKey = do
    e'snap <- Repository.getSnapshot repo partialKey
    case e'snap of
        Left ex    -> errorL' $ "Could not retrieve snapshot: " <> T.pack (show ex)
        Right snap -> return snap

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
    patt = simplify $ compile $ T.unpack g

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
    either (errorL' . ("Cannot parse URL: "<>)) id <$> parseURL repoURL

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
          log' $ "Using profile: " <> name
          return profile
    | otherwise =
          errorL' $ "Cannot find profile: " <> name

addGlobsFromProfile :: Profile -> FileGlobs -> FileGlobs
addGlobsFromProfile Profile{..} (FileGlobs inc exc) =
    FileGlobs (inc ++ profileInclude) (exc ++ profileExclude)
