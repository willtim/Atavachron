{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GADTs #-}

-- |  High-level commands exposed to the command-line interface.
--
module Atavachron.Commands where

import Prelude hiding (concatMap)

import Control.Exception
import Control.Logging
import Control.Monad
import Control.Monad.Reader
import Control.Monad.State
import Control.Monad.Trans.Resource

import Data.Maybe
import Data.Monoid
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as T
import Data.Time.Clock

import qualified System.IO as IO

import GHC.Conc (numCapabilities)

import Text.Printf

import System.FilePath.Glob
import qualified System.Directory as Dir

import Atavachron.Path
import Atavachron.Tree (FileMeta(..), Diff(..))
import qualified Atavachron.Tree as Tree
import Atavachron.Repository (Repository(..), Snapshot(..), SnapshotName)
import qualified Atavachron.Repository as Repository
import Atavachron.Env
import Atavachron.Pipelines
import Atavachron.Streaming (mkTaskGroup)
import qualified Atavachron.Streaming as S


data Command
  = CInit    InitOptions
  | CBackup  BackupOptions
  | CVerify  VerifyOptions
  | CRestore RestoreOptions
  | CList    ListOptions
  | CDiff    DiffOptions
--  | Help

-- Wherefore art thou OverloadedRecordLabels?

data InitOptions = InitOptions
    { iRepoURL    :: Text
    }

data BackupOptions = BackupOptions
    { bSourceDir  :: Text
    , bRepoURL    :: Text
    }

data VerifyOptions = VerifyOptions
    { vSnapshotID :: Text
    , vRepoURL    :: Text
    }

data RestoreOptions = RestoreOptions
    { rSnapshotID  :: Text
    , rRepoURL     :: Text
    , rTargetDir   :: Text
    , rIncludeGlob :: Maybe Text
    }

data ListOptions = ListOptions
    { lRepoURL  :: Text
    , lArgument :: ListArgument
    }

data ListArgument
    = ListSnapshots
    | ListFiles SnapshotName
    -- | Keys

data DiffOptions = DiffOptions
    { dRepoURL     :: Text
    , dSnapshotID1 :: Text
    , dSnapshotID2 :: Text
    }

runCommand :: Command -> IO ()
runCommand (CInit options)    = initialise options
runCommand (CBackup options)  = backup options
runCommand (CVerify options)  = verify options
runCommand (CRestore options) = restore options
runCommand (CList options)    = list options
runCommand (CDiff options)    = diff options

------------------------------------------------------------

initialise :: InitOptions -> IO ()
initialise InitOptions{..} = do
    pass  <- newPassword
    void $ Repository.initRepository iRepoURL pass
    T.hPutStrLn IO.stderr $ "Repository successfully created at " <> iRepoURL

backup :: BackupOptions -> IO ()
backup BackupOptions{..} = do
    pass      <- askPassword
    sourceDir <- parseAbsDir' bSourceDir
    repo      <- Repository.resolveRepository bRepoURL pass
    runBackup repo sourceDir

verify :: VerifyOptions -> IO ()
verify VerifyOptions{..} = do
    pass      <- askPassword
    repo      <- Repository.resolveRepository vRepoURL pass
    snap      <- getSnapshot repo  vSnapshotID
    runVerify repo snap

restore :: RestoreOptions -> IO ()
restore RestoreOptions{..} = do
    pass      <- askPassword
    targetDir <- parseAbsDir' rTargetDir
    repo      <- Repository.resolveRepository rRepoURL pass
    snap      <- getSnapshot repo rSnapshotID
    let filePred = maybe allFiles parseGlob rIncludeGlob
    runRestore repo snap filePred targetDir

list :: ListOptions -> IO ()
list ListOptions{..} = do
    case lArgument of
        ListSnapshots        -> listSnapshots lRepoURL
        ListFiles partialKey -> listFiles lRepoURL partialKey

diff :: DiffOptions -> IO ()
diff DiffOptions{..} = do
    pass      <- askPassword
    repo      <- Repository.resolveRepository dRepoURL pass
    env       <- makeEnv (Restore (AbsDir mempty) allFiles) repo
    snap1     <- getSnapshot repo dSnapshotID1
    snap2     <- getSnapshot repo dSnapshotID2
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

listSnapshots :: Text -> IO ()
listSnapshots repoURL = do
    pass      <- askPassword
    repo      <- Repository.resolveRepository repoURL pass
    flip S.mapM_ (Repository.listSnapshots repo) $ \(key, e'snap) ->
        case e'snap of
            Left ex            ->
                errorL' $ "Failed to fetch snapshot: " <> T.pack (show ex)
            Right Snapshot{..} -> do
                hostDir <- getFilePath sHostDir
                printf "%s | %-8.8s | %-8.8s | %-32.32s | %-16.16s | %-16.16s\n"
                       (T.unpack $ T.take 8 key)
                       (T.unpack sUserName)
                       (T.unpack sHostName)
                       hostDir
                       (show sStartTime)
                       (show sFinishTime)

listFiles :: Text -> SnapshotName -> IO ()
listFiles repoURL partialKey = do
    pass <- askPassword
    repo <- Repository.resolveRepository repoURL pass
    env  <- makeEnv (Restore (AbsDir mempty) allFiles) repo
    snap <- liftIO $ getSnapshot repo partialKey
    runResourceT
        . flip evalStateT initialProgress
        . flip runReaderT env
        . S.mapM_ (liftIO . printFile . fst)
        . S.lefts
        $ snapshotTree snap
  where
    printFile item = do
        fp <- getFilePath (filePath item)
        putStrLn fp

runBackup :: Repository -> Path Abs Dir -> IO ()
runBackup repo sourceDir = do
    env      <- makeEnv (Backup sourceDir) repo
    snapshot <-
        runResourceT
          . flip evalStateT initialProgress
          . flip runReaderT env
          $ backupPipeline sourceDir

    res <- Repository.putSnapshot repo snapshot
    case res of
        Left ex   -> errorL' $ "Failed to write snapshot: " <> T.pack (show ex)
        Right key -> do
            T.hPutStrLn IO.stderr $ "\nWrote snapshot " <> T.take 8 key
            runReaderT commitFilesCache env

runVerify :: Repository -> Snapshot -> IO ()
runVerify repo snapshot = do
    env <- makeEnv (Restore (AbsDir mempty) allFiles) repo
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

runRestore :: Repository -> Snapshot -> FilePredicate -> Path Abs Dir -> IO ()
runRestore repo snapshot sourcePred targetDir = do
    env <- makeEnv (Restore targetDir sourcePred) repo
    runResourceT
        . flip evalStateT initialProgress
        . flip runReaderT env
        $ restoreFiles snapshot

-- TODO optionally read this from an Expresso config file?
makeEnv :: p -> Repository -> IO (Env p)
makeEnv params repo = do
    debug' $ "Available cores: " <> T.pack (show numCapabilities)
    startT      <- getCurrentTime
    -- for now, a conservative size to minimise memory usage.
    let taskBufferSize = numCapabilities
    taskGroup   <- mkTaskGroup numCapabilities
    -- for now, default to XDG standard
    cachePath   <- fromMaybe (errorL' "Cannot parse XDG directory") . parseAbsDir
                       <$> Dir.getXdgDirectory Dir.XdgCache "atavachron"
    return Env
         { envRepository     = repo
         , envStartTime      = startT
         , envTaskBufferSize = taskBufferSize
         , envTaskGroup      = taskGroup
         , envRetries        = 5
         , envCachePath      = cachePath
         , envParams         = params
         }

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

parseGlob :: Text -> FilePredicate
parseGlob g = FilePredicate $ \path ->
        match patt <$> getFilePath path
  where
    patt = simplify $ compile $ T.unpack g

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
