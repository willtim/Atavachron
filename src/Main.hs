module Main where

import Atavachron.Commands

import Control.Logging
import qualified Crypto.Saltine as Saltine

import Data.Semigroup ((<>))
import Data.Text (Text)
import Options.Applicative

import Paths_Atavachron (version)
import Data.Version (showVersion)


data Options = Options
  { optCommand  :: Command
  , optLogLevel :: LogLevel
  }

optionsP :: Parser Options
optionsP = Options
  <$> subparser
    (  command "init"    (info initOptionsP   ( progDesc "Initialise a new repository." ))
    <> command "backup"  (info backupOptionsP  ( progDesc "Create and upload a new snapshot to the repository." ))
    <> command "verify"  (info verifyOptionsP  ( progDesc "Verify the integrity of a snapshot and its files in the repository." ))
    <> command "restore" (info restoreOptionsP ( progDesc "Restore files from a snapshot to a target directory." ))
    <> command "list"    (info listOptionsP    ( progDesc "List snapshots and/or files." ))
    <> command "diff"    (info diffOptionsP    ( progDesc "Diff two snapshots." ))
    -- <> command "help"   (info helpOptionsP   ( progDesc "Help for a particular command"))
    )
  <*> logLevelP

logLevelP :: Parser LogLevel
logLevelP = flag LevelInfo LevelDebug
  (  long "debug"
  <> help "Debug logging" )

initOptionsP :: Parser Command
initOptionsP = CInit <$> (InitOptions <$> repoUrlP)

backupOptionsP :: Parser Command
backupOptionsP = CBackup <$> (BackupOptions <$> sourceDirP <*> repoUrlP)

verifyOptionsP :: Parser Command
verifyOptionsP = CVerify <$> (VerifyOptions <$> snapIdP <*> repoUrlP)

restoreOptionsP :: Parser Command
restoreOptionsP = CRestore <$> (RestoreOptions <$> snapIdP <*> repoUrlP <*> targetDirP <*> includeP)

listOptionsP :: Parser Command
listOptionsP = CList <$> (ListOptions <$> repoUrlP <*> listArgP)

listArgP :: Parser ListArgument
listArgP = listSnapshotsP <|> listFilesP

diffOptionsP :: Parser Command
diffOptionsP = CDiff <$> (DiffOptions <$> repoUrlP <*> snapIdP <*> snapIdP)

listSnapshotsP :: Parser ListArgument
listSnapshotsP = flag' ListSnapshots
  (  long "snapshots"
  <> help "List snapshots" )

listFilesP :: Parser ListArgument
listFilesP = ListFiles <$> snapIdP

sourceDirP :: Parser Text
sourceDirP = strOption
  (  long "source-dir"
  <> short 'd'
  <> metavar "SOURCE"
  <> help "Source directory" )

targetDirP :: Parser Text
targetDirP = strOption
  (  long "target-dir"
  <> short 'd'
  <> metavar "TARGET"
  <> help "Target directory" )

snapIdP :: Parser Text
snapIdP = strArgument
  (  metavar "SNAPSHOT-ID"
  <> help "Snapshot ID" )

includeP :: Parser (Maybe Text)
includeP = optional $ strOption
  (  long "include"
  <> short 'i'
  <> metavar "PATTERN"
  <> help "Only include files matching PATTERN" )

repoUrlP :: Parser Text
repoUrlP = strOption
  (  long "repository"
  <> short 'r'
  <> metavar "REPO-URL"
  <> help "The repository URL" )

main :: IO ()
main = do
    Saltine.sodiumInit
    options <- execParser $ info (optionsP <**> helper)
               ( fullDesc
                 <> header (unwords [ "Atavachron"
                                    , showVersion version
                                    , "© 2018 Tim Williams"
                                    ]))
    withStdoutLogging $ do
        setLogLevel (optLogLevel options)
        runCommand (optCommand options)
