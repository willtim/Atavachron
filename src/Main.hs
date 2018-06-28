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
    (  command "init"      (info initOptionsP     ( progDesc "Initialise a new repository." ))
    <> command "backup"    (info backupOptionsP   ( progDesc "Create and upload a new snapshot to the repository." ))
    <> command "verify"    (info verifyOptionsP   ( progDesc "Verify the integrity of a snapshot and its files in the repository." ))
    <> command "restore"   (info restoreOptionsP  ( progDesc "Restore files from a snapshot to a target directory." ))
    <> command "snapshots" (info snapshotOptionsP ( progDesc "List snapshots in the repository." ))
    <> command "list"      (info listOptionsP     ( progDesc "List files for a particular snapshot." ))
    <> command "diff"      (info diffOptionsP     ( progDesc "Diff two snapshots." ))
    <> command "keys"      (info keyOptionsP      ( progDesc "Management of password-protected access keys."))
    -- <> command "help"      (info helpOptionsP    ( progDesc "Help for a particular command"))
    )
  <*> logLevelP

logLevelP :: Parser LogLevel
logLevelP = flag LevelInfo LevelDebug
  (  long "debug"
  <> help "Debug logging" )

initOptionsP :: Parser Command
initOptionsP = CInit <$> (InitOptions <$> repoUrlP)

backupOptionsP :: Parser Command
backupOptionsP = CBackup <$> (BackupOptions <$> repoUrlP <*> sourceDirP <*> globPairP)

verifyOptionsP :: Parser Command
verifyOptionsP = CVerify <$> (VerifyOptions <$> repoUrlP <*> snapIdP <*> globPairP)

restoreOptionsP :: Parser Command
restoreOptionsP = CRestore <$> (RestoreOptions <$> repoUrlP <*> snapIdP <*> targetDirP <*> globPairP)

snapshotOptionsP :: Parser Command
snapshotOptionsP = CSnapshots <$> (SnapshotOptions <$> repoUrlP)

listOptionsP :: Parser Command
listOptionsP = CList <$> (ListOptions <$> repoUrlP <*> snapIdP <*> globPairP)

diffOptionsP :: Parser Command
diffOptionsP = CDiff <$> (DiffOptions <$> repoUrlP <*> snapIdP <*> snapIdP)

keyOptionsP :: Parser Command
keyOptionsP = CKeys <$> (KeyOptions <$> repoUrlP <*> keysArgP)

keysArgP :: Parser KeysArgument
keysArgP = listKeysP <|> addKeyP

globPairP :: Parser GlobPair
globPairP = GlobPair <$> includeGlobP <*> excludeGlobP

listKeysP :: Parser KeysArgument
listKeysP = flag' ListKeys
  (  long "list"
  <> help "List access keys" )

addKeyP :: Parser KeysArgument
addKeyP = AddKey <$> strOption
  (  long "add"
  <> metavar "NAME"
  <> help "Add access key" )

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

includeGlobP :: Parser (Maybe Text)
includeGlobP = optional $ strOption
  (  long "include"
  <> short 'i'
  <> metavar "PATTERN"
  <> help "Only include files matching PATTERN" )

excludeGlobP :: Parser (Maybe Text)
excludeGlobP = optional $ strOption
  (  long "exclude"
  <> short 'e'
  <> metavar "PATTERN"
  <> help "Exclude any files matching PATTERN" )

repoUrlP :: Parser URL
repoUrlP = URL <$> strOption
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
