module Main where

import Control.Logging
import qualified Crypto.Saltine as Saltine

import Data.Semigroup ((<>))
import Data.Text (Text)
import Options.Applicative

import Paths_atavachron (version)
import Data.Version (showVersion)

import Atavachron.Commands
import Atavachron.Config (URL(..))

data Options = Options
  { optCommand    :: Command
  , optConfigFile :: Maybe FilePath
  , optLogLevel   :: LogLevel
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
    <> command "config"    (info configOptionsP   ( progDesc "Validate or generate a configuration file."))
    -- <> command "help"      (info helpOptionsP    ( progDesc "Help for a particular command"))
    )
  <*> configFileP
  <*> logLevelP

configFileP :: Parser (Maybe FilePath)
configFileP = optional $ strOption
  (  long "config"
  <> short 'c'
  <> metavar "FILEPATH"
  <> help "configuration file path" )

logLevelP :: Parser LogLevel
logLevelP = flag LevelInfo LevelDebug
  (  long "debug"
  <> help "Debug logging" )

initOptionsP :: Parser Command
initOptionsP = CInit <$>
    ((InitOptions <$> repoUrlP) <|> (InitOptionsProfile <$> profileNameP))

backupOptionsP :: Parser Command
backupOptionsP = CBackup <$>
    ((BackupOptions <$> repoUrlP <*> sourceDirP <|> BackupOptionsProfile <$> profileNameP)
    <*> fileGlobsP
    <*> forceFullScanP)

verifyOptionsP :: Parser Command
verifyOptionsP = CVerify <$>
    ((VerifyOptions <$> repoUrlP <|> VerifyOptionsProfile <$> profileNameP)
    <*> snapIdP
    <*> fileGlobsP)

restoreOptionsP :: Parser Command
restoreOptionsP = CRestore <$>
    ((RestoreOptions <$> repoUrlP <|> RestoreOptionsProfile <$> profileNameP)
    <*> snapIdP
    <*> targetDirP
    <*> fileGlobsP)

snapshotOptionsP :: Parser Command
snapshotOptionsP = CSnapshots <$>
    ((SnapshotOptions <$> repoUrlP <*> optional sourceDirP) <|>
     (SnapshotOptionsProfile <$> profileNameP))

listOptionsP :: Parser Command
listOptionsP = CList <$>
    ((ListOptions <$> repoUrlP <|> ListOptionsProfile <$> profileNameP)
    <*> snapIdP
    <*> fileGlobsP)

diffOptionsP :: Parser Command
diffOptionsP = CDiff <$>
    ((DiffOptions <$> repoUrlP <|> DiffOptionsProfile <$> profileNameP)
    <*> snapIdP
    <*> snapIdP)

keyOptionsP :: Parser Command
keyOptionsP = CKeys <$>
    ((KeyOptions <$> repoUrlP <|> KeyOptionsProfile <$> profileNameP)
    <*> keysArgP)

configOptionsP :: Parser Command
configOptionsP = CConfig <$> (validateP <|> generateP)

keysArgP :: Parser KeysArgument
keysArgP = listKeysP <|> addKeyP

validateP :: Parser ConfigOptions
validateP = flag' ValidateConfig
  (  long "validate"
  <> help "Validate configuration file" )

generateP :: Parser ConfigOptions
generateP = flag' GenerateConfig
  (  long "generate"
  <> help "Generate default configuration file" )

fileGlobsP :: Parser FileGlobs
fileGlobsP = FileGlobs <$> includeGlobsP <*> excludeGlobsP

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

includeGlobsP :: Parser [Text]
includeGlobsP = many $ strOption
  (  long "include"
  <> short 'i'
  <> metavar "PATTERN"
  <> help "Only include files matching PATTERN" )

excludeGlobsP :: Parser [Text]
excludeGlobsP = many $ strOption
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

profileNameP :: Parser Text
profileNameP = strOption
  (  long "profile"
  <> short 'p'
  <> metavar "PROFILE-NAME"
  <> help "A profile defined in a configuration file" )

forceFullScanP :: Parser Bool
forceFullScanP = flag False True
  (  long "force-full-scan"
  <> help "Force reading and chunking all files, even if metadata unchanged" )

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
        runCommand (optConfigFile options) (optCommand options)
