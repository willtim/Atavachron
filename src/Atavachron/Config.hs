{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}

-- | Support for an Expresso-based configuration file.
--
-- The configuration file can hold "profiles" containing commonly used sets of backup parameters.
-- It also allows us to change/tweak various runtime parameters.
--
module Atavachron.Config (
      Config(..)
    , Profile(..)
    , Pruning(..)
    , PruneSettings(..)
    , Overridable(..)
    , URL(..)
    , findProfileByName
    , overridableToMaybe
    , tryLoadingConfigFile
    , writeDefaultConfigFile
    ) where

import Control.Logging
import Control.Monad (when)
import Control.Monad.Except (ExceptT(..), runExceptT)
import Expresso
import Expresso.TH.QQ
import qualified Data.List as L
import Data.Text (Text)
import qualified Data.Text as T

import System.Info
import System.Directory
import System.FilePath


-- | Schema for Atavachron configuration files.
schema :: Type
schema =
    [expressoType|
        { cachePath         : Overridable Text
        , taskThreads       : Overridable Int
        , taskBufferSize    : Overridable Int
        , garbageExpiryDays : Overridable Int
        , maxRetries        : Int
        , backupBinary      : Bool
        , profiles :
           [ { name     : Text
             , location : Text
             , include  : [Text]
             , exclude  : [Text]
             , source   : Text
             , pruning  : < Disabled : {}
                          , Enabled  :
                            { keep_daily   : Maybe Int
                            , keep_weekly  : Maybe Int
                            , keep_monthly : Maybe Int
                            , keep_yearly  : Maybe Int
                            }
                          >
             }
           ]
        }
    |]

-- | Type synonyms used in the schema definition.
synonyms :: [SynonymDecl]
synonyms =
    [ [expressoTypeSyn| type Maybe a = <Just : a, Nothing : {}> |]
    , [expressoTypeSyn| type Overridable a = <Default : {}, Override : a> |]
    ]

-- | Haskell representation of the Atavachron configuration file.
data Config = Config
    { configCachePath          :: Overridable Text
    , configTaskThreads        :: Overridable Integer
    , configTaskBufferSize     :: Overridable Integer
    , configGarbageExpiryDays  :: Overridable Integer
    , configMaxRetries         :: Integer
    , configBackupBinary       :: Bool
    , configProfiles           :: [Profile]
    } deriving Show

-- | Values that have (potentially runtime dependent) defaults.
data Overridable a = Default | Override a
    deriving Show

overridableToMaybe :: Overridable a -> Maybe a
overridableToMaybe (Override x) = Just x
overridableToMaybe Default = Nothing

-- | A backup configuration profile.
data Profile = Profile
    { profileName     :: Text
    , profileLocation :: URL    -- i.e. repository
    , profileInclude  :: [Text]
    , profileExclude  :: [Text]
    , profileSource   :: Text   -- i.e. source directory
    , profilePruning  :: Pruning
    } deriving Show

-- | Pruning can be either disabled or enabled with settings.
data Pruning = Disabled | Enabled PruneSettings
    deriving Show

-- | Settings for the prune operation.
-- Warning: If all fields are set to Nothing, everything
-- will be pruned!
data PruneSettings = PruneSettings
  { keepDaily   :: Maybe Integer
  , keepWeekly  :: Maybe Integer
  , keepMonthly :: Maybe Integer
  , keepYearly  :: Maybe Integer
  } deriving Show

-- | An (unchecked) URL
newtype URL = URL { urlText :: Text }
    deriving Show

-- | Config files are entirely optional, we only fail with an error if
-- one of the following is true:
-- * the user has explicitly provided the location and we cannot find it;
-- * we find it but cannot parse or typecheck it.
tryLoadingConfigFile :: Maybe FilePath -> IO (Maybe Config)
tryLoadingConfigFile (Just explicitFilePath) = do
    exists <- doesFileExist explicitFilePath
    when (not exists) $
        errorL' $ "Cannot find configuration file at supplied location: "
                <> T.pack explicitFilePath
    Just <$> loadConfig explicitFilePath
tryLoadingConfigFile Nothing = do
    filePath <- getDefaultConfigFilePath
    exists   <- doesFileExist filePath
    if exists
       then Just <$> loadConfig filePath
       else return Nothing

-- | Write a default (empty) config file, taking care never to overwrite an
-- existing file.
writeDefaultConfigFile :: Maybe FilePath -> IO ()
writeDefaultConfigFile mfp = do
    filePath <- maybe getDefaultConfigFilePath return mfp
    exists   <- doesFileExist filePath
    when exists $
        errorL' $ "Configuration file already exists at supplied location: "
                <> T.pack filePath
    str <- showValue' $ inj emptyConfig
    writeFile filePath str
    log' $ "Wrote empty configuration file: " <> T.pack filePath

loadConfig :: FilePath -> IO Config
loadConfig filePath = do
    res <- loadConfig' filePath
    case res of
        Left err  -> errorL' $ "Could not load configuration file: " <> T.pack err
        Right cfg -> do
            log' $ "Loaded configuration file: " <> T.pack filePath
            return cfg

loadConfig' :: FilePath -> IO (Either String Config)
loadConfig' filePath = runExceptT $ do
    envs <- installSynonyms synonyms envs
    ExceptT $ evalFile' envs (Just schema) filePath
  where
    -- TODO install some additional built-ins
    envs = installBinding "system" TText (inj System.Info.os)
         . installBinding "doesPathExist" (TFun TText TBool) (inj doesPathExist)
         $ initEnvironments

getDefaultConfigFilePath :: IO FilePath
getDefaultConfigFilePath = do
    configDir <- getXdgDirectory XdgConfig "atavachron"
    createDirectoryIfMissing True configDir
    return $ configDir </> "atavachron.x"

emptyConfig :: Config
emptyConfig = Config
    { configCachePath         = Default
    , configTaskThreads       = Default
    , configTaskBufferSize    = Default
    , configGarbageExpiryDays = Default
    , configMaxRetries        = 5
    , configBackupBinary      = True
    , configProfiles          = []
    }

findProfileByName :: Text -> Config -> Maybe Profile
findProfileByName name cfg =
    L.find (\p -> profileName p == name) $ configProfiles cfg


------------------------------------------------------------
-- HasValue instances

instance HasValue Config where
    proj v = Config
        <$> v .: "cachePath"
        <*> v .: "taskThreads"
        <*> v .: "taskBufferSize"
        <*> v .: "garbageExpiryDays"
        <*> v .: "maxRetries"
        <*> v .: "backupBinary"
        <*> v .: "profiles"

    inj Config{..} = mkRecord
        [ "cachePath"         .= inj configCachePath
        , "taskThreads"       .= inj configTaskThreads
        , "taskBufferSize"    .= inj configTaskBufferSize
        , "garbageExpiryDays" .= inj configGarbageExpiryDays
        , "maxRetries"        .= inj configMaxRetries
        , "backupBinary"      .= inj configBackupBinary
        , "profiles"          .= inj configProfiles
        ]

instance HasValue a => HasValue (Overridable a) where
    proj = choice [("Override", fmap Override . proj)
                  ,("Default",  const $ pure Default)
                  ]
    inj (Override x) = mkVariant "Override" (inj x)
    inj Default = mkVariant "Default" unit

instance HasValue Pruning where
    proj = choice [("Enabled", fmap Enabled . proj)
                  ,("Disabled",  const $ pure Disabled)
                  ]
    inj (Enabled s) = mkVariant "Enabled" (inj s)
    inj Disabled = mkVariant "Disabled" unit

instance HasValue Profile where
    proj v = Profile
        <$> v .: "name"
        <*> v .: "location"
        <*> v .: "include"
        <*> v .: "exclude"
        <*> v .: "source"
        <*> v .: "pruning"

    inj Profile{..} = mkRecord
        [ "name"     .= inj profileName
        , "location" .= inj profileLocation
        , "include"  .= inj profileInclude
        , "exclude"  .= inj profileExclude
        , "source"   .= inj profileSource
        , "pruning"  .= inj profilePruning
        ]

instance HasValue PruneSettings where
    proj v = PruneSettings
        <$> v .: "keep_daily"
        <*> v .: "keep_weekly"
        <*> v .: "keep_monthly"
        <*> v .: "keep_yearly"

    inj PruneSettings{..} = mkRecord
        [ "keep_daily"   .= inj keepDaily
        , "keep_weekly"  .= inj keepWeekly
        , "keep_monthly" .= inj keepMonthly
        , "keep_yearly"  .= inj keepYearly
        ]

instance HasValue URL where
    proj v = URL <$> proj v
    inj (URL s) = inj s
