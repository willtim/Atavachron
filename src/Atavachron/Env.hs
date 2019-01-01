{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

-- | Environment and other types used throught Atavachron pipelines.
--

module Atavachron.Env where

import Control.Lens
import Control.Exception

import Data.Int
import Data.Typeable

import Data.Time (UTCTime)

import Atavachron.Repository
import Atavachron.Path
import Atavachron.Chunk.Builder
import Atavachron.Chunk.Encode
import Atavachron.Streaming (TaskGroup)


-- | Environment used during backup, verify and restore.
data Env = Env
  { envRepository     :: Repository       -- ^ The remote destination repository.
  , envStartTime      :: !UTCTime         -- ^ Start time of current backup.
  , envRetries        :: !Int             -- ^ Number of times to retry failed gets and puts to the store.
  , envTaskBufferSize :: !Int             -- ^ Size of readahead for the stream, used for parallel processing.
  , envTaskGroup      :: !TaskGroup       -- ^ Sized with the number of processing cores.
  , envCachePath      :: !(Path Abs Dir)  -- ^ Directory used to store local cache files.
  , envFilePredicate  :: !FilePredicate   -- ^ Predicate for including/excluding files to process.
  , envDirectory      :: !(Path Abs Dir)  -- ^ The local directory being backed up or restored to.
  , envBackupBinary   :: !Bool            -- ^ If true, include an (unencrypted) backup of the program binary.
  }

-- for now, just a predicate on the filepath
newtype FilePredicate = FilePredicate
    { unFilePredicate :: forall t. Path Rel t -> IO Bool }

applyPredicate :: FilePredicate -> Path Rel t -> IO Bool
applyPredicate (FilePredicate f) item = f item

allFiles :: FilePredicate
allFiles = FilePredicate (const $ return True)

noFiles :: FilePredicate
noFiles = FilePredicate (const $ return False)

conjunction :: [FilePredicate] -> FilePredicate
conjunction preds = FilePredicate $ \p ->
    and <$> mapM (`applyPredicate` p) preds

disjunction :: [FilePredicate] -> FilePredicate
disjunction preds = FilePredicate $ \p ->
    or <$> mapM (`applyPredicate` p) preds

-- | Progress state used during backup.
-- TODO better errors and warnings, e.g. show retries?
data Progress = Progress
  { _prFiles      :: !Int64
  , _prChunks     :: !Int64
  , _prInputSize  :: !Int64
  , _prDedupSize  :: !Int64 -- deduplicated size
  , _prStoredSize :: !Int64 -- deduplicated and compressed size
  , _prErrors     :: !Int64
  } deriving (Show)

makeLenses ''Progress

initialProgress :: Progress
initialProgress = Progress 0 0 0 0 0 0

data ErrorKind
  = StoreError
  | RetrieveError
  | DecryptError
  | VerifyError
  deriving Show

data Error t = Error
  { errKind    :: !ErrorKind
  , errOffsets :: !(TaggedOffsets t)
  , errStoreID :: !StoreID
  , errCause   :: !(Maybe SomeException)
  } deriving Show

instance (Typeable t, Show t) => Exception (Error t)
