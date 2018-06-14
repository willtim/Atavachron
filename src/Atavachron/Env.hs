{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

-- | Environment types used throught Atavachron pipelines.
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


 -- | Environment used during backup, verify and restore.
data Env params = Env
  { envRepository     :: Repository       -- ^ The remote destination repository.
  , envStartTime      :: !UTCTime         -- ^ Start time of current backup.
  , envRetries        :: !Int             -- ^ Number of times to retry failed gets and puts to the store.
--  , envTaskGroup      :: !TaskGroup       -- ^ Sized with the number of processing cores.
  , envCachePath      :: !(Path Abs Dir)  -- ^ Directory used to store local cache files.
  , envParams         :: params           -- ^ Task specific parameters.
  }

data Backup = Backup
  { bSourceDir  :: !(Path Abs Dir)  -- ^ the local directory being backed up
  }

data Restore = Restore
  { rTargetDir  :: !(Path Abs Dir) -- ^ the local directory to restore files to
  , rPredicate  :: !FilePredicate  -- ^ predicate which defines what to restore
  }

-- for now, just a predicate on the filepath
newtype FilePredicate = FilePredicate
    { unFilePredicate :: forall t. Path Rel t -> IO Bool }

applyPredicate :: FilePredicate -> Path Rel t -> IO Bool
applyPredicate (FilePredicate f) item = f item

allFiles :: FilePredicate
allFiles = FilePredicate (const $ return True)

-- | Progress state used during backup.
-- TODO throughput rates, corrupt file/chunks
data Progress = Progress
  { _prFiles           :: !Int64
  , _prChunks          :: !Int64
  , _prInputSize       :: !Int64
  , _prDedupSize       :: !Int64 -- deduplicated size
  , _prCompressedSize  :: !Int64 -- deduplicated and compressed size
  , _prErrors          :: !Int64
  , _prWarnings        :: !Int64
  } deriving (Show)

makeLenses ''Progress

initialProgress :: Progress
initialProgress = Progress 0 0 0 0 0 0 0

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
