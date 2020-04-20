{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RankNTypes #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

-- | Environment and other types used throughout the Atavachron pipelines.
--

module Atavachron.Env where

import Control.Exception
import Control.Monad.IO.Class

import Data.Int
import Data.IORef
import Data.Typeable
import Data.Time (UTCTime)

import Numeric.Natural

import Atavachron.Repository
import Atavachron.Path
import Atavachron.Chunk.Builder
import Atavachron.Chunk.Encode


-- | Environment used during backup, verify and restore.
data Env = Env
  { envRepository        :: Repository     -- ^ The remote destination repository.
  , envStartTime         :: UTCTime        -- ^ Start time of current backup.
  , envRetries           :: Natural        -- ^ Number of times to retry failed gets and puts to the store.
  , envTaskBufferSize    :: Natural        -- ^ Size of task queue used for parallel processing.
  , envTaskGroup         :: Natural        -- ^ Number of parallel worker threads (default is available processing cores).
  , envCachePath         :: Path Abs Dir   -- ^ Directory used to store local cache files.
  , envFilePredicate     :: FilePredicate  -- ^ Predicate for including/excluding files to process.
  , envDirectory         :: Path Abs Dir   -- ^ The local directory being backed up or restored to.
  , envBackupBinary      :: Bool           -- ^ If true, include an (unencrypted) backup of the program binary.
  , envGarbageExpiryDays :: Natural        -- ^ A garbage expiry time in days, greater than the longest likely backup.
  , envProgressRef       :: IORef Progress -- ^ Global variable holding the backup progress indicator.
  }

-- for now, just a predicate on the filepath
newtype FilePredicate = FilePredicate
    { unFilePredicate :: forall t. Path Rel t -> IO Bool }

applyPredicate :: FilePredicate -> Path Rel t -> IO Bool
applyPredicate (FilePredicate f) = f

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
-- NOTE: Progress is a monoid in order to allow aggregation of the
-- progress of concurrent tasks.
data Progress = Progress
  { prFiles      :: !Int64
  , prChunks     :: !Int64
  , prInputSize  :: !Int64
  , prDedupSize  :: !Int64 -- deduplicated size
  , prStoredSize :: !Int64 -- deduplicated and compressed size
  , prErrors     :: !Int64 -- download errors, upload retries
  } deriving (Show)

instance Semigroup Progress where
    (<>) p q =
        Progress { prFiles      = prFiles p      + prFiles q
                 , prChunks     = prChunks p     + prChunks q
                 , prInputSize  = prInputSize p  + prInputSize q
                 , prDedupSize  = prDedupSize p  + prDedupSize q
                 , prStoredSize = prStoredSize p + prStoredSize q
                 , prErrors     = prErrors p     + prErrors q
                 }

instance Monoid Progress where
    mempty = Progress 0 0 0 0 0 0
    mappend = (<>)

updateProgress :: MonadIO m => IORef Progress -> Progress -> m Progress
updateProgress ref upd =
    liftIO $ atomicModifyIORef' ref $ \p -> let !p' = p <> upd in (p', p')

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
