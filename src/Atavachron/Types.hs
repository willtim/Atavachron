{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

-- | Domain-specific types used throughout Atavachron.
--

module Atavachron.Types where

import Codec.Serialise

import Control.Lens
import Control.Exception

import Data.Function
import Data.Int
import Data.Typeable
import qualified Data.ByteString as B

import Data.Sequence (Seq)
import Data.Text (Text)
import Data.Time (UTCTime)

import System.Posix.Types

import GHC.Generics (Generic)

import Atavachron.Path
import Atavachron.Chunk.CDC
import Atavachron.Chunk.Builder
import Atavachron.Chunk.Process
import Atavachron.Streaming (TaskGroup)
import qualified Atavachron.Store as Store


-- | Represents a remote repository used to backup files to.
-- A repository must be initialised before use.
data Repository = Repository
  { repoID       :: !Text        -- ^ The repository identifier, ideally a short readable label.
  , repoURL      :: !Text        -- ^ The repository URL
  , repoStore    :: !Store.Store -- ^ The Store implementation to use.
  , repoManifest :: !Manifest
  }

-- | A repository manifest file contains the keys and CDC parameters
-- needed to both decode existing snapshots and create new ones with
-- de-duplication. A manifest is associated with one remote URL and
-- stored at "/manifest". It is encrypted using a named master key
-- stored in "/keys" which is itself encrypted using public-key
-- cryptography.
data Manifest = Manifest
  { mVersion    :: !Int -- expected to be "1" currently
  , mChunkKey   :: !ChunkKey
  , mStoreIDKey :: !StoreIDKey
  , mCDCKey     :: !CDCKey
  , mCDCParams  :: !CDCParams
  } deriving (Generic)

-- | Generate a new manifest with fresh secret keys.
newManifest :: IO Manifest
newManifest = do
  cdcKey     <- newCDCKey
  chunkKey   <- newChunkKey
  storeIDKey <- newStoreIDKey

  return Manifest
    { mVersion    = 1
    , mChunkKey   = chunkKey
    , mStoreIDKey = storeIDKey
    , mCDCKey     = cdcKey
    , mCDCParams  = defaultCDCParams
    }

instance Serialise Manifest

-- | A snapshot represents a directory and all its contents at a given
-- point in time. These are stored at: "/snapshots/<StoreID>"
data Snapshot = Snapshot
    { sUserName   :: !Text           -- e.g. tim
    , sHostName   :: !Text           -- e.g. x1c
    , sHostDir    :: !(Path Abs Dir) -- e.g. /home/tim
    , sUID        :: !CUid
    , sGID        :: !CGid
    , sStartTime  :: !UTCTime
    , sFinishTime :: !UTCTime
    , sTree       :: !ChunkList
    } deriving (Generic, Show)

instance Serialise CUid where
    encode (CUid w) = encode w
    decode = CUid <$> decode

instance Serialise CGid where
    encode (CGid w) = encode w
    decode = CGid <$> decode

instance Serialise CMode where
    encode (CMode w) = encode w
    decode = CMode <$> decode

instance Serialise Snapshot

type Offset = Int

-- | An ordered list of chunks and associated offsets.
newtype ChunkList = ChunkList { unChunkList :: Seq (StoreID, Offset) }
  deriving (Generic, Eq, Show, Read)

instance Serialise ChunkList

-- | Like @ChunkList@, but handle small files efficiently. Used in @TreeEntry@.
data ChunkListDelta
  = NewChunkList !(Seq (StoreID, Offset))
  | PrevChunk    !Offset
  deriving (Generic, Eq, Show, Read)

instance Serialise ChunkListDelta

data TreeEntry b
  = FileEntry !(FileMeta (Path b File)) !ChunkListDelta
  | DirEntry  !(FileMeta (Path b Dir))
  | LinkEntry !(FileMeta (Path b File)) !Target
  deriving (Generic, Show)

instance Serialise (TreeEntry Rel)

-- | Tag for tree data.
data Tree = Tree
  deriving (Eq, Show)

data FileCacheEntry b = FileCacheEntry
    { ceFileItem  :: !(FileMeta (Path b File))
    , ceChunkList :: !ChunkList
    }
  deriving (Generic)

instance Serialise (FileCacheEntry Rel)

-- | The FileMeta type contains exploded metadata for a file, directory or soft link.
-- This is the archival format.
data FileMeta path = FileMeta
    { filePath   :: !path -- e.g. Abs/Rel File/Dir
    , fileMode   :: !CMode
    , fileUID    :: !CUid
    , fileGID    :: !CGid
    , fileMTime  :: !UTCTime
    , fileATime  :: !UTCTime
    , fileCTime  :: !UTCTime
    , fileINode  :: !Int64
    , fileSize   :: !Int64
    } deriving (Generic, Functor, Show)

-- FileMeta has identity based on file path.
instance Eq path => Eq (FileMeta path) where
    (==) = (==) `on` filePath

type FileItem = FileMeta (Path Abs File)
type DirItem  = FileMeta (Path Abs Dir)

-- | Non-regular files
data OtherItem
    = LinkItem !FileItem !Target
    | DirItem  !DirItem

type Target = B.ByteString

data Kind = FileK | DirK | LinkK !Target
    deriving (Generic, Eq, Show)

instance (Serialise path) => Serialise (FileMeta path)

 -- | Environment used during backup, verify and restore.
data Env params = Env
  { envRepository     :: Repository       -- ^ The remote destination repository.
  , envStartTime      :: !UTCTime         -- ^ Start time of current backup.
  , envTaskWindowSize :: !Int             -- ^ Size of readahead for the stream, used for parallel processing.
  , envRetries        :: !Int             -- ^ Number of times to retry failed gets and puts to the store.
  , envTaskGroup      :: !TaskGroup       -- ^ Sized with the number of processing cores.
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

newtype FilePredicate = FilePredicate { unFilePredicate :: forall path. FileMeta path -> Bool }

applyPredicate :: FilePredicate -> FileMeta path -> Bool
applyPredicate (FilePredicate f) item = f item

allFiles :: FilePredicate
allFiles = FilePredicate (const True)

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
  = UploadError
  | DownloadError
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

-- We use this class to relativise the paths in different structures.
class HasPath e where
    mapPath :: (forall t. Path b t -> Path b' t) -> e b -> e b'
    getParent :: e Abs -> Path Abs Dir

instance HasPath FileCacheEntry where
    mapPath f (FileCacheEntry item chunks) = FileCacheEntry (item {filePath = f (filePath item)}) chunks
    getParent (FileCacheEntry item _)      = parent (filePath item)

instance HasPath TreeEntry where
    mapPath f (FileEntry item chunks) = FileEntry (item {filePath = f (filePath item)}) chunks
    mapPath f (DirEntry item)         = DirEntry  (item {filePath = f (filePath item)})
    mapPath f (LinkEntry item target) = LinkEntry (item {filePath = f (filePath item)}) target
    getParent (FileEntry item _)      = parent (filePath item)
    getParent (DirEntry item)         = parent (filePath item)
    getParent (LinkEntry item _)      = parent (filePath item)
