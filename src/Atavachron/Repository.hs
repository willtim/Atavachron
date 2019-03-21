{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

-- | A layer on top of the Store abstraction which provides functions
-- for putting and getting objects of specific types, e.g. Chunks and
-- Shapshots.

module Atavachron.Repository
    ( -- * Types
      Repository(..)
    , Manifest(..)
    , Snapshot(..)
    , SnapshotName
    , ChunkList(..)
    , AccessKeyName
    , AccessKey(..)
    , CachedCredentials
    , ManifestKey
      -- * Functions
    , authenticate
    , authenticate'
    , deleteGarbageChunk
    , deleteSnapshot
    , doesSnapshotExist
    , garbageCollectChunk
    , getAccessKey
    , getChunk
    , getGarbageModTime
    , getSnapshot
    , hasGarbageChunk
    , initRepository
    , listAccessKeys
    , listChunks
    , listGarbageChunks
    , listSnapshots
    , listSnapshotsForSource
    , newAccessKey
    , newManifest
    , putAccessKey
    , putChunk
    , putProgramBinary
    , putSnapshot
    , restoreGarbageChunk
    ) where

import Codec.Serialise
import Codec.Serialise.Decoding
import Codec.Serialise.Encoding
import qualified Codec.Compression.BZip as BZip

import Control.Exception
import Control.Logging
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.Resource

import qualified Crypto.Scrypt as Scrypt
import qualified Crypto.Saltine.Class as Saltine
import qualified Crypto.Saltine.Core.SecretBox as SecretBox
import qualified Crypto.Saltine.Internal.ByteSizes as ByteSizes

import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as LB

import Data.Maybe
import Data.Text (Text)
import qualified Data.Text as T
import Data.Text.Encoding (encodeUtf8)
import Data.Time (UTCTime)
import Data.Sequence (Seq)

import Streaming.Prelude (Of(..))
import qualified Streaming.Prelude as S

import System.Posix.Types
import System.Environment (getExecutablePath)

import GHC.Generics (Generic)

import Atavachron.Chunk.CDC
import Atavachron.Chunk.Encode
import Atavachron.Path
import Atavachron.Streaming (Stream')
import Atavachron.Store (Store, Key)
import qualified Atavachron.Store as Store


-- | Represents a remote repository used to backup files to.
-- A repository must be initialised before use.
data Repository = Repository
  { repoURL         :: !Text        -- ^ The repository URL
  , repoStore       :: !Store.Store -- ^ The Store implementation to use.
  , repoManifestKey :: !ManifestKey -- ^ The master key used to decrypt the manifest.
  , repoManifest    :: !Manifest    -- ^ The metadata needed for encoding/decoding chunks.
  }

-- | A repository manifest file contains the keys and CDC parameters
-- needed to both decode existing snapshots and create new ones with
-- de-duplication. A manifest is associated with one remote URL and
-- stored at "/manifest". It is encrypted using a named master key
-- stored in "/keys" which is itself encrypted using a password.
data Manifest = Manifest
  { mVersion    :: !Int -- expected to be "1" currently, we only
                        -- intend to bump this up if we ever break
                        -- backwards compatibility.
  , mChunkKey   :: !ChunkKey
  , mStoreIDKey :: !StoreIDKey
  , mCDCKey     :: !CDCKey
  , mCDCParams  :: !CDCParams
  } deriving Generic

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
    , sExeBinary  :: !(Maybe StoreID)
    } deriving (Generic, Show)

-- NOTE: Rather than create new Haskell types for every Snapshot version, we choose instead to
-- create a custom decoder that will handle older versions. This is because we are only likely
-- to add fields in practice and this way there are less types to deal with.
instance Serialise Snapshot where
   encode Snapshot{..} =
       encodeListLen 10   <> -- 10 fields
       encodeWord 1       <> -- Version 1, instead of 0
       encode sUserName   <>
       encode sHostName   <>
       encode sHostDir    <>
       encode sUID        <>
       encode sGID        <>
       encode sStartTime  <>
       encode sFinishTime <>
       encode sTree       <>
       encode sExeBinary
   decode = do
      len <- decodeListLen
      tag <- decodeWord
      case (len, tag) of
          -- version 0 could not reference the binary used for backup
          (9,  0) -> Snapshot <$> decode <*> decode <*> decode <*>
                                  decode <*> decode <*> decode <*>
                                  decode <*> decode <*> pure Nothing
          (10, 1) -> Snapshot <$> decode <*> decode <*> decode <*>
                                  decode <*> decode <*> decode <*>
                                  decode <*> decode <*> decode
          _ -> fail $ "Invalid Snapshot encoding: " ++ show (len, tag)

type SnapshotName = Text

-- | An ordered list of chunks and associated offsets.
-- Note: an empty sequence implies the previous seen chunk
data ChunkList = ChunkList
    { clChunks :: !(Seq StoreID)
    , clOffset :: !Offset -- offset in first chunk
    }
  deriving (Generic, Eq, Show)

instance Serialise ChunkList

instance Serialise CUid where
    encode (CUid w) = encode w
    decode = CUid <$> decode

instance Serialise CGid where
    encode (CGid w) = encode w
    decode = CGid <$> decode

instance Serialise CMode where
    encode (CMode w) = encode w
    decode = CMode <$> decode


newtype ManifestKey = ManifestKey { unManifestKey :: SecretBox.Key }
    deriving Generic

instance Serialise ManifestKey

data AccessKey = AccessKey
  { akSalt        :: !Scrypt.Salt
  , akManifestKey :: !CipherText
  } deriving Generic

instance Serialise AccessKey

instance Serialise Scrypt.Salt where
    encode = encode . Scrypt.getSalt
    decode = Scrypt.Salt <$> decode

type AccessKeyName = Text

data CachedCredentials = CachedCredentials
  { ccPasswordHash  :: !SecretBox.Key
  , ccAccessKeyName :: !AccessKeyName
  } deriving Generic

instance Serialise CachedCredentials

data SnapshotNotFound = SnapshotNotFound SnapshotName
    deriving Show
instance Exception SnapshotNotFound

data AmbigiousSnapshotKey = AmbigiousSnapshotKey SnapshotName
    deriving Show
instance Exception AmbigiousSnapshotKey

data MissingObject = MissingObject Key
    deriving Show
instance Exception MissingObject

data ManifestDecryptFailed = ManifestDecryptFailed
    deriving Show
instance Exception ManifestDecryptFailed

data SnapshotDecryptFailed = SnapshotDecryptFailed Key
    deriving Show
instance Exception SnapshotDecryptFailed

rootPath :: Store.Path
rootPath = Store.Path mempty

snapshotsPath :: Store.Path
snapshotsPath = Store.Path "snapshots"

chunksPath :: Store.Path
chunksPath = Store.Path "chunks"

garbagePath :: Store.Path
garbagePath = Store.Path "garbage"

keysPath :: Store.Path
keysPath = Store.Path "keys"

binariesPath :: Store.Path
binariesPath = Store.Path "bin"

manifestStoreKey :: Store.Key
manifestStoreKey = Store.Key rootPath "atavachron-manifest"

-- | Get the chunk associated with a key, if it exists.
-- NOTE:
-- 1) We first check that the key exists, so that we can return a known exception
-- in the case that the object is unexpectedly missing.
-- 2) If we cannot find the chunk in the chunks folder, we check the garbage folder.
getChunk :: Repository -> StoreID -> IO (Either SomeException CipherText)
getChunk Repository{..} storeID = do
    res <- get chunksPath
    case res of
        Left (fromException -> Just (MissingObject{})) -> get garbagePath
        _ -> return res
  where
    get path = do
        let key = mkChunkKey path storeID
        e'present <- try (Store.hasKey repoStore key)
        case e'present of
            Right True  -> (>>= deserialise') <$> try (Store.get repoStore key)
            Right False -> return (Left $ toException $ MissingObject key)
            Left ex     -> return $ Left ex

-- | Put the chunk under the supplied content-derived key, if no existing entry present.
-- Returns true if the chunk was already present in the repository.
-- NOTE: The last port of call for deduplication is here. The repository may have been
-- updated by another machine and so we first need to check if there is already an entry for the
-- supplied key before performing the write.
putChunk :: Repository -> StoreID -> CipherText -> IO (Either SomeException Bool)
putChunk Repository{..} storeID chunk = do
    let pickled = serialise chunk
        key     = mkChunkKey chunksPath storeID
    e'present <- try (Store.hasKey repoStore key)
    case e'present of
       Right True  -> return $ Right True -- entry already exists, no need to do anything more
       Right False -> fmap (const False) <$> try (Store.put repoStore key pickled)
       Left ex     -> return $ Left ex

-- | Chunks get stored under an additional folder which corresponds to the first
-- two characters of the hex-encoded Store ID. This helps avoid creating too
-- many entries in a single directory, which can cause problems for some filesystems.
mkChunkKey :: Store.Path -> StoreID -> Key
mkChunkKey parentPath storeID = Store.Key path name
  where
    name = hexEncode storeID
    path = Store.Path $ Store.unPath parentPath <> "/" <> T.take 2 name

-- | List all snapshots in the repo.
listSnapshots
    :: Repository
    -> Stream' (SnapshotName, Either SomeException Snapshot) (ResourceT IO) ()
listSnapshots repo = do
    let store  = repoStore repo
        keyStr = Store.list store snapshotsPath
    S.zip (S.map Store.kName keyStr)
        $ S.mapM (liftIO . getSnapshotByKey repo) keyStr

-- | List snapshots for a particular hostname and sourceDir.
-- TODO
-- we should offer the ability to filter by more than just
-- hostName and sourceDir, for example: user name or date/time.
listSnapshotsForSource
    :: Repository
    -> (Text, Path Abs Dir)
    -> Stream' (SnapshotName, Either SomeException Snapshot) (ResourceT IO) ()
listSnapshotsForSource repo (hostName, sourceDir) =
    S.filter snapshotsFilter $ listSnapshots repo
  where
    snapshotsFilter (_, Right s) =
        sHostName s == hostName &&
        sHostDir s  == sourceDir
    snapshotsFilter _ = True -- always want to report errors

-- | List all chunks in the repo!
-- Used by garbage collection and chunk existence check.
listChunks
    :: Repository
    -> Stream' StoreID (ResourceT IO) ()
listChunks repo = do
    let store = repoStore repo
    S.map (hexDecode . Store.kName)
        $ Store.list store chunksPath

-- | List all garbage chunks in the repo!
-- Used by garbage deletion and chunk existence check.
listGarbageChunks
    :: Repository
    -> Stream' StoreID (ResourceT IO) ()
listGarbageChunks repo = do
    let store = repoStore repo
    S.map (hexDecode . Store.kName)
        $ Store.list store garbagePath

-- | Retrieve a snapshot by a potentially partial key.
getSnapshot :: Repository -> SnapshotName -> IO (Either SomeException Snapshot)
getSnapshot repo partialKey = do
    -- for now we will just list all the snapshots and search them
    keys :> ()  <- runResourceT
                 . S.toList
                 . S.filter match
                 $ Store.list (repoStore repo) snapshotsPath
    case keys of
        [k] -> getSnapshotByKey repo k
        []  -> return . Left . toException $ SnapshotNotFound partialKey
        _   -> return . Left . toException $ AmbigiousSnapshotKey partialKey
  where
    match (Store.Key _ name) = partialKey `T.isPrefixOf` name

-- | Retrieve a snapshot by its fully-specified key.
getSnapshotByKey :: Repository -> Key -> IO (Either SomeException Snapshot)
getSnapshotByKey repo key = do
    debug' $ "Loading snapshot with key: " <> Store.kName key
    let Manifest{..} = repoManifest repo
    e'blob <- try (Store.get (repoStore repo) key)
    return $ e'blob >>= decrypt (unChunkKey mChunkKey)
                                (toException $ SnapshotDecryptFailed key)

-- | Query the store for the existence of a snapshot using a potentially partial key.
doesSnapshotExist :: Repository -> SnapshotName -> IO Bool
doesSnapshotExist repo partialKey = do
    -- for now we will just list all the snapshots and search them
    runResourceT
        . S.fold_ (\_ _ -> True) False id
        . S.filter match
        $ Store.list (repoStore repo) snapshotsPath
  where
    match (Store.Key _ name) = partialKey `T.isPrefixOf` name

-- | Put the snapshot in the repository and generate a key for it.
putSnapshot :: Repository -> Snapshot -> IO (Either SomeException SnapshotName)
putSnapshot Repository{..} snapshot = do
    let (key, pickled) = computeSnapshotKey repoManifest snapshot

    debug' $ "Writing snapshot with key: " <> Store.kName key

    res <- encryptBytes (unChunkKey $ mChunkKey repoManifest) pickled
               >>= try . Store.put repoStore key . serialise

    return $ const (Store.kName key) <$> res

-- | Permanently delete a snapshot. Used by the pruning command.
-- NOTE: This is a destructive update!
deleteSnapshot :: Repository -> Snapshot -> IO (Either SomeException ())
deleteSnapshot Repository{..} snapshot = do
    let key = fst $ computeSnapshotKey repoManifest snapshot
    debug' $ "Deleting snapshot: " <> Store.kName key
    try (Store.delete repoStore key)

-- | Does the supplied storeID exist in the garbage folder?
hasGarbageChunk :: Repository -> StoreID -> IO (Either SomeException Bool)
hasGarbageChunk Repository{..} storeID = do
    let key = mkChunkKey garbagePath storeID
    try (Store.hasKey repoStore key)

-- | When was this chunk marked as garbage?
getGarbageModTime :: Repository -> StoreID -> IO (Either SomeException UTCTime)
getGarbageModTime Repository{..} storeID = do
    let key = mkChunkKey garbagePath storeID
    try (Store.modTime repoStore key)

-- | Restore a chunk from the garbage folder to the chunks folder.
restoreGarbageChunk :: Repository -> StoreID -> IO (Either SomeException ())
restoreGarbageChunk Repository{..} storeID = do
    let src  = mkChunkKey garbagePath storeID
        dest = mkChunkKey chunksPath  storeID
    try (Store.move repoStore src dest)

-- | Move an object to the garbage folder.
-- NOTE: This is a destructive update!
garbageCollectChunk :: Repository -> StoreID -> IO (Either SomeException ())
garbageCollectChunk Repository{..} storeID = do
    let srcKey  = mkChunkKey chunksPath  storeID
        destKey = mkChunkKey garbagePath storeID
    debug' $ "Moving to garbage: " <> Store.kName srcKey
    try (Store.move repoStore srcKey destKey)

-- | Permanently delete the garbage chunk from the repository (!)
deleteGarbageChunk :: Repository -> StoreID -> IO (Either SomeException ())
deleteGarbageChunk Repository{..} storeID = do
    let key  = mkChunkKey garbagePath storeID
    debug' $ "Deleting garbage chunk: " <> Store.kName key
    try (Store.delete repoStore key)

-- | Compute the key for a snapshot using its contents.
computeSnapshotKey :: Manifest -> Snapshot -> (Store.Key, B.ByteString)
computeSnapshotKey Manifest{..} snapshot = (key, pickled)
  where
    pickled  = LB.toStrict $ serialise snapshot
    storeID  = hashBytes mStoreIDKey pickled
    key      = Store.Key snapshotsPath $ hexEncode storeID

-- | Put the program binary (Atavachron itself) in the repository and
-- generate a key for it. We compress using the BZip2 file format and
-- do not encrypt or chunk.
putProgramBinary :: Repository -> IO (Either SomeException StoreID)
putProgramBinary repo = do
    programPath <- getExecutablePath
    debug' $ "Storing executable " <> T.pack programPath
    binary      <- BZip.compress <$> LB.readFile programPath
    let Manifest{..} = repoManifest repo
        storeID  = hashBytes mStoreIDKey (LB.toStrict binary)
        key      = Store.Key binariesPath $ hexEncode storeID

    debug' $ "Writing Atavachron binary with key: " <> Store.kName key
    res <- try (Store.put (repoStore repo) key binary)
    return $ const storeID <$> res

-- | Initialise a repository using the supplied store and password.
initRepository :: Store -> Text -> IO CachedCredentials
initRepository store pass = do
    hasManifest <- Store.hasKey store manifestStoreKey
    when hasManifest $
        errorL' "URL already hosts a repository"

    manifest    <- newManifest
    manifestKey <- ManifestKey <$> SecretBox.newKey

    let pickled = LB.toStrict $ serialise manifest

    either (\(ex :: SomeException) -> errorL' $ "Failed to initialise repository: " <> T.pack (show ex)) id
        <$> (encryptBytes (unManifestKey manifestKey) pickled
               >>= try . Store.put store manifestStoreKey . serialise)
    newAccessKey store manifestKey "default" pass

-- | Create and store a new access key using the supplied name and password.
newAccessKey :: Store -> ManifestKey -> Text -> Text -> IO CachedCredentials
newAccessKey store manifestKey name pass = do
    salt <- Scrypt.newSalt
    let key  = keyFromPassword salt pass
    ciphertext <- encryptBytes key (Saltine.encode $ unManifestKey manifestKey)
    either (\(ex :: SomeException) -> errorL' $ "Could not write access key: " <> T.pack (show ex)) id
        <$> putAccessKey store name (AccessKey salt ciphertext)
    return $ CachedCredentials key name

-- | Get repository access using the supplied store and password.
authenticate :: Store -> Text -> IO (Repository, CachedCredentials)
authenticate store pass = do
    hasManifest <- Store.hasKey store manifestStoreKey
    unless hasManifest $
        errorL' $ "Could not find a repository at the supplied URL"

    -- for now, just try each key in succession
    let keyStr = listAccessKeys store
    (manifestKey, cc) <- fromMaybe (errorL' "Password does not match any stored!")
               <$> runResourceT (S.foldM_ tryPass (return Nothing) return keyStr)
    (,cc) <$> resolveRepository store manifestKey

  where
    tryPass
        :: Maybe (ManifestKey, CachedCredentials)
        -> (AccessKeyName, Either SomeException AccessKey)
        -> (ResourceT IO) (Maybe (ManifestKey, CachedCredentials))
    tryPass Nothing (name, Left ex) = do
        warn' $ "Could not retrieve key '" <> name <> "' : " <> T.pack (show ex)
        return Nothing
    tryPass Nothing (name, Right AccessKey{..}) = do
        debug' $ "Trying key: " <> name
        let key = keyFromPassword akSalt pass
            cc  = CachedCredentials key name
        return $ (,cc) . ManifestKey
             <$> (decryptBytes key akManifestKey >>= Saltine.decode)
    tryPass m _ = return m

-- | Get repository access using the supplied store and cached credentials.
authenticate' :: Store -> CachedCredentials -> IO Repository
authenticate' store CachedCredentials{..} = do
    hasManifest <- Store.hasKey store manifestStoreKey
    unless hasManifest $
        errorL' $ "Could not find a repository at the supplied URL"

    res      <- getAccessKey store ccAccessKeyName
    case res of
        Left ex -> errorL $ "Could not retrieve access key: " <> T.pack (show ex)
        Right AccessKey{..} -> do
            let manifestKey = maybe (errorL $ "Cached credentials do not match access key: " <> ccAccessKeyName)
                                    ManifestKey
                            $ (decryptBytes ccPasswordHash akManifestKey >>= Saltine.decode)
            resolveRepository store manifestKey

-- Loads and decodes the Manifest
resolveRepository :: Store -> ManifestKey -> IO Repository
resolveRepository store manifestKey = do
    manifest <- either (\ex -> errorL' $ "Cannot obtain manifest: " <> T.pack (show ex)) id
                     . (>>= decrypt (unManifestKey manifestKey) (toException ManifestDecryptFailed))
                   <$> try (Store.get store manifestStoreKey)
    return Repository
        { repoURL         = Store.name store
        , repoStore       = store
        , repoManifestKey = manifestKey
        , repoManifest    = manifest
        }

keyFromPassword :: Scrypt.Salt -> Text -> SecretBox.Key
keyFromPassword salt pass = key
  where
    key    = fromMaybe (errorL' "Failed to generate secret key from password hash")
           $ Saltine.decode $ Scrypt.getHash hash
    hash   = Scrypt.scrypt params salt (Scrypt.Pass $ encodeUtf8 pass)
    params = fromMaybe (errorL' "Failed to generate Scrypt parameters")
           $ Scrypt.scryptParamsLen 14 8 1 (fromIntegral $ ByteSizes.secretBoxKey)


-- | List the available access keys for accessing the repository.
listAccessKeys
    :: Store
    -> Stream' (AccessKeyName, Either SomeException AccessKey) (ResourceT IO) ()
listAccessKeys store = do
    let keyStr = S.map Store.kName $ Store.list store keysPath
    S.zip keyStr
        $ S.mapM (liftIO . getAccessKey store) keyStr

-- | Retrieve an AccessKey using the provided name.
getAccessKey :: Store -> AccessKeyName -> IO (Either SomeException AccessKey)
getAccessKey store name = (>>= deserialise') <$> try (Store.get store key)
  where
    key   = Store.Key keysPath name

-- | Store an AccessKey using the provided name, e.g. "default".
-- NOTE: allows overwriting an existing entry.
putAccessKey :: Store -> AccessKeyName -> AccessKey -> IO (Either SomeException ())
putAccessKey store name = try . Store.put store (Store.Key keysPath name) . serialise

deserialise' :: Serialise a => LB.ByteString -> Either SomeException a
deserialise' = mapLeft toException . deserialiseOrFail

mapLeft :: (a -> b) -> Either a c -> Either b c
mapLeft f = either (Left . f) Right

-- | Decryption helper used for snapshots and the manifest.
-- Supplied exception is used for when decryption fails.
decrypt :: Serialise a => SecretBox.Key -> SomeException -> LB.ByteString -> Either SomeException a
decrypt key ex = deserialise'
             >=> maybe (Left ex) Right . decryptBytes key
             >=> deserialise' . LB.fromStrict
