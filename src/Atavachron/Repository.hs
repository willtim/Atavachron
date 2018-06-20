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
    , ManifestKey(..)
    , AccessKeyName
    , AccessKey(..)
    , CachedCredentials
      -- * Functions
    , newManifest
    , putChunk
    , getChunk
    , listSnapshots
    , getSnapshot
    , putSnapshot
    , initRepository
    , authenticate
    , authenticate'
    , listAccessKeys
    , getAccessKey
    , putAccessKey
    ) where

import Codec.Serialise

import Control.Exception
import Control.Monad
import Control.Logging

import qualified Crypto.Scrypt as Scrypt
import qualified Crypto.Saltine.Class as Saltine
import qualified Crypto.Saltine.Core.SecretBox as SecretBox
import qualified Crypto.Saltine.Internal.ByteSizes as ByteSizes

import qualified Data.ByteString.Lazy as LB

import Data.Maybe
import Data.Monoid
import Data.Text (Text)
import qualified Data.Text as T
import Data.Text.Encoding (encodeUtf8)
import Data.Time (UTCTime)
import Data.Sequence (Seq)

import Streaming.Prelude (Of(..))
import qualified Streaming.Prelude as S

import System.Posix.Types

import GHC.Generics (Generic)

import Atavachron.Chunk.CDC
import Atavachron.Chunk.Encode
import Atavachron.Path
import Atavachron.Streaming
import Atavachron.Store (Store, Key)
import qualified Atavachron.Store as Store
import qualified Atavachron.Store.LocalFS as Store
import qualified Atavachron.Store.S3 as Store


-- | Represents a remote repository used to backup files to.
-- A repository must be initialised before use.
data Repository = Repository
  { repoURL      :: !Text        -- ^ The repository URL
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
    } deriving (Generic, Show)

instance Serialise Snapshot

type SnapshotName = Text

-- | An ordered list of chunks and associated offsets.
-- Note: an empty sequence implies the previous seen chunk
data ChunkList = ChunkList
    { clChunks :: !(Seq StoreID)
    , clOffset :: !Offset -- offset in first chunk
    }
  deriving (Generic, Eq, Show, Read)

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

keysPath :: Store.Path
keysPath = Store.Path "keys"

manifestStoreKey :: Store.Key
manifestStoreKey = Store.Key rootPath "manifest"

-- | Get the chunk associated with a key, if it exists.
-- NOTE: We first check that the key exists, so that we can return a known exception
-- in the case that the object is unexpectedly missing.
getChunk :: Repository -> StoreID -> IO (Either SomeException CipherText)
getChunk Repository{..} storeID = do
    let key     = mkChunkKey storeID
    e'present <- try (Store.hasKey repoStore key)
    case e'present of
       Right True  -> (>>= deserialise') <$> try (Store.get repoStore key)
       Right False -> return (Left $ toException $ MissingObject key)
       Left ex     -> return $ Left ex

 -- | Put the chunk under the supplied content-derived key, if no existing entry present.
-- NOTE: The last port of call for deduplication is here. The repository may have been
-- updated by another machine and so we first need to check if there is already an entry for the
-- supplied key before performing the write.
putChunk :: Repository -> StoreID -> CipherText -> IO (Either SomeException ())
putChunk Repository{..} storeID chunk = do
    let pickled = serialise chunk
        key     = mkChunkKey storeID
    e'present <- try (Store.hasKey repoStore key)
    case e'present of
       Right True  -> return $ Right () -- entry already exists, no need to do anything more
       Right False -> try (Store.put repoStore key pickled)
       Left ex     -> return $ Left ex

-- Chunks get stored under an additional folder which corresponds to the first
-- two characters of the hex-encoded Store ID. This helps avoid creating too
-- many entries in a single directory, which can cause problems for some filesystems.
mkChunkKey :: StoreID -> Key
mkChunkKey storeID = Store.Key path name
  where
    name = hexEncode storeID
    path = Store.Path $ Store.unPath chunksPath <> "/" <> T.take 2 name

listSnapshots :: Repository -> Stream' (SnapshotName, Either SomeException Snapshot) IO ()
listSnapshots repo = do
    let store  = repoStore repo
        keyStr = Store.list store snapshotsPath
    S.zip (S.map Store.kName keyStr) $ S.mapM (getSnapshotByKey repo) keyStr

-- | Retrieve a snapshot by a potentially partial key.
getSnapshot :: Repository -> SnapshotName -> IO (Either SomeException Snapshot)
getSnapshot repo partialKey = do
    -- for now we will just list all the snapshots and search them
    keys :> ()  <- S.toList
                 $ S.filter match
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
    let Manifest{..} = repoManifest repo
    e'blob <- try (Store.get (repoStore repo) key)
    return $ e'blob >>= decrypt (unChunkKey mChunkKey)
                                (toException $ SnapshotDecryptFailed key)

-- | Put the snapshot in the repository and generate a key for it.
putSnapshot :: Repository -> Snapshot -> IO (Either SomeException SnapshotName)
putSnapshot repo snapshot = do
    let Manifest{..} = repoManifest repo
        pickled  = LB.toStrict $ serialise snapshot
        storeID  = hashBytes mStoreIDKey pickled
        key      = Store.Key snapshotsPath $ hexEncode storeID

    res <- encryptBytes (unChunkKey mChunkKey) pickled
               >>= try . Store.put (repoStore repo) key . serialise

    return $ const (Store.kName key) <$> res

-- | Initialise a repository using the supplied URL and password.
initRepository :: Text -> Text -> IO CachedCredentials
initRepository repoURL pass = do
    store       <- either (errorL' . ("Cannot parse URL: "<>)) id <$> parseURL repoURL
    hasManifest <- Store.hasKey store manifestStoreKey
    when hasManifest $
        errorL' "URL already hosts a repository"

    manifest    <- newManifest
    manifestKey <- ManifestKey <$> SecretBox.newKey

    let pickled = LB.toStrict $ serialise manifest

    either (\(ex :: SomeException) -> errorL' $ "Failed to initialise repository: " <> T.pack (show ex)) id
        <$> (encryptBytes (unManifestKey manifestKey) pickled
               >>= try . Store.put store manifestStoreKey . serialise)

    salt <- Scrypt.newSalt
    let key  = keyFromPassword salt pass
        name = "default"
    ciphertext <- encryptBytes key (Saltine.encode $ unManifestKey manifestKey)

    either (\(ex :: SomeException) -> errorL' $ "Could not write access key: " <> T.pack (show ex)) id
        <$> putAccessKey store name (AccessKey salt ciphertext)

    return $ CachedCredentials key name

-- | Get repository access using the supplied password.
authenticate :: Text -> Text -> IO (Repository, CachedCredentials)
authenticate  repoURL pass = do
    store    <- either (errorL' . ("Cannot parse URL: "<>)) id
                   <$> parseURL repoURL
    -- for now, just try each key in succession
    let keyStr = listAccessKeys store
    (manifestKey, cc) <- fromMaybe (errorL' "Password does not match any stored!")
               <$> S.foldM_ tryPass (return Nothing) return keyStr
    (,cc) <$> resolveRepository repoURL store manifestKey

  where
    tryPass :: Maybe (ManifestKey, CachedCredentials)
            -> (AccessKeyName, Either SomeException AccessKey)
            -> IO (Maybe (ManifestKey, CachedCredentials))
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

-- | Get repository access using the supplied cached credentials.
authenticate' :: Text -> CachedCredentials -> IO Repository
authenticate' repoURL CachedCredentials{..} = do
    store    <- either (errorL' . ("Cannot parse URL: "<>)) id
                   <$> parseURL repoURL
    res      <- getAccessKey store ccAccessKeyName
    case res of
        Left ex -> errorL $ "Could not retrieve access key: " <> T.pack (show ex)
        Right AccessKey{..} -> do
            let manifestKey = maybe (errorL $ "Cached credentials do not match access key: " <> ccAccessKeyName)
                                    ManifestKey
                            $ (decryptBytes ccPasswordHash akManifestKey >>= Saltine.decode)
            resolveRepository repoURL store manifestKey

-- Loads and decodes the Manifest
resolveRepository :: Text -> Store -> ManifestKey -> IO Repository
resolveRepository repoURL store manifestKey = do
    manifest <- either (\ex -> errorL' $ "Cannot obtain manifest: " <> T.pack (show ex)) id
                     . (>>= decrypt (unManifestKey manifestKey) (toException ManifestDecryptFailed))
                   <$> try (Store.get store manifestStoreKey)
    return Repository
        { repoURL      = repoURL
        , repoStore    = store
        , repoManifest = manifest
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
listAccessKeys :: Store -> Stream' (AccessKeyName, Either SomeException AccessKey) IO ()
listAccessKeys store = do
    let keyStr = S.map Store.kName $ Store.list store keysPath
    S.zip keyStr $ S.mapM (getAccessKey store) keyStr

-- | Retrieve an AccessKey using the provided name.
getAccessKey :: Store -> AccessKeyName -> IO (Either SomeException AccessKey)
getAccessKey store name = (>>= deserialise') <$> try (Store.get store key)
  where
    key   = Store.Key keysPath name

-- | Store an AccessKey using the provided name, e.g. "default".
-- NOTE: allows overwriting an existing entry.
putAccessKey :: Store -> AccessKeyName -> AccessKey -> IO (Either SomeException ())
putAccessKey store name = try . Store.put store (Store.Key keysPath name) . serialise

-- TODO
parseURL :: Text -> IO (Either Text Store)
parseURL repoDir =
    case ":" `T.breakOn` repoDir of
        ("file", T.unpack -> _:rest) -> do
            m'path <- return $ parseAbsDir rest
            return $ case m'path of
                Nothing   -> Left $ "Cannot parse file URL: " <> repoDir
                Just path -> Right $ Store.newLocalFS path
        ("s3", T.drop 1 -> rest)   ->
            return $ case Store.parseS3URL rest of
                Nothing   -> Left $ "Cannot parse S3 URL: " <> repoDir
                Just (region, bucketName) -> Right $ Store.newS3Store region bucketName


        _ -> return $ Left $ "Cannot parse URL: " <> repoDir

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
