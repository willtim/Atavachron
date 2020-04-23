{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}

{-# LANGUAGE FlexibleContexts #-}

-- | An S3 Store implementation for Atavachron.
--

module Atavachron.Store.S3
    ( newS3Store
    , parseS3URL
    ) where

import           Control.Arrow
import           Control.Monad.Catch
import           Control.Monad.IO.Class
import           Control.Monad.Trans.Resource
import           Control.Monad.Trans

import qualified Data.ByteString.Lazy    as LB
import           Data.Conduit
import qualified Data.Conduit.Binary     as CB
import qualified Data.Conduit.List       as CL
import qualified Data.Ini                as INI
import           Data.Text               (Text)
import qualified Data.Text               as T
import qualified Data.Text.Encoding      as T

import           Network.HTTP.Client
import           Network.HTTP.Types.Status
import           Network.HTTP.Conduit (tlsManagerSettings)

import qualified Aws
import qualified Aws.Core as Aws
import qualified Aws.S3 as S3

import           Data.Time
import           System.Directory
import           System.FilePath

import           Streaming (Stream, Of(..))
import qualified Streaming.Prelude       as S

import           Atavachron.Logging
import           Atavachron.Store (Store(..))
import qualified Atavachron.Store as Store


type Config      = Aws.Configuration
type Endpoint    = S3.S3Configuration Aws.NormalQuery
type Host        = Text
type BucketName  = Text
type ObjectKey   = Text

newS3Store :: (Host, Endpoint, BucketName) -> IO Store
newS3Store (host, endpoint, bucketName) = do

    cfg <- baseConfiguration

    let name = "s3://" <> host <> "/" <> bucketName

        params = (cfg, endpoint)

        list :: Store.Path -> Stream (Of Store.Key) (ResourceT IO) ()
        list (Store.Path prefix) =
            S.map fromObjectKey $ listObjects' params bucketName prefix

        get :: Store.Key -> IO LB.ByteString
        get key = getObject' params bucketName (toObjectKey key)

        put :: Store.Key -> LB.ByteString -> IO ()
        put key = putObject' params bucketName (toObjectKey key)

        hasKey :: Store.Key -> IO Bool
        hasKey key = doesObjectExist params bucketName (toObjectKey key)

        move :: Store.Key -> Store.Key -> IO ()
        move src dest = moveObject params bucketName (toObjectKey src) (toObjectKey dest)

        delete :: [Store.Key] -> IO ()
        delete keys = deleteObjects params bucketName (map toObjectKey keys)

        modTime :: Store.Key -> IO UTCTime
        modTime key = objectModTime params bucketName (toObjectKey key)

        toObjectKey :: Store.Key -> ObjectKey
        toObjectKey (Store.Key (Store.Path prefix) k) =
            prefix <> "/" <> k

        fromObjectKey :: ObjectKey -> Store.Key
        fromObjectKey key =
            let (path, key') = first (T.dropEnd 1) $ T.breakOnEnd "/" key
            in Store.Key (Store.Path path) key'

    return Store{..}

parseS3URL :: Text -> Maybe (Host, Endpoint, BucketName)
parseS3URL url = do
    let (host, bucketName) = second (T.drop 1) $ T.breakOn "/" url
        endpoint = S3.s3v4 Aws.HTTPS (T.encodeUtf8 host) False S3.AlwaysUnsigned -- NB: assumes UTF8 (!)
        --endpoint = S3.s3 Aws.HTTPS (T.encodeUtf8 host) True -- NB: assumes UTF8 (!)
    return (host, endpoint { S3.s3RequestStyle = S3.PathStyle }, bucketName)

listObjects'
    :: (Config, Endpoint)
    -> BucketName -- ^ The bucket to query.
    -> Text       -- ^ Limit the response to keys with this prefix.
    -> Stream (Of ObjectKey) (ResourceT IO) ()
listObjects' (cfg, s3cfg) b p = do
    mgr <- liftIO $ newManager tlsManagerSettings
    let src = Aws.awsIteratedSource cfg s3cfg mgr $
                  (S3.getBucket b) { S3.gbPrefix = Just p }
    S.map S3.objectKey
      . S.concat
      . S.mapM (fmap S3.gbrContents . Aws.readResponseIO)
      . toStream
      $ src
  where
     toStream :: Monad m => ConduitT () a m () -> Stream (Of a) m ()
     toStream c = runConduit (transPipe lift c .| CL.mapM_ S.yield)

getObject'
    :: (Config, Endpoint)
    -> BucketName
    -> ObjectKey  -- ^ The source object key.
    -> IO LB.ByteString
getObject' (cfg, s3cfg) b k = do
    mgr <- newManager tlsManagerSettings
    runResourceT $ do
        S3.GetObjectResponse { S3.gorResponse = rsp } <-
            Aws.pureAws cfg s3cfg mgr $
                S3.getObject b k
        runConduit $ responseBody rsp .| CB.sinkLbs

putObject'
    :: (Config, Endpoint)
    -> BucketName    -- ^ The bucket to store the file in.
    -> ObjectKey     -- ^ The destination object key.
    -> LB.ByteString -- ^ The bytes to upload.
    -> IO ()
putObject' (cfg, s3cfg) b k bytes = do
    mgr <- newManager tlsManagerSettings
    let body = RequestBodyLBS bytes
    runResourceT $ do
        S3.PutObjectResponse {} <-
            Aws.pureAws cfg s3cfg mgr $
                S3.putObject b k body
        return ()

-- NOTE: Move has to be implemented as a copy followed by a delete.
moveObject
    :: (Config, Endpoint)
    -> BucketName    -- ^ The bucket to store the file in.
    -> ObjectKey     -- ^ The source object key.
    -> ObjectKey     -- ^ The destination object key.
    -> IO ()
moveObject (cfg, s3cfg) b k1 k2 = do
    mgr <- newManager tlsManagerSettings
    runResourceT $ do
        S3.CopyObjectResponse {} <-
            Aws.pureAws cfg s3cfg mgr $
                let src = S3.ObjectId {oidBucket=b, oidObject=k1, oidVersion=Nothing}
                in S3.copyObject b k2 src S3.CopyMetadata
        S3.DeleteObjectResponse {} <-
            Aws.pureAws cfg s3cfg mgr $
                S3.DeleteObject {doObjectName=k1,doBucket=b}
        return ()

deleteObjects
    :: (Config, Endpoint)
    -> BucketName    -- ^ The bucket to store the file in.
    -> [ObjectKey]   -- ^ The objects to delete.
    -> IO ()
deleteObjects (cfg, s3cfg) b ks = do
    mgr <- newManager tlsManagerSettings
    runResourceT $ do
        S3.DeleteObjectsResponse {} <-
            Aws.pureAws cfg s3cfg mgr $
                S3.DeleteObjects { dosObjects=zip ks (repeat Nothing)
                                 , dosBucket=b
                                 , dosQuiet   = True
                                 , dosMultiFactorAuthentication = Nothing}
        return ()

doesObjectExist
    :: (Config, Endpoint)
    -> BucketName
    -> ObjectKey
    -> IO Bool
doesObjectExist (cfg, s3cfg) b k = do
    mgr <- newManager tlsManagerSettings
    runResourceT $
        catchJust selector
            (do { res <- Aws.pureAws cfg s3cfg mgr $ S3.headObject b k
                ; case res of
                      S3.HeadObjectResponse Just{} -> return True -- 200 OK
                      _ -> return False })
            (\() -> return False)
  where
    selector (HttpExceptionRequest _ (StatusCodeException res _))
        | statusCode (responseStatus res) == 404 = Just () -- do we need this check?
    selector _ = Nothing

objectModTime
    :: (Config, Endpoint)
    -> BucketName
    -> ObjectKey
    -> IO UTCTime
objectModTime (cfg, s3cfg) b k = do
    mgr <- newManager tlsManagerSettings
    runResourceT $ do
        S3.HeadObjectResponse (Just meta) <-
            Aws.pureAws cfg s3cfg mgr $ S3.headObject b k
        return $ S3.omLastModified meta


-- | The default configuration, with credentials loaded from
-- environment variable or configuration file (see
-- 'loadCredentialsDefault' below).
baseConfiguration :: MonadIO m => m Config
baseConfiguration = liftIO $ do
  cr <- loadCredentialsDefault
  case cr of
    Nothing  -> throwM $ Aws.NoCredentialsException "Could not locate aws credentials"
    Just cr' -> return Aws.Configuration
                { timeInfo = Aws.Timestamp
                , credentials = cr'
                , logger = Aws.defaultLog Aws.Warning
                , proxy = Nothing
                }

-- | Load credentials from environment variables if possible, or
-- alternatively from a default file with the default key name.
--
-- Default file (used by awscli): /<user directory>/.aws/credentials@
-- Default key name: @default@
--
-- TODO configure a credentials file and key?
loadCredentialsDefault :: IO (Maybe Aws.Credentials)
loadCredentialsDefault = do
  homeDir <- getHomeDirectory
  let file = homeDir </> ".aws" </> "credentials"
  doesExist <- doesFileExist file
  if doesExist
     then Just <$> loadCredentialsFromAwsConfig file "default"
     else Aws.loadCredentialsFromEnv

-- | Load AWS credentials from awscli INI file.
loadCredentialsFromAwsConfig :: FilePath -> Text -> IO Aws.Credentials
loadCredentialsFromAwsConfig file profile = do
    res <- INI.readIniFile file
    case res >>= extract of
        Left err ->
            panic $ "Could not parse aws credentials ini file: " <> T.pack err
        Right (keyID, secret) ->
            Aws.makeCredentials (T.encodeUtf8 keyID) (T.encodeUtf8 secret)
  where
    extract ini =
        (,) <$> INI.lookupValue profile "aws_access_key_id" ini
            <*> INI.lookupValue profile "aws_secret_access_key" ini
