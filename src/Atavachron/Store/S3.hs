{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

{-# LANGUAGE FlexibleContexts #-}

-- | An S3 Store implementation for Atavachron.
--

module Atavachron.Store.S3
    ( newS3Store
    , parseS3URL
    ) where

import           Control.Arrow
import           Control.Logging
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
import           Network.HTTP.Conduit (newManager, tlsManagerSettings, responseBody)

import qualified Aws
import qualified Aws.Core as Aws
import qualified Aws.S3 as S3

import           System.Directory
import           System.FilePath

import           Streaming (Stream, Of(..))
import qualified Streaming.Prelude       as S

import           Atavachron.Store (Store(..))
import qualified Atavachron.Store as Store


type Config      = Aws.Configuration
type Endpoint    = S3.S3Configuration Aws.NormalQuery
type BucketName  = Text
type ObjectKey   = Text

newS3Store :: Text -> Endpoint -> BucketName -> IO Store
newS3Store name endpoint bucketName = do
    cfg <- baseConfiguration
    let params = (cfg, endpoint)

        list :: Store.Path -> Stream (Of Store.Key) (ResourceT IO) ()
        list path@(Store.Path prefix) =
            S.mapMaybe (fromObjectKey path) $ listObjects' params bucketName prefix

        get :: Store.Key -> IO LB.ByteString
        get key = getObject' params bucketName (toObjectKey key)

        put :: Store.Key -> LB.ByteString -> IO ()
        put key bs = putObject' params bucketName (toObjectKey key) bs

        hasKey :: Store.Key -> IO Bool
        hasKey key = doesObjectExist params bucketName (toObjectKey key)

        toObjectKey :: Store.Key -> ObjectKey
        toObjectKey (Store.Key (Store.Path prefix) k) =
            prefix <> "/" <> k

        fromObjectKey :: Store.Path -> ObjectKey -> Maybe Store.Key
        fromObjectKey path@(Store.Path prefix) key =
            Store.Key path <$> T.stripPrefix (prefix <> "/") key

    return Store{..}


parseS3URL :: Text -> Maybe (Endpoint, BucketName)
parseS3URL url = do
    let (host, bucketName) = second (T.drop 1) $ T.breakOn "/" url
        endpoint = S3.s3v4 Aws.HTTPS (T.encodeUtf8 host) False S3.AlwaysUnsigned -- NB: assumes UTF8 (!)
        --endpoint = S3.s3 Aws.HTTPS (T.encodeUtf8 host) True -- NB: assumes UTF8 (!)
    return (endpoint { S3.s3RequestStyle = S3.PathStyle }, bucketName)

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
                      S3.HeadObjectResponse (Just{}) -> return True -- 200 OK
                      _ -> return False })
            (\() -> return False)
  where
    selector (HttpExceptionRequest _ (StatusCodeException res _))
        | statusCode (responseStatus res) == 404 = Just () -- do we need this check?
    selector _ = Nothing


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
            errorL $ "Could not parse aws credentials ini file: " <> T.pack err
        Right (keyID, secret) ->
            Aws.makeCredentials (T.encodeUtf8 keyID) (T.encodeUtf8 secret)
  where
    extract ini =
        (,) <$> INI.lookupValue profile "aws_access_key_id" ini
            <*> INI.lookupValue profile "aws_secret_access_key" ini
