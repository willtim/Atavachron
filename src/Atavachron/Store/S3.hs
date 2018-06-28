{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}

-- | An S3 Store implementation for Atavachron.
--

module Atavachron.Store.S3
    ( newS3Store
    , parseS3URL
    ) where

import           Control.Arrow
import           Control.Lens
import           Control.Monad
import           Control.Monad.Catch
import           Control.Monad.IO.Class
import           Control.Monad.Trans.AWS
import           Data.Attoparsec.Text
import qualified Data.ByteString.Lazy    as LB
import           Data.Conduit
import qualified Data.Conduit.Binary     as CB
import qualified Data.Conduit.List       as CL
import           Data.Maybe
import           Data.Monoid
import           Data.Text               (Text)
import qualified Data.Text               as T

import           Network.AWS.Data
import           Network.AWS.S3
import           Network.HTTP.Types.Status
-- import           System.IO

import           Streaming (Stream, Of(..){-, hoist-})
import qualified Streaming.Prelude       as S

import           Atavachron.Store (Store(..))
import qualified Atavachron.Store as Store


newS3Store :: Text -> Region -> BucketName -> Store
newS3Store name region bucketName = Store{..}
  where

    list :: Store.Path -> Stream (Of Store.Key) IO ()
    list path@(Store.Path prefix) = do
        keys <- liftIO $ listObjects' region bucketName prefix
        S.each $ catMaybes $ map (fromObjectKey path) keys

    get :: Store.Key -> IO LB.ByteString
    get key = getObject' region bucketName (toObjectKey key)

    put :: Store.Key -> LB.ByteString -> IO ()
    put key bs = putObject' region bucketName (toObjectKey key) bs

    hasKey :: Store.Key -> IO Bool
    hasKey key = doesObjectExist region bucketName (toObjectKey key)

    toObjectKey :: Store.Key -> ObjectKey
    toObjectKey (Store.Key (Store.Path prefix) k) =
        ObjectKey $ prefix <> "/" <> k

    fromObjectKey :: Store.Path -> ObjectKey -> Maybe Store.Key
    fromObjectKey path@(Store.Path prefix) key =
        Store.Key path <$> T.stripPrefix (prefix <> "/") (view _ObjectKey key)


parseS3URL :: Text -> Maybe (Region, BucketName)
parseS3URL url = do
    let (host, bucketName) = second (BucketName . T.drop 1) $ T.breakOn "/" url
    [regionT, "amazonaws", "com"] <- return $  T.splitOn "." host
    region <- T.stripPrefix "s3-" regionT
                  >>= either (const Nothing) Just . parseOnly parser
    return (region, bucketName)


-- NOTE: currently only used for listing small numbers of objects.
listObjects'
    :: Region     -- ^ Region to operate in.
    -> BucketName -- ^ The bucket to query.
    -> Text       -- ^ Limit the response to keys with this prefix.
    -> IO [ObjectKey]
listObjects' r b p = do
--    lgr <- newLogger Error stdout
    env <- newEnv Discover <&> {-set envLogger lgr .-} set envRegion r
    runResourceT . runAWST env $
        paginate (set loPrefix (Just p) $ listObjects b)
                =$= CL.concatMap (map (view oKey) . view lorsContents)
                 $$ CL.consume
  -- where
  --   toStreaming :: Source m a -> Stream (Of a) m ()
  --   toStreaming src = hoist lift src $$ CL.mapM_ S.yield


getObject'
    :: Region     -- ^ Region to operate in.
    -> BucketName
    -> ObjectKey  -- ^ The source object key.
    -> IO LB.ByteString
getObject' r b k = do
--    lgr <- newLogger Error stdout
    env <- newEnv Discover <&> {-set envLogger lgr .-} set envRegion r
    runResourceT . runAWST env $ do
        rs <- send (getObject b k)
        view gorsBody rs `sinkBody` CB.sinkLbs


putObject'
    :: Region        -- ^ Region to operate in.
    -> BucketName    -- ^ The bucket to store the file in.
    -> ObjectKey     -- ^ The destination object key.
    -> LB.ByteString -- ^ The bytes to upload.
    -> IO ()
putObject' r b k bytes = do
--    lgr <- newLogger Error stdout
    env <- newEnv Discover <&> {-set envLogger lgr .-} set envRegion r
    runResourceT . runAWST env $
        void . send $ putObject b k (toBody bytes)


doesObjectExist
    :: Region
    -> BucketName
    -> ObjectKey
    -> IO Bool
doesObjectExist r b k = do
    env <- newEnv Discover <&> set envRegion r
    runResourceT . runAWST env $ do
        catch (((==200) . view horsResponseStatus) <$> send (headObject b k))
              (\ex@(ServiceError (s :: ServiceError)) ->
                   if view serviceStatus s == status404
                       then return False
                       else throwM ex)
