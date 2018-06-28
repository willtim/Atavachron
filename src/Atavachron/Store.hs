{-# LANGUAGE OverloadedStrings #-}

-- | An abstract store interface, for potentially providing multiple
-- remote repository implementations.
-- This module should be imported qualified.

module Atavachron.Store where

import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.ByteString.Lazy as LB

import Atavachron.Streaming

-- | A path used to qualify keys
newtype Path = Path { unPath :: Text }
    deriving (Eq, Ord)

instance Show Path where
    show = T.unpack . unPath

-- | A qualified key
data Key = Key
  { kPath :: !Path
  , kName :: !Text
  } deriving (Eq, Ord, Show)

-- | An external, likely remote, store.
-- NOTE: get and put are restricted to objects less than 10MB.
data Store = Store
  { name   :: Text -- typically a URL
  , list   :: Path -> Stream' Key IO ()
  , get    :: Key  -> IO LB.ByteString
  , put    :: Key  -> LB.ByteString -> IO ()
  , hasKey :: Key  -> IO Bool
  --  , remove :: Key -> IO ()
  }
