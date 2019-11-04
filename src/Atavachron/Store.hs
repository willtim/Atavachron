{-# LANGUAGE OverloadedStrings #-}

-- | An abstract store interface, for potentially providing multiple
-- remote repository implementations.
-- This module should be imported qualified.

module Atavachron.Store where

import Control.Monad.Trans.Resource

import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.ByteString.Lazy as LB

import Atavachron.Streaming
import Data.Time

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
  { -- typically a URL
    name    :: Text
    -- must list *all* keys recursively under the path
  , list    :: Path -> Stream' Key (ResourceT IO) ()
  , get     :: Key -> IO LB.ByteString
  , put     :: Key -> LB.ByteString -> IO ()
  , hasKey  :: Key -> IO Bool
    -- used by garbage collection
  , move    :: Key -> Key -> IO ()
    -- used by prune and garbage deletion
  , delete  :: [Key] -> IO ()
  , modTime :: Key -> IO UTCTime
  }
