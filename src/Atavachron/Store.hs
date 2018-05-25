-- | An abstract store interface, for potentially providing multiple
-- remote repository implementations.

module Atavachron.Store where

import Data.Text (Text)
import qualified Data.ByteString.Lazy as LB

import Atavachron.Streaming

-- | A path used to qualify keys
newtype Path = Path { unPath :: Text }

-- | A qualified key
data Key = Key
  { kPath :: !Path
  , kName :: !Text
  }

-- | An external, likely remote, store.
-- NOTE: get and put are restricted to objects less than 10MB.
data Store = Store
  { list :: Path -> Stream' Key IO ()
  , get  :: Key  -> IO (Maybe LB.ByteString)
  , put  :: Key  -> LB.ByteString -> IO ()
  --  , remove :: Key -> IO ()
  }
