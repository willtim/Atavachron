
-- | Use SQLite as a simple disk-backed mutable Set.
-- This allows is to scale up to huge numbers of files and chunks.

module Atavachron.DB.MSet
  ( open
  , close
  , withSet
  , withEmptySet
  , insert
  , delete
  , member
  , notMember
  , size
  , elems
  , clear
  , MSet
  ) where

import Control.Monad.IO.Class
import Control.Monad.Catch
import Data.Int
import Data.Text (Text)

import Streaming (Stream, Of(..))

import qualified Atavachron.DB.MMap as MMap
import Atavachron.DB.MMap (MMap)
import Atavachron.DB.Field
import Atavachron.DB.Connection


newtype MSet k = MSet { unMSet :: MMap k () }

withSet :: (MonadIO m, MonadMask m, Field k) => Connection -> Text -> (MSet k -> m r) -> m r
withSet conn name f = MMap.withMap conn name (f . MSet)

withEmptySet :: (MonadIO m, MonadMask m, Field k) => Connection -> Text -> (MSet k -> m r) -> m r
withEmptySet conn name f = MMap.withEmptyMap conn name (f . MSet)

open :: (Field k) => Connection -> Text -> IO (MSet k)
open conn name = MSet <$> MMap.open conn name

close :: MSet k -> IO ()
close = MMap.close . unMSet

insert :: (Field k) => MSet k -> k -> IO ()
insert (MSet m) k = MMap.insert m k ()

delete :: (Field k) => MSet k -> k -> IO ()
delete (MSet m) = MMap.delete m

member :: (Field k) => MSet k -> k -> IO Bool
member (MSet m) = MMap.member m

notMember :: (Field k) => MSet k -> k -> IO Bool
notMember (MSet m) = fmap not . MMap.member m

size :: MSet k -> IO Int64
size = MMap.size . unMSet

elems :: (Field k) => MSet k -> Stream (Of k) IO ()
elems (MSet m) = MMap.keys m

clear :: MSet k -> IO ()
clear = MMap.clear . unMSet
