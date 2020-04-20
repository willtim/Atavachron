{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Use SQLite as a simple disk-backed collections.
-- The Field class and instances.
module Atavachron.DB.Field
  ( Field(..)
  ) where

import           Prelude hiding (lookup)
import           Data.Int
import           Data.Proxy
import qualified Data.Text as T
import           Database.SQLite3 (SQLData(..))

import Codec.Serialise
import qualified Data.ByteString.Lazy as LB

import Atavachron.Chunk.Encode (StoreID(..))

class Field v where
    sqlType   :: Proxy v -> T.Text
    toField   :: v -> SQLData
    fromField :: SQLData -> Maybe v

instance {-# OVERLAPPING #-} Field Int64 where
    sqlType _ = "INTEGER(8)"
    toField   = SQLInteger
    fromField (SQLInteger i) = Just i
    fromField _              = Nothing

instance {-# OVERLAPPING #-} Field StoreID where
    sqlType _ = "BLOB(32)"
    toField (StoreID b)   = SQLBlob b
    fromField (SQLBlob b) = Just (StoreID b)
    fromField _           = Nothing

instance {-# OVERLAPPABLE #-} Serialise v => Field v where
    sqlType _ = "BLOB"
    toField   = SQLBlob . LB.toStrict . serialise
    fromField (SQLBlob b) = Just . deserialise . LB.fromStrict $! b
    fromField _           = Nothing

instance {-# OVERLAPPING #-} Serialise v => Field (Maybe v) where
    sqlType _ = "BLOB"
    toField (Just v)   = SQLBlob . LB.toStrict $ serialise v
    toField Nothing    = SQLNull
    fromField (SQLBlob b) = Just . Just . deserialise . LB.fromStrict $! b
    fromField SQLNull     = Just Nothing
    fromField _           = Nothing
