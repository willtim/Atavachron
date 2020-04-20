{-# LANGUAGE OverloadedStrings #-}
module Atavachron.DB
    ( connect
    , disconnect
    , beginTx
    , commitTx
    , withTx
    , renameTable
    , Connection
    , Field
    ) where

import           Data.Text (Text)
import qualified Data.Text as T
import qualified Database.SQLite3 as SQL
import Control.Monad.IO.Class
import Atavachron.DB.Connection
import Atavachron.DB.Field


beginTx :: Connection -> IO ()
beginTx (Connection database) =
    SQL.exec database "BEGIN TRANSACTION;"

commitTx :: Connection -> IO ()
commitTx (Connection database) =
    SQL.exec database "COMMIT TRANSACTION;"

withTx :: MonadIO m => Connection -> m r -> m r
withTx conn f = do
    liftIO $ beginTx conn
    r <- f
    liftIO $ commitTx conn
    return r

renameTable :: Connection -> Text -> Text -> IO ()
renameTable (Connection database) old new =
    SQL.exec database $
        T.unlines [ "DROP TABLE IF EXISTS " <> new <> ";"
                  , "ALTER TABLE " <> old <> " RENAME TO " <> new <> ";"
                  ]
