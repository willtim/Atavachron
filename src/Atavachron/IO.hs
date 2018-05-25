{-# LANGUAGE ScopedTypeVariables #-}

-- | This module contains performant IO routines for ByteStrings that
-- use raw file paths.

module Atavachron.IO where

import Control.Monad
import Control.Monad.IO.Class
import Data.Foldable
import qualified Data.ByteString as B
import qualified Data.ByteString.Internal as BSI

import Data.Sequence (Seq)
import qualified Data.Sequence as Seq

import Streaming.Prelude (yield, Of(..))
import qualified Streaming.Prelude as S
import qualified Atavachron.Streaming as S

import System.Posix.Types

import qualified System.Posix.IO.ByteString as IO
import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Ptr (plusPtr)
import Foreign.C.Error

import Atavachron.Streaming

-- | This uses POSIX read.
fdGetContents
    :: (MonadIO m)
    => Fd
    -> Int -- ^ chunk size
    -> Stream' B.ByteString m ()
fdGetContents fd sz = loop
  where
    loop = do
        chunk <- liftIO $ fdGet fd sz
        let !size = B.length chunk
        yield $ chunk
        unless (size < sz) loop

-- | This uses POSIX read. EOF is indicated by returning a smaller/empty ByteString.
fdGet :: Fd -> Int -> IO B.ByteString
fdGet fd sz = BSI.createAndTrim sz $ \buf ->
                   fromIntegral <$> IO.fdReadBuf fd buf (fromIntegral sz)

-- | This uses POSIX write.
fdPut :: Fd -> B.ByteString -> IO ()
fdPut fd (BSI.PS ps s l) =
     withForeignPtr ps $ \p ->
        throwErrnoIf_ (/= fromIntegral l) "fdPut" $
            IO.fdWriteBuf fd (p `plusPtr` s) (fromIntegral l)

buffer :: Monad m => Stream' B.ByteString m r -> Stream' B.ByteString m r
buffer str = do
    (bss, _) :> r <- S.catMaybes $ S.mapAccum step (mempty, 0) str
    yield $ fold bss -- remaining
    return r
  where
    size = 32768 -- 32K

    step :: (Seq B.ByteString, Int)
         -> B.ByteString
         -> ((Seq B.ByteString, Int), Maybe B.ByteString)
    step (bss, len) bs
        | len' > size = ((mempty, 0),  Just $ fold bss')
        | otherwise   = ((bss', len'), Nothing)
      where
        bss' = bss Seq.|> bs
        len' = len + B.length bs
