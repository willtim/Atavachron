{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE ViewPatterns #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

-- | Functions for enumerating files and directories, performing diffs
-- and serialising/deserialising the resultant file metadata.

module Atavachron.Tree where

import Codec.Serialise

import Control.Monad
import Control.Monad.Catch
import Control.Monad.ST.Unsafe
import Control.Monad.Reader
import Control.Monad.Trans.Resource

import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as LB
import Data.Function (on)
import Data.Int
import Data.Maybe
import qualified Data.List as List
import Data.Ord (comparing)
import Data.Sequence (Seq)
import qualified Data.Sequence as Seq
import Data.Time (NominalDiffTime)

import Streaming
import Streaming.Prelude (yield, next)
import qualified Streaming.Prelude as S

import System.Posix.Types
import System.Posix.Files.ByteString (FileStatus)
import qualified System.Posix.Files.ByteString as Files
import qualified System.Posix.Directory.ByteString as Dir

import GHC.Generics (Generic)

import Atavachron.Env
import Atavachron.Repository
import Atavachron.Path
import Atavachron.Streaming (Stream')
import qualified Atavachron.Streaming as S
import Atavachron.Chunk.Builder


data TreeEntry b
  = FileEntry !(FileMeta (Path b File)) !ChunkList
  | DirEntry  !(FileMeta (Path b Dir))
  | LinkEntry !(FileMeta (Path b File)) !Target
  deriving (Generic, Show)

instance Serialise (TreeEntry Rel)

-- | Tag for tree data.
data Tree = Tree
  deriving (Eq, Show)

data FileCacheEntry b = FileCacheEntry
    { ceFileItem  :: !(FileMeta (Path b File))
    , ceChunkList :: !ChunkList
    }
  deriving (Generic)

instance Serialise (FileCacheEntry Rel)

-- | The FileMeta type contains exploded metadata for a file, directory or soft link.
-- This is the archival format.
data FileMeta path = FileMeta
    { filePath   :: !path -- e.g. Abs/Rel File/Dir
    , fileMode   :: !CMode
    , fileUID    :: !CUid
    , fileGID    :: !CGid
    , fileMTime  :: !NominalDiffTime
    , fileATime  :: !NominalDiffTime
    , fileCTime  :: !NominalDiffTime
    , fileINode  :: !Int64
    , fileSize   :: !Int64
    } deriving (Generic, Functor, Show)

-- FileMeta has identity based on file path.
instance Eq path => Eq (FileMeta path) where
    (==) = (==) `on` filePath

type FileItem = FileMeta (Path Abs File)
type DirItem  = FileMeta (Path Abs Dir)

-- | Non-regular files
data OtherItem
    = LinkItem !FileItem !Target
    | DirItem  !DirItem
    deriving Show

type Target = B.ByteString

data Kind = FileK | DirK | LinkK !Target
    deriving (Generic, Eq, Show)

instance (Serialise path) => Serialise (FileMeta path)

instance Serialise NominalDiffTime where
    encode = encode . toRational
    decode = fromRational <$> decode

-- | RawItem is what we generate using enumDir.
-- Items can be either a regular file, a directory or a soft link.
data RawItem = RawItem
  { itemKind   :: !Kind
  , itemName   :: !RawName
  , itemStatus :: !FileStatus
  }

-- | We use this class to relativise the paths in different structures.
class HasPath e where
    mapPath :: (forall t. Path b t -> Path b' t) -> e b -> e b'
    getParent :: e Abs -> Path Abs Dir

instance HasPath FileCacheEntry where
    mapPath f (FileCacheEntry item chunks) = FileCacheEntry (item {filePath = f (filePath item)}) chunks
    getParent (FileCacheEntry item _)      = parent (filePath item)

instance HasPath TreeEntry where
    mapPath f (FileEntry item chunks) = FileEntry (item {filePath = f (filePath item)}) chunks
    mapPath f (DirEntry item)         = DirEntry  (item {filePath = f (filePath item)})
    mapPath f (LinkEntry item target) = LinkEntry (item {filePath = f (filePath item)}) target
    getParent (FileEntry item _)      = parent (filePath item)
    getParent (DirEntry item)         = parent (filePath item)
    getParent (LinkEntry item _)      = parent (filePath item)


-- | Enumerate items in the supplied path. Not Recursive. Not Ordered.
-- ResourceT is used to guarantee closure of handles in the presence of exceptions.
enumDir :: MonadResource m => Path Abs Dir -> Stream' RawItem m ()
enumDir dir = do
  let rfp = getRawFilePath dir
  (key, ds) <- lift $ allocate (liftIO $ Dir.openDirStream rfp)
                               (liftIO . Dir.closeDirStream)

  let nextName = liftIO (Dir.readDirStream ds) >>= checkName

      checkName ""   = release key
      checkName "."  = nextName
      checkName ".." = nextName
      checkName ".cache" = nextName -- do not backup our working files
      checkName name = liftIO (Files.getSymbolicLinkStatus path)
                       >>= checkStat name path
          where path = rfp <> "/" <> name

      checkStat name path stat
          | Files.isRegularFile stat = do
              isRead <- liftIO $ Files.fileAccess path True False False
              when isRead $ do
                  fs <- liftIO $ Files.getFileStatus path
                  yield $ RawItem FileK name fs
              nextName
          | Files.isDirectory stat = do
              isReadExec <- liftIO $ Files.fileAccess path True False True
              when isReadExec $ do
                  fs <- liftIO $ Files.getFileStatus path
                  yield $ RawItem DirK name fs
              nextName
          | Files.isSymbolicLink stat = do
              isRead <- liftIO $ Files.fileAccess path True False False
              target <- liftIO $ Files.readSymbolicLink path
              when isRead $ do
                  fs <- liftIO $ Files.getFileStatus path
                  yield $ RawItem (LinkK target) name fs
              nextName
          | otherwise = nextName

  nextName

-- | Enumerate all files and folders recursively, returning an ordered
-- stream of files (including non-regular files, i.e. directories and
-- symbolic links).
-- NOTE: In order to sort all entries in a particular directory for
-- diffing, we do need to realise them in memory.  We must use the
-- exact same scheme for ordering as the diff algorithm.
recurseDir :: MonadResource m => Path Abs Dir -> Stream' (Either FileItem OtherItem) m ()
recurseDir dir = do
    inDir :> _ <- lift $ S.toList $ enumDir dir
    let files = List.sortBy (comparing itemName) inDir
    forM_ files $ \raw@RawItem{..} ->
        case itemKind of
          DirK  -> do
              let dir' = pushDir dir itemName
              yield $ Right $ DirItem $ makeFileMeta raw dir'
              recurseDir dir'
          LinkK target -> do
              let file = makeFilePath dir itemName
              yield $ Right $ LinkItem (makeFileMeta raw file) target
          FileK -> do
              let file = makeFilePath dir itemName
              yield $ Left  $ makeFileMeta raw file


makeFileMeta :: RawItem -> Path b t -> FileMeta (Path b t)
makeFileMeta RawItem{..} path = FileMeta
    { filePath  = path
    , fileMode  =                Files.fileMode              itemStatus
    , fileUID   =                Files.fileOwner             itemStatus
    , fileGID   =                Files.fileGroup             itemStatus
    , fileMTime =                Files.modificationTimeHiRes itemStatus
    , fileATime =                Files.accessTimeHiRes       itemStatus
    , fileCTime =                Files.statusChangeTimeHiRes itemStatus
    , fileINode = fromIntegral $ Files.fileID                itemStatus
    , fileSize  = fromIntegral $ Files.fileSize              itemStatus
    }

-- | Compress paths in the incoming stream by encoding each successive
-- path as relative to its predecessor.
relativePaths
    :: (Monad m, HasPath entry)
    => Path Abs Dir
    -> Stream' (entry Abs)  m r
    -> Stream' (entry Rel) m r
relativePaths = S.mapAccumM_ $ \prevDir entry -> do
    let entry'   = mapPath (relativise prevDir) entry
        absDir   = getParent entry
    return (absDir, entry')

-- | Uncompress to absolute paths by decoding each successive path
-- relative to its predecessor.
absolutePaths
    :: (Monad m, HasPath entry)
    => Path Abs Dir
    -> Stream' (entry Rel) m r
    -> Stream' (entry Abs) m r
absolutePaths = S.mapAccumM_ $ \prevDir entry -> do
    let entry'   = mapPath (prevDir </>) entry
        absDir   = getParent entry'
    return (absDir, entry')


data Diff a b
  = Insert b
  | Change b
  | Delete a
  | Keep   a
  deriving (Functor, Show)

diff :: MonadResource m
     => (old -> FileItem)
     -> (new -> FileItem)
     -> Stream' old m ()
     -> Stream' new m ()
     -> Stream' (Diff old new) m ()
diff f g = loop
  where
    loop oldS newS = do
        old <- lift (next oldS)
        new <- lift (next newS)
        case (old, new) of
            (Left{}, Left{})        -> return ()
            (Left{}, Right (y, ys)) -> insert y >> loop mempty ys
            (Right (x, xs), Left{}) -> delete x >> loop xs mempty
            (Right (x, xs), Right (y, ys))
                | filePath1 x == filePath2 y -> keepOrChange x y >> loop xs ys
                  -- y must have been inserted in NEW, since y cannot follow x in OLD
                | filePath1 x >  filePath2 y -> insert y >> loop (S.cons x xs) ys
                  -- x must have been deleted in NEW, since y can only follow x in OLD
                | filePath1 x <  filePath2 y -> delete x >> loop xs (S.cons y ys)
                | otherwise -> error "diff: Assertion failed"

    delete old = yield (Delete old)

    insert new = yield (Insert new)

    keepOrChange old new
        | fileMTime1 old == fileMTime2 new = yield (Keep old)
        | otherwise  = yield (Change new)

    filePath1  = filePath  . f
    fileMTime1 = fileMTime . f
    filePath2  = filePath  . g
    fileMTime2 = fileMTime . g


data DeserialiseTreeError = DeserialiseTreeError SomeException
    deriving Show

instance Exception DeserialiseTreeError


-- | Tree data written as one large chunked and de-duplicated file.
serialiseTree
    :: forall m r. (MonadReader Env m)
    => Stream' (Either (FileItem, ChunkList) OtherItem) m r
    -> Stream' (RawChunk Tree B.ByteString) m r
serialiseTree str = do
    sourceDir <- asks envDirectory
    S.map (RawChunk Tree . LB.toStrict . serialise)
      . relativePaths sourceDir
      . S.map mkTreeEntry
      . S.left compressLists
      $ str
  where

    mkTreeEntry
        :: (Either (FileItem, ChunkList) OtherItem)
        -> TreeEntry Abs
    mkTreeEntry (Left (file, chunks))         = FileEntry file chunks
    mkTreeEntry (Right (DirItem dir))         = DirEntry dir
    mkTreeEntry (Right (LinkItem lnk target)) = LinkEntry lnk target


-- NOTE: it's catastrophic if we cannot deserialise the tree
deserialiseTree
    :: (MonadReader Env m, MonadThrow m, MonadIO m)
    => Stream' (RawChunk Tree B.ByteString) m r
    -> Stream' (Either (FileItem, ChunkList) OtherItem) m r
deserialiseTree str = do
    targetDir <- lift . asks $ envDirectory
    S.left uncompressLists
        . S.map extract
        . absolutePaths targetDir
        . loop deserialiseIncremental Nothing
        $ str
  where

    loop !mdecoder !m'unused !str = do
        res     <- lift $ next str
        decoder <- liftIO . unsafeSTToIO $ mdecoder
        case decoder of
            Partial k ->
                case res of
                    Left{}                     -> loop (k m'unused) Nothing str
                    Right (RawChunk{..}, str') -> loop (k $ Just $ toBS m'unused <> rcData) Nothing str'

            Done unused _ entry -> do
                yield entry
                let unused' = toBS m'unused <> unused
                case res of
                    Left r     | B.null unused -> return r
                               | otherwise     -> loop deserialiseIncremental (Just $ unused') str
                    Right (RawChunk{..}, str') -> loop deserialiseIncremental (Just $ unused' <> rcData) str'

            Fail _ _ ex -> throwM $ DeserialiseTreeError (toException ex)

    toBS = fromMaybe mempty

    extract (FileEntry file chunks) = Left (file, chunks)
    extract (DirEntry dir)          = Right (DirItem dir)
    extract (LinkEntry lnk target)  = Right (LinkItem lnk target)


-- | An optimisation for small files, whereby we avoid repeated instances of
-- the same storeID, by writing out an empty list.
compressLists
    :: Monad m
    => Stream' (t, ChunkList) m r
    -> Stream' (t, ChunkList) m r
compressLists = flip S.mapAccum_ Nothing $ \prevCL (t, currCL) ->
    let currCL' = case (clChunks <$> prevCL, clChunks currCL) of
                      ( Just (Seq.viewr -> _ Seq.:> storeID),
                        (Seq.viewl -> storeID' Seq.:< Seq.Empty))
                            | storeID == storeID' -> ChunkList mempty (clOffset currCL)
                      _                           -> currCL
    in (Just currCL, (t, currCL'))

uncompressLists
    :: Monad m
    => Stream' (t, ChunkList) m r
    -> Stream' (t, ChunkList) m r
uncompressLists = flip S.mapAccum_ Nothing $ \prevChunk (t, currCL) ->
    let currCL' = case (prevChunk, currCL) of
                      (Just storeID, ChunkList s offset)
                          | null s -> ChunkList (Seq.singleton storeID) offset
                      (Nothing     , ChunkList s _)
                          | null s -> error "unpackChunkLists: Assertion failed!"
                      _            -> currCL
        prevChunk'
                = case clChunks currCL' of
                      (Seq.viewr -> _ Seq.:> storeID) -> Just storeID
                      _ -> Nothing
    in (prevChunk', (t, currCL'))


-- | Apply the FilePredicate to the supplied tree metadata and
-- filter out files/directories for backup or restore.
--
-- NOTE: The complication here is that we want to ensure that any
-- parent directories are restored, if they are needed by matching globs
-- of any child file or directory items.
filterItems
    :: forall m e r. (MonadReader Env m, MonadIO m)
    => (e -> FileItem)
    -> Stream' (Either e OtherItem) m r
    -> Stream' (Either e OtherItem) m r
filterItems extract str = do
    p         <- asks envFilePredicate
    targetDir <- asks envDirectory

    let apply :: FileMeta (Path Abs t) -> IO Bool
        apply item = applyPredicate p (relativise targetDir $ filePath item)

    let loop
            :: Stream' (Either e OtherItem) m r
            -> Seq (FileMeta (Path Abs Dir))
            -> Stream' (Either e OtherItem) m r
        loop !str !dirs = do
            res <- lift $ S.next str
            case res of
                Left r -> return r
                Right (x, str') -> step x str' dirs

        step
            :: Either e OtherItem
            -> Stream' (Either e OtherItem) m r
            -> Seq (FileMeta (Path Abs Dir))
            -> Stream' (Either e OtherItem) m r
        step x@(Left (extract -> item))  str' dirs = do
            keep <- liftIO $ apply item
            if keep
                then yieldWithDirs dirs x >> loop str' mempty
                else loop str' dirs
        step x@(Right (LinkItem item _)) str' dirs = do
            keep <- liftIO $ apply item
            when keep $ S.yield x
            loop str' dirs
        step x@(Right (DirItem item))    str' dirs = do
            keep <- liftIO $ apply item
            if keep
                then yieldWithDirs dirs x >> loop str' mempty
                else loop str' (pushDir item dirs)

    loop str mempty

  where

    yieldWithDirs dirs x =
        S.each (fmap (Right . DirItem) dirs) >> S.yield x

    isPrefixOf' item1 item2 =
        isPrefixOf (filePath item1) (filePath item2)

    pushDir item dirs =
        Seq.filter (`isPrefixOf'` item) dirs Seq.|> item
