{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE ViewPatterns #-}

-- | Functions for enumerating files and directories, performing diffs
-- and serialising/deserialising the resultant file metadata.

module Atavachron.Tree where

import Control.Monad
import Control.Monad.Catch
import Control.Monad.ST.Unsafe
import Control.Monad.Reader
import Control.Monad.Trans.Resource

import Codec.Serialise

import Data.Maybe
import Data.Monoid

import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as LB

import qualified Data.List as List
import qualified Data.Sequence as Seq

import Streaming
import Streaming.Prelude (yield, next)
import qualified Streaming.Prelude as S

import Data.Time.Clock.POSIX (posixSecondsToUTCTime)

import System.Posix.Files.ByteString (FileStatus)
import qualified System.Posix.Files.ByteString as Files
import qualified System.Posix.Directory.ByteString as Dir

import Atavachron.Types
import Atavachron.Path
import Atavachron.Streaming (Stream')
import qualified Atavachron.Streaming as S
import Atavachron.Chunk.Builder

import Data.Ord (comparing)

-- | RawItem is what we generate using enumDir.
-- Items can be either a regular file, a directory or a soft link.
data RawItem = RawItem
  { itemKind   :: !Kind
  , itemName   :: !RawName
  , itemStatus :: !FileStatus
  }

data PatchingError = PatchingError
    deriving Show

instance Exception PatchingError


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
      checkName ".atavachron" = nextName -- do not backup our working files
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
    , fileMode  =                         Files.fileMode              itemStatus
    , fileUID   =                         Files.fileOwner             itemStatus
    , fileGID   =                         Files.fileGroup             itemStatus
    , fileMTime = posixSecondsToUTCTime $ Files.modificationTimeHiRes itemStatus
    , fileATime = posixSecondsToUTCTime $ Files.accessTimeHiRes       itemStatus
    , fileCTime = posixSecondsToUTCTime $ Files.statusChangeTimeHiRes itemStatus
    , fileINode = fromIntegral          $ Files.fileID                itemStatus
    , fileSize  = fromIntegral          $ Files.fileSize              itemStatus
    }

-- | Compress paths in the incoming stream by encoding each successive
-- path as relative to its predecessor.
relativePaths
    :: (Monad m, HasPath entry)
    => Path Abs Dir
    -> Stream' (entry Abs)  m r
    -> Stream' (entry Rel) m r
relativePaths = S.mapAccumM_ $ \prevDir entry -> do
    let entry'   = mapPath (relativise' prevDir) entry
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
     => Stream' (FileItem, ChunkList) m ()
     -> Stream' FileItem m ()
     -> Stream' (Diff (FileItem, ChunkList) FileItem) m ()
diff = loop
  where
    loop oldS newS = do
        old <- lift (next oldS)
        new <- lift (next newS)
        case (old, new) of
            (Left{}, Left{})        -> return ()
            (Left{}, Right (y, ys)) -> insert y >> loop mempty ys
            (Right (x, xs), Left{}) -> delete x >> loop xs mempty
            (Right (x, xs), Right (y, ys))
                | filePath' x == filePath y -> keepOrChange x y >> loop xs ys
                  -- y must have been inserted in NEW, since y cannot follow x in OLD
                | filePath' x >  filePath y -> insert y >> loop (S.cons x xs) ys
                  -- x must have been deleted in NEW, since y can only follow x in OLD
                | filePath' x <  filePath y -> delete x >> loop xs (S.cons y ys)
                | otherwise -> error "diff: Assertion failed"

    delete old = yield (Delete old)

    insert new = yield (Insert new)

    keepOrChange old new
        | fileMTime' old == fileMTime new = yield (Keep old)
        | otherwise  = yield (Change new)

    filePath'  = filePath  . fst
    fileMTime' = fileMTime . fst

patch :: forall m. MonadThrow m
      => Stream' (Diff FileItem FileItem) m ()
      -> Stream' FileItem m ()
      -> Stream' FileItem m ()
patch = loop
  where
    loop ds es = do
        edits  <- lift (next ds)
        case edits of
            Right (Insert x, xs) -> insert x >> patch xs es
            Right (Delete x, xs) -> patch xs (delete x es)
            Right (Keep   x, xs) -> insert x >> patch xs (delete x es)
            Right (Change x, xs) -> insert x >> patch xs (delete x es)
            Left{}               -> do
                entries <- lift (next es)
                case entries of
                    Left{}       -> return ()
                    Right{}      -> lift $ throwM PatchingError

    insert :: FileItem -> Stream' FileItem m ()
    insert = yield

    delete :: FileItem -> Stream' FileItem m () -> Stream' FileItem m ()
    delete e es = do
        entries <- lift (next es)
        case entries of
            Left{}          -> lift $ throwM PatchingError
            Right(e',es')
                | e == e'   -> es'
                | otherwise -> lift $ throwM PatchingError

data DeserialiseTreeError = DeserialiseTreeError SomeException
    deriving Show

instance Exception DeserialiseTreeError


-- | Tree data written as one large chunked and de-duplicated file.
serialiseTree
    :: forall m r. (MonadReader (Env Backup) m)
    => Stream' (Either (FileItem, ChunkList) OtherItem) m r
    -> Stream' (RawChunk Tree B.ByteString) m r
serialiseTree str = do
    sourceDir <- asks $ bSourceDir . envParams
    S.map (RawChunk Tree . LB.toStrict . serialise)
      . relativePaths sourceDir
      . S.map mkTreeEntry
      . reorderOtherItems
      . S.left compressLists
      $ str
  where

    mkTreeEntry
        :: (Either (FileItem, ChunkListDelta) OtherItem)
        -> TreeEntry Abs
    mkTreeEntry (Left (file, chunks))         = FileEntry file chunks
    mkTreeEntry (Right (DirItem dir))         = DirEntry dir
    mkTreeEntry (Right (LinkItem lnk target)) = LinkEntry lnk target

    -- The interleaving of non-regular files and (chunked/encoded)
    -- regular files depends on the evaluation strategy used. For
    -- example, uses of parallel map will read-ahead for only regular
    -- files. This code ensures consistent ordering, to ensure
    -- deduplication of snapshot tree chunks.
    -- NOTE: This direct implementation turned out to be *much*
    -- faster than one based on S.concat and S.mapAccum.
    reorderOtherItems
        :: Stream' (Either (FileItem, chunks) OtherItem) m r
        -> Stream' (Either (FileItem, chunks) OtherItem) m r
    reorderOtherItems = loop Seq.empty
      where
        loop !items !str = do
            res <- lift $ next str
            case res of
                Left r -> return r
                Right (e'item, str') ->
                    case e'item of
                        l@(Left (fileItem, _)) -> do
                            let (!pending, !items') = Seq.spanl ((<= pathElems (filePath fileItem)) . elems) items
                            S.each $ fmap Right pending Seq.|> l
                            loop items' str'
                        (Right nrItem) ->
                            loop (items Seq.|> nrItem) str'

        elems (DirItem item)    = pathElems (filePath item)
        elems (LinkItem item _) = pathElems (filePath item)

-- NOTE: it's catastrophic if we cannot deserialise the tree
deserialiseTree
    :: (MonadReader (Env Restore) m, MonadThrow m, MonadIO m)
    => Stream' (RawChunk Tree B.ByteString) m r
    -> Stream' (Either (FileItem, ChunkList) OtherItem) m r
deserialiseTree str = do
    targetDir <- lift . asks $ rTargetDir . envParams
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
-- the same storeID.
compressLists
    :: Monad m
    => Stream' (t, ChunkList) m r
    -> Stream' (t, ChunkListDelta) m r
compressLists = flip S.mapAccum_ Nothing $ \prevCL (t, currCL) ->
    let currCL' = case (unChunkList <$> prevCL, unChunkList currCL) of
                      ( Just (Seq.viewr -> _ Seq.:> (storeID, _)),
                        (Seq.viewl -> (storeID', offset) Seq.:< Seq.Empty))
                                  | storeID == storeID' -> PrevChunk offset
                      (_, chunks) | otherwise           -> NewChunkList chunks
    in (Just currCL, (t, currCL'))

uncompressLists
    :: Monad m
    => Stream' (t, ChunkListDelta) m r
    -> Stream' (t, ChunkList) m r
uncompressLists = flip S.mapAccum_ Nothing $ \prevChunk (t, currCL) ->
    let currCL' = case (prevChunk, currCL) of
                      (Just storeID, PrevChunk offset)    -> ChunkList $ Seq.singleton (storeID, offset)
                      (Nothing     , PrevChunk {})        -> error "unpackChunkLists: Assertion failed!"
                      (_           , NewChunkList chunks) -> ChunkList chunks
        prevChunk'
                = case unChunkList currCL' of
                      (Seq.viewr -> _ Seq.:> (storeID, _)) -> Just storeID
                      _ -> Nothing
    in (prevChunk', (t, currCL'))
