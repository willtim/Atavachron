{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE EmptyDataDecls #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE ViewPatterns #-}

-- | An encoding-independent, type-safe and efficient file path.
--
-- The standard library FilePath (a synonym for String) is flawed, in
-- that it takes the bytes returned by a POSIX system call and assumes
-- a default encoding when no encoding information was given. It is
-- also somewhat inefficient to re-allocate all this string data on
-- the Haskell heap, when instead one can just keep references to the
-- original returned bytes.

module Atavachron.Path where

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8

import Codec.Serialise

import Data.Monoid

import Data.Foldable
import Data.Sequence (Seq)
import qualified Data.Sequence as Seq
import qualified Data.Text as T
import Data.Text.Encoding (encodeUtf8)

import qualified System.FilePath.Posix as FilePath
import System.Posix.ByteString.FilePath

import qualified GHC.IO.Encoding as GHC
import qualified GHC.Foreign as GHC

import qualified Data.List as List

type RawName = B.ByteString

data Abs
data Rel
data File
data Dir

-- | An encoding-independent, type-safe and efficient file path
-- representation which ensures *normal form*. A path in normal form
-- has no redundant current directory (dot) elements, no redundant
-- parent directory (dot-dot) elements, and no redundant
-- directory-separators.
--
data Path :: * -> * -> * where
  -- an efficient representation which permits sharing of bytestring
  -- elements when concatenating paths.
  AbsDir   :: !(Seq RawName)            -> Path Abs Dir
  -- an efficient normal form for relative paths
  -- comprising of pops followed by pushes.
  RelDir   :: !Int -> !(Seq RawName)    -> Path Rel Dir
  -- a file path comprises a directory and a file name.
  FilePath :: !(Path b Dir) -> !RawName -> Path b File

-- | Path concatenation.
(</>) :: Path b Dir -> Path Rel t -> Path b t
(</>) dir (RelDir pops ns) =
    foldl pushDir (foldl' (const . popDir) dir [1..pops]) ns
(</>) dir (FilePath relDir name) =
    FilePath (dir </> relDir) name

pushDir :: Path b Dir -> RawName -> Path b Dir
pushDir (AbsDir ns) n      = AbsDir $ ns Seq.|> n
pushDir (RelDir pops ns) n = RelDir pops $ ns Seq.|> n

popDir :: Path b Dir -> Path b Dir
popDir (AbsDir ns) = AbsDir $ dropR 1 ns
popDir (RelDir pops ns)
    | null ns   = RelDir (if pops>0 then pred pops else pops) ns
    | otherwise = RelDir pops (dropR 1 ns)

-- should be in Data.Sequence
dropR :: Int -> Seq a -> Seq a
dropR i s = fst $ Seq.splitAt (length s - i) s

-- | Get the parent directory of a path.
parent :: Path Abs t -> Path Abs Dir
parent (AbsDir ns) = AbsDir ns
parent (FilePath dir _) = dir

-- | Get the filename of a file path.
filename :: Path b File -> RawName
filename (FilePath _ name) = name

makeFilePath :: Path b Dir -> RawName -> Path b File
makeFilePath = FilePath

pathElems :: Path Abs t -> Seq RawName
pathElems (AbsDir ns) = ns
pathElems (FilePath dir n) = pathElems dir Seq.|> n

getRawFilePath :: Path b t -> RawFilePath
getRawFilePath = B.concat . ("/":) . List.intersperse "/" . go
  where
    go :: Path b t -> [RawName]
    go (AbsDir ns)      = toList ns
    go (RelDir pops ns) = replicate pops ".." <> toList ns
    go (FilePath dir n) = go dir <> [n]

-- | Relativise the second path, using the first path as the prefix.
relativise :: Path Abs Dir -> Path Abs t -> Path Rel t
relativise prefix absDir@(AbsDir{})      = makeRelDir prefix absDir
relativise prefix (FilePath absDir name) = flip makeFilePath name $ makeRelDir prefix absDir

makeRelDir :: Path Abs Dir -> Path Abs Dir -> Path Rel Dir
makeRelDir path1 path2 = RelDir pops pushes
  where
    pops   = length . Seq.drop depth $ pathElems path1
    pushes = Seq.drop depth $ pathElems path2
    root   = getCommonPrefix path1 path2
    depth  = getDepth root

getDepth :: Path Abs Dir -> Int
getDepth (AbsDir ns) = length ns

getCommonPrefix :: Path Abs Dir -> Path Abs Dir -> Path Abs Dir
getCommonPrefix (AbsDir ns) (AbsDir ns') =
    AbsDir $ go Seq.empty ns ns'
  where
    go zs (Seq.viewl -> x Seq.:< xs') (Seq.viewl -> y Seq.:< ys')
        | x == y        = go (x Seq.<| zs) xs' ys'
        | otherwise     = zs
    go zs _      _      = zs

instance Show (Path b t) where
    show = B8.unpack . getRawFilePath

instance Eq (Path b Dir) where
    (==) (AbsDir ns) (AbsDir ns') =
        ns == ns'
    (==) (RelDir pops ns) (RelDir pops' ns') =
        pops == pops' && ns == ns'

instance Ord (Path Abs Dir) where
    compare (AbsDir ns) (AbsDir ns') =
        compare ns ns'

instance Eq (Path b File) where
    (==) (FilePath dir name) (FilePath dir' name') =
        name == name' && dir == dir'

instance Ord (Path Abs File) where
    compare (FilePath dir name) (FilePath dir' name') =
        case compare dir dir' of
            EQ -> compare name name'
            x  -> x

instance Serialise (Path Abs Dir) where
    encode (AbsDir ns) = encode ns
    decode = AbsDir <$> decode

instance Serialise (Path Rel Dir) where
    encode (RelDir pops ns) = encode pops <> encode ns
    decode = RelDir <$> decode <*> decode

instance Serialise (Path Abs File) where
    encode (FilePath dir name) = encode dir <> encode name
    decode = FilePath <$> decode <*> decode

instance Serialise (Path Rel File) where
    encode (FilePath dir name) = encode dir <> encode name
    decode = FilePath <$> decode <*> decode

-- | This is necessary in order to use many Haskell library
-- functions.
getFilePath :: Path b t -> IO FilePath
getFilePath p = do
    let !rfp = getRawFilePath p
    enc <- GHC.getFileSystemEncoding
    B.useAsCString rfp $! GHC.peekCString enc

-- TODO should use GHC.getFileSystemEncoding
parseAbsDir :: FilePath -> Maybe (Path Abs Dir)
parseAbsDir fp
    | FilePath.isValid fp && FilePath.isAbsolute fp =
      case FilePath.splitDirectories fp of
          ("/":ns) -> Just $ AbsDir (Seq.fromList $ map (encodeUtf8 . T.pack) ns)
          _        -> Nothing
    | otherwise = Nothing
