{-# LANGUAGE RecordWildCards #-}
module Atavachron.Prune where

import Atavachron.Config (PruneSettings(..))
import Atavachron.Repository (Snapshot(..), SnapshotName)

import Data.Function (on)
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Ord
import Data.Time (UTCTime(..))
import Data.Time.Calendar
import Data.Time.Calendar.Julian
import Data.Time.Calendar.WeekDate

import Data.List (groupBy, sortOn)

data TimeMeta a = TimeMeta
  { tmDays   :: Integer
  , tmWeeks  :: Integer
  , tmMonths :: Integer
  , tmYear   :: Integer
  , tmTime   :: UTCTime
  , tmData   :: a
  } deriving Show

-- | Prunes the supplied snapshot map according to the settings.
-- NOTE: It is distinct backups that count, not any kind of
-- time-to-live, e.g. keep 3 days means the last 3 recent distinct
-- days. Snapshots selected by previous rules are not considered by
-- later rules.
pruneSnapshots
    :: PruneSettings
    -> Map SnapshotName Snapshot
    -> Map SnapshotName Snapshot
pruneSnapshots settings snapshots =
    Map.fromList $ prune settings (sStartTime . snd) (Map.toList snapshots)

prune :: PruneSettings -> (a -> UTCTime) -> [a] -> [a]
prune PruneSettings{..} f items = pruned
  where
    (_, pruned) = maybe id (keep tmYear)   keepYearly
                . maybe id (keep tmMonths) keepMonthly
                . maybe id (keep tmWeeks)  keepWeekly
                . maybe id (keep tmDays)   keepDaily
                $ (metas, [])

    keep g n (ms, xs) =
        let (ms', ms'') = splitAt (fromInteger n) (distinct g ms)
        in (ms'', xs ++ map tmData ms')

    distinct g = map head . groupBy ((==) `on` g)

    -- latest to earliest
    metas = sortOn (Down . tmTime) [mkTimeMeta (f i) i | i <- items]

mkTimeMeta :: UTCTime -> a -> TimeMeta a
mkTimeMeta time = TimeMeta days weeks months year time
  where
    days = toModifiedJulianDay day

    months = 12 * (year - year0) + (fromIntegral month - fromIntegral month0)
    weeks  = 52 * (year - year0) + (fromIntegral week - fromIntegral week0)

    (_, month, _)     = toJulian day
    (year, week, _)   = toWeekDate day
    (_, month0, _)    = toJulian day0
    (year0, week0, _) = toWeekDate day0

    day  = utctDay time
    day0 = ModifiedJulianDay 0


------------------------------------------------------------
-- Test

test :: IO ()
test = mapM_ print [ mkTimeMeta m () | m <- prune settings id items]
  where
    settings = PruneSettings (Just 3) (Just 2) Nothing (Just 2)
    items = map (\(Just d) -> UTCTime d 0)
            [ fromGregorianValid 2019 03 13 -- D1
            , fromGregorianValid 2019 03 13
            , fromGregorianValid 2019 03 12 -- D2
            , fromGregorianValid 2019 03 11 -- D3
            , fromGregorianValid 2019 03 10 -- W1
            , fromGregorianValid 2019 03 09
            , fromGregorianValid 2018 12 22 -- W2
            , fromGregorianValid 2018 11 30 -- Y1
            , fromGregorianValid 2018 10 01
            , fromGregorianValid 2018 09 01
            , fromGregorianValid 2017 03 31 -- Y2
            ]
