-- MT routing redesign (docs/路由重构设计.md 2.4): the backup level a message has reached, on every table that keeps messages.
--   RoutingLevel  0 = main route, 1..10 = backup level (A2PBackupRouting, A2PBackup2Routing ... A2PBackup10Routing).
--                 NULL = written before this column existed: read as "level not known" (see below).
-- The channels already used stay in the existing column RoutingSsIDs (unchanged).
--
-- Run before (or after) the upgrade. The code finds the column by itself (GmmsConfig DB.RoutingLevelColumn=auto,
-- the same way as DB.DrErrorCodeColumns) and only writes it once every table has it; after running this on a node
-- that is up: RELOAD_ROUTING.
-- Without the column (or with NULL): a message retried from the DB finds the next level from level 1 again, skipping
-- the channels in RoutingSsIDs (what the code does today), so it is never sent twice to the same channel.
-- MySQL 8.0.12+: add ", ALGORITHM=INSTANT" to each ALTER for no table copy; 5.7 copies the table (run off-peak).
-- A table that does not exist in a DB is skipped by the code; leave it out here too.

ALTER TABLE messagestore ADD COLUMN RoutingLevel tinyint NULL DEFAULT NULL;
ALTER TABLE PMQ  ADD COLUMN RoutingLevel tinyint NULL DEFAULT NULL;
ALTER TABLE RMQ  ADD COLUMN RoutingLevel tinyint NULL DEFAULT NULL;
ALTER TABLE SMQ  ADD COLUMN RoutingLevel tinyint NULL DEFAULT NULL;
ALTER TABLE WDQ  ADD COLUMN RoutingLevel tinyint NULL DEFAULT NULL;
ALTER TABLE QDQ  ADD COLUMN RoutingLevel tinyint NULL DEFAULT NULL;
ALTER TABLE RDQ  ADD COLUMN RoutingLevel tinyint NULL DEFAULT NULL;
ALTER TABLE SDQ  ADD COLUMN RoutingLevel tinyint NULL DEFAULT NULL;
ALTER TABLE IQDQ ADD COLUMN RoutingLevel tinyint NULL DEFAULT NULL;
ALTER TABLE CSMQ ADD COLUMN RoutingLevel tinyint NULL DEFAULT NULL;

-- PMQ daily tables (PMQ_yyyyMMdd_0N) created before the upgrade: new ones get the column from the code.
-- This prints one ALTER per existing table; run its output.
SELECT CONCAT('ALTER TABLE `', table_name, '` ADD COLUMN RoutingLevel tinyint NULL DEFAULT NULL;')
  FROM information_schema.columns c0
 WHERE table_schema = DATABASE() AND table_name LIKE 'PMQ\_%' AND column_name = 'ID'
   AND NOT EXISTS (SELECT 1 FROM information_schema.columns c1
                    WHERE c1.table_schema = c0.table_schema AND c1.table_name = c0.table_name AND c1.column_name = 'RoutingLevel');

-- Check: every table has the column (expect one row per table above, plus the PMQ daily tables).
-- SELECT table_name FROM information_schema.columns WHERE table_schema = DATABASE() AND column_name = 'RoutingLevel';

-- Rollback (set DB.RoutingLevelColumn=false first, or drop and send RELOAD_ROUTING: the code stops writing it):
-- ALTER TABLE RMQ DROP COLUMN RoutingLevel;   -- and the same for the other tables
