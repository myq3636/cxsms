-- Channel steps restored in stream mode (docs/通道外发处理恢复与替换缺陷修复设计.md 3.3): the sender before the channel's
-- steps (after the customer's), on every table that keeps messages.
--   InSenderAddr  the sender each send to a channel starts from: a backup channel or a retry from RMQ rewrites it for
--                 its own channel, the rewrites of the channel before are not stacked.
--                 NULL = written before this column existed: the sender the message was last sent with is taken.
-- The recipient before the channel's steps is the existing column OriginalRecipientAddr (unchanged).
--
-- Run before (or after) the upgrade. The code finds the column by itself (GmmsConfig DB.InSenderAddrColumn=auto,
-- the same way as DB.RoutingLevelColumn) and only writes it once every table has it; after running this on a node
-- that is up: RELOAD_ROUTING.
-- Without the column: an MT retried from the DB takes the sender it was sent with; for a retry to the same channel
-- a prefix added by SMSOptionAddOriPrefix4NewMT or a Suffix of SMSOptionOriNumberLenControl may be added again.
-- MySQL 8.0.12+: add ", ALGORITHM=INSTANT" to each ALTER for no table copy; 5.7 copies the table (run off-peak).
-- A table that does not exist in a DB is skipped by the code; leave it out here too.

ALTER TABLE messagestore ADD COLUMN InSenderAddr varchar(100) NULL DEFAULT NULL;
ALTER TABLE PMQ  ADD COLUMN InSenderAddr varchar(100) NULL DEFAULT NULL;
ALTER TABLE RMQ  ADD COLUMN InSenderAddr varchar(100) NULL DEFAULT NULL;
ALTER TABLE SMQ  ADD COLUMN InSenderAddr varchar(100) NULL DEFAULT NULL;
ALTER TABLE WDQ  ADD COLUMN InSenderAddr varchar(100) NULL DEFAULT NULL;
ALTER TABLE QDQ  ADD COLUMN InSenderAddr varchar(100) NULL DEFAULT NULL;
ALTER TABLE RDQ  ADD COLUMN InSenderAddr varchar(100) NULL DEFAULT NULL;
ALTER TABLE SDQ  ADD COLUMN InSenderAddr varchar(100) NULL DEFAULT NULL;
ALTER TABLE IQDQ ADD COLUMN InSenderAddr varchar(100) NULL DEFAULT NULL;
ALTER TABLE CSMQ ADD COLUMN InSenderAddr varchar(100) NULL DEFAULT NULL;

-- PMQ daily tables (PMQ_yyyyMMdd_0N) created before the upgrade: new ones get the column from the code.
-- This prints one ALTER per existing table; run its output.
SELECT CONCAT('ALTER TABLE `', table_name, '` ADD COLUMN InSenderAddr varchar(100) NULL DEFAULT NULL;')
  FROM information_schema.columns c0
 WHERE table_schema = DATABASE() AND table_name LIKE 'PMQ\_%' AND column_name = 'ID'
   AND NOT EXISTS (SELECT 1 FROM information_schema.columns c1
                    WHERE c1.table_schema = c0.table_schema AND c1.table_name = c0.table_name AND c1.column_name = 'InSenderAddr');

-- Check: every table has the column (expect one row per table above, plus the PMQ daily tables).
-- SELECT table_name FROM information_schema.columns WHERE table_schema = DATABASE() AND column_name = 'InSenderAddr';

-- Rollback (set DB.InSenderAddrColumn=false first, or drop and send RELOAD_ROUTING: the code stops writing it):
-- ALTER TABLE RMQ DROP COLUMN InSenderAddr;   -- and the same for the other tables
