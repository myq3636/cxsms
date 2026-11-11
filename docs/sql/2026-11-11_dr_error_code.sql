-- DR error codes (docs/DR错误码设计.md): two columns on every table that keeps messages.
--   VendorErrorCode  the vendor DR / failed submit as the channel sent it, e.g. stat=UNDELIV;err=5;nec=3:001B
--   DrErrorCode      the internal DR error code, 3 digits (conf/drErrorCode.cfg)
-- Run before (or after) the upgrade; the code finds the columns by itself (GmmsConfig DB.DrErrorCodeColumns=auto)
-- and only writes them once every table has them. After running this on a node that is up: RELOAD_DRERROR.
-- MySQL 8.0.12+: add ", ALGORITHM=INSTANT" to each ALTER for no table copy; 5.7 copies the table (run off-peak).
-- A table that does not exist in a DB is skipped by the code; leave it out here too.

ALTER TABLE messagestore ADD COLUMN VendorErrorCode varchar(64) NULL DEFAULT NULL, ADD COLUMN DrErrorCode varchar(3) NULL DEFAULT NULL;
ALTER TABLE PMQ  ADD COLUMN VendorErrorCode varchar(64) NULL DEFAULT NULL, ADD COLUMN DrErrorCode varchar(3) NULL DEFAULT NULL;
ALTER TABLE RMQ  ADD COLUMN VendorErrorCode varchar(64) NULL DEFAULT NULL, ADD COLUMN DrErrorCode varchar(3) NULL DEFAULT NULL;
ALTER TABLE SMQ  ADD COLUMN VendorErrorCode varchar(64) NULL DEFAULT NULL, ADD COLUMN DrErrorCode varchar(3) NULL DEFAULT NULL;
ALTER TABLE WDQ  ADD COLUMN VendorErrorCode varchar(64) NULL DEFAULT NULL, ADD COLUMN DrErrorCode varchar(3) NULL DEFAULT NULL;
ALTER TABLE QDQ  ADD COLUMN VendorErrorCode varchar(64) NULL DEFAULT NULL, ADD COLUMN DrErrorCode varchar(3) NULL DEFAULT NULL;
ALTER TABLE RDQ  ADD COLUMN VendorErrorCode varchar(64) NULL DEFAULT NULL, ADD COLUMN DrErrorCode varchar(3) NULL DEFAULT NULL;
ALTER TABLE SDQ  ADD COLUMN VendorErrorCode varchar(64) NULL DEFAULT NULL, ADD COLUMN DrErrorCode varchar(3) NULL DEFAULT NULL;
ALTER TABLE IQDQ ADD COLUMN VendorErrorCode varchar(64) NULL DEFAULT NULL, ADD COLUMN DrErrorCode varchar(3) NULL DEFAULT NULL;
ALTER TABLE CSMQ ADD COLUMN VendorErrorCode varchar(64) NULL DEFAULT NULL, ADD COLUMN DrErrorCode varchar(3) NULL DEFAULT NULL;

-- PMQ daily tables (PMQ_yyyyMMdd_0N) created before the upgrade: new ones get the columns from the code.
-- This prints one ALTER per existing table; run its output.
SELECT CONCAT('ALTER TABLE `', table_name, '` ADD COLUMN VendorErrorCode varchar(64) NULL DEFAULT NULL, ADD COLUMN DrErrorCode varchar(3) NULL DEFAULT NULL;')
  FROM information_schema.columns c0
 WHERE table_schema = DATABASE() AND table_name LIKE 'PMQ\_%' AND column_name = 'ID'
   AND NOT EXISTS (SELECT 1 FROM information_schema.columns c1
                    WHERE c1.table_schema = c0.table_schema AND c1.table_name = c0.table_name AND c1.column_name = 'DrErrorCode');

-- Rollback (the code stops writing the columns by itself when they are gone and RELOAD_DRERROR is sent,
-- or set DB.DrErrorCodeColumns=false first):
-- ALTER TABLE RDQ DROP COLUMN VendorErrorCode, DROP COLUMN DrErrorCode;   -- and the same for the other tables
