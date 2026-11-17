-- MySQL 5.5 -> 8.0 (docs/MySQL5.5升级8.0操作手册.md): the queue tables as InnoDB and the indexes of the MQM queries.
-- Run on the 8.0 database after the import (the dump already has ENGINE=InnoDB; the ALTER TABLE ... ENGINE lines
-- are for a database converted in place). Each ALTER rebuilds the table: on a big table it takes a while and
-- blocks the writes to that table, run it with a2p stopped.

USE gmms;

-- only when a table is still MyISAM (SELECT TABLE_NAME, ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA='gmms'):
-- ALTER TABLE cdr ENGINE=InnoDB;
-- ALTER TABLE csmq ENGINE=InnoDB;
-- ALTER TABLE iqdq ENGINE=InnoDB;
-- ALTER TABLE pmq ENGINE=InnoDB;
-- ALTER TABLE qdq ENGINE=InnoDB;
-- ALTER TABLE rdq ENGINE=InnoDB;
-- ALTER TABLE rdq_copy ENGINE=InnoDB;
-- ALTER TABLE rmq ENGINE=InnoDB;
-- ALTER TABLE sdq ENGINE=InnoDB;
-- ALTER TABLE smq ENGINE=InnoDB;
-- ALTER TABLE tmp_cdr ENGINE=InnoDB;
-- ALTER TABLE wdq ENGINE=InnoDB;

-- RDQ ReSendDRTask (every 10 s): ... WHERE ActionCode != 0 AND now() > NextRetryTime ... ORDER BY DateIn LIMIT 500
ALTER TABLE rdq ADD INDEX idx_NextRetryTime (NextRetryTime);
-- RMQ resend tasks: ... WHERE ActionCode != 0 AND now() > NextRetryTime ... ORDER BY DateIn LIMIT 500
ALTER TABLE rmq ADD INDEX idx_NextRetryTime (NextRetryTime);
-- SMQ SendStoredMessageTask / SendROPFailedMessageTask: ... (NextRetryTime is null OR now() > NextRetryTime) ...
ALTER TABLE smq ADD INDEX idx_NextRetryTime (NextRetryTime);
-- WDQ ExpiredMessageTask (every 60 s): ... AND (now >= ExpiryDate) AND (... < ExpiryDate) ORDER BY DateIn LIMIT 50
ALTER TABLE wdq ADD INDEX idx_ExpiryDate (ExpiryDate);

-- rollback of the indexes:
-- ALTER TABLE rdq DROP INDEX idx_NextRetryTime;
-- ALTER TABLE rmq DROP INDEX idx_NextRetryTime;
-- ALTER TABLE smq DROP INDEX idx_NextRetryTime;
-- ALTER TABLE wdq DROP INDEX idx_ExpiryDate;
