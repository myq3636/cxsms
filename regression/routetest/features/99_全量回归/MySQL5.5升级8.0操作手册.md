# MySQL 5.5 升级 8.0 操作手册

> 日期：2026-11-17。在测试服务器 8.219.93.186 上按本手册做过一遍（MySQL 5.5.56 → 8.0.40，gmms 库 14 张表），第 6 节是那次的实测记录。目的、原因见《DB积压性能测试设计》第 5 节：队列表是 MyISAM（表级锁），MQM 的定时查询没有索引、每次全表扫描，表大了以后写入被挡住。

## 1. 结论和改了什么

| 项 | 5.5（原来） | 8.0（之后） |
|---|---|---|
| 版本 | 5.5.56，`/usr/local/mysql`，`/etc/my.cnf` | 8.0.40，`/usr/local/mysql-8.0.40`（链接 `/usr/local/mysql8`），`/etc/my8.cnf` |
| 数据目录 | `/usr/local/mysql/data/` | `/usr/local/mysql8-data/`（5.5 的数据目录原样保留，用于回退） |
| 端口、socket | 3306、`/tmp/mysql.sock` | 不变（应用的 `DS_gmms_URL` 不用改） |
| 引擎 | 12 张 MyISAM、2 张 InnoDB | 全部 InnoDB（行级锁） |
| 索引 | 只有 `ID`、`InMsgID`、`OutMsgID` | 另加 `rdq(NextRetryTime)`、`rmq(NextRetryTime)`、`smq(NextRetryTime)`、`wdq(ExpiryDate)`（`docs/sql/2026-11-17_mysql8_innodb_indexes.sql`） |
| 用户 | `root@'%'`，旧格式密码 | `root@'%'`、`root@localhost`，`mysql_native_password`，**沿用 5.5 的密码哈希**（不用知道明文） |
| 应用 | — | 不用改配置。驱动已经是 `mysql-connector-j-8.0.33`。代码只改了一处：每天新建的 PMQ 分表从 `ENGINE=MyISAM` 改成 `InnoDB`（`MessageStoreManager.createPMQTable`） |

## 2. 兼容性检查（升级前要确认的）

| 项 | 检查方法 | 本系统的情况 |
|---|---|---|
| `lower_case_table_names` | 5.5：`SELECT @@lower_case_table_names` | **是 1**。代码用 `RDQ`、`WDQ` 访问表 `rdq`、`wdq`。**8.0 只能在初始化数据目录时设定，之后不能改**：`/etc/my8.cnf` 里必须先写好再 `--initialize` |
| `sql_mode` | `SELECT @@sql_mode` | 5.5 是空的。8.0 默认是严格模式（`STRICT_TRANS_TABLES`、`NO_ZERO_DATE`、`ONLY_FULL_GROUP_BY` 等），写入不合规的值会报错。为了和原来一致设成 `NO_ENGINE_SUBSTITUTION`（不严格）。以后要收紧的话，先在测试环境打开并跑全部用例 |
| 认证插件 | `SELECT user, host, plugin FROM mysql.user` | 8.0 默认 `caching_sha2_password`：5.5 自带的 mysql 客户端（运维脚本、测试脚本在用）连不上。用 `mysql_native_password` |
| JDBC 驱动 | `ls Gmms/WEB-INF/lib \| grep mysql` | `mysql-connector-j-8.0.33.jar`，支持 8.0；`DS_ccb_Driver` 还写着旧的 `com.mysql.jdbc.Driver`，8.0.33 里它仍然可用（打一条弃用 WARN） |
| 字符集 | `SELECT table_name, table_collation FROM information_schema.tables WHERE table_schema='gmms'` | 表都是 `utf8`（8.0 里叫 `utf8mb3`），导入后保持不变。新库的默认改为 `utf8mb4` |
| SQL 语法 | 在代码里搜 `0000-00-00`、`GROUP BY`、`TYPE=`、`PASSWORD(`、`SQL_CALC_FOUND_ROWS` | 都没有 |
| 存储过程、触发器 | `information_schema.routines`、`triggers` | 都没有 |
| 建表语句 | 代码里搜 `ENGINE=` | `createPMQTable` 写死了 `ENGINE=MyISAM`，已改成 `InnoDB` |
| 连接数 | `SELECT @@max_connections`、`SHOW STATUS LIKE 'Max_used_connections'` | 151、历史最高 26（每个模块 `DS_gmms_MaxPoolSize=3`）；8.0 配 500 |
| 磁盘 | `df -h` | 安装包 0.9 GB、解压 2.5 GB，数据（导入后）约导出文件的 2～3 倍 |

## 3. 准备（不停服务）

```bash
# 1. 安装包（glibc 2.17 版本适用于 CentOS 7）
cd /usr/local
curl -sSL -o mysql-8.0.40-linux-glibc2.17-x86_64.tar.xz \
  https://dev.mysql.com/get/Downloads/MySQL-8.0/mysql-8.0.40-linux-glibc2.17-x86_64.tar.xz
tar xJf mysql-8.0.40-linux-glibc2.17-x86_64.tar.xz
mv mysql-8.0.40-linux-glibc2.17-x86_64 mysql-8.0.40
ln -sfn /usr/local/mysql-8.0.40 /usr/local/mysql8
/usr/local/mysql8/bin/mysqld --version          # Ver 8.0.40
```

2. 写 `/etc/my8.cnf`（先用临时端口 3308，和 5.5 同时运行）：

```ini
[client]
port = 3308
socket = /tmp/mysql8.sock
default-character-set = utf8mb4

[mysqld]
basedir = /usr/local/mysql8
datadir = /usr/local/mysql8-data
port = 3308
socket = /tmp/mysql8.sock
pid-file = /usr/local/mysql8-data/mysqld.pid
log-error = /usr/local/mysql8-data/mysqld.err
lower_case_table_names = 1              # 必须在初始化前写好，以后不能改
sql_mode = NO_ENGINE_SUBSTITUTION       # 和 5.5 一样不严格
character-set-server = utf8mb4
collation-server = utf8mb4_general_ci
default_authentication_plugin = mysql_native_password
skip-name-resolve
mysqlx = OFF
max_connections = 500
max_allowed_packet = 16M
default-storage-engine = InnoDB
innodb_buffer_pool_size = 4G            # 按机器内存调整，专用机可以到内存的 50%～70%
innodb_redo_log_capacity = 1G
innodb_flush_log_at_trx_commit = 1
innodb_file_per_table = 1
log-bin = mysql-bin
binlog_format = ROW
server-id = 1
binlog_expire_logs_seconds = 604800     # binlog 保留 7 天，避免占满磁盘
log_timestamps = SYSTEM

[mysql]
no-auto-rehash
```

3. 初始化并启动（3308）：

```bash
mkdir -p /usr/local/mysql8-data
/usr/local/mysql8/bin/mysqld --defaults-file=/etc/my8.cnf --initialize-insecure --user=root
nohup /usr/local/mysql8/bin/mysqld_safe --defaults-file=/etc/my8.cnf --user=root > /dev/null 2>&1 &
/usr/local/mysql8/bin/mysql --defaults-file=/etc/my8.cnf -uroot -e \
  "SELECT @@version, @@lower_case_table_names, @@sql_mode, @@default_storage_engine"
# 8.0.40  1  NO_ENGINE_SUBSTITUTION  InnoDB
```

4. 用户：沿用 5.5 的密码哈希（`$PW` 是 `DS_gmms_Password`，只在命令里用，不要打印）：

```bash
H55=$(MYSQL_PWD="$PW" /usr/local/mysql/bin/mysql -h<IP> -P3306 -uroot -N \
  -e "SELECT password FROM mysql.user WHERE user='root' AND host='%'")
/usr/local/mysql8/bin/mysql --defaults-file=/etc/my8.cnf -uroot -e "
  CREATE USER 'root'@'%' IDENTIFIED WITH mysql_native_password AS '$H55';
  GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
  ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password AS '$H55';
  FLUSH PRIVILEGES;"
# 5.5 自带的客户端能连上 8.0：
MYSQL_PWD="$PW" /usr/local/mysql/bin/mysql -h<IP> -P3308 -uroot -e "SELECT @@version"
```

## 4. 切换（要停 a2p）

停服务的时间 = 导出 + 导入 + 建索引 + 重启，测试机上 51 MB 的库不到 1 分钟；线上按库的大小估（先在测试机上用线上大小的数据量练一次）。

```bash
# 1. 停 a2p 的所有模块（确认没有 -Dmodule= 进程了）
/usr/local/a2p/bin/a2pStop.sh CoreEngine1 CoreEngine2 CoreEngine3 MultiSmppClient1 MultiSmppClient2 \
  MsgQueueMonitor1 MsgQueueMonitor2 MultiSmppServer1 MultiSmppServer2
ps -ef | grep -c "[D]module="      # 0

# 2. 从 5.5 导出（锁全部表，保证一致）
B=/usr/local/a2p/backconf/$(date +%Y%m%d)_mysql55; mkdir -p $B
MYSQL_PWD="$PW" /usr/local/mysql/bin/mysqldump -h<IP> -P3306 -uroot --databases gmms \
  --lock-all-tables --routines --triggers --hex-blob --default-character-set=utf8 > $B/gmms_mysql55.sql

# 3. 改成 InnoDB 后导入 8.0
sed -e 's/ENGINE=MyISAM/ENGINE=InnoDB/g' $B/gmms_mysql55.sql > $B/gmms_innodb.sql
MYSQL_PWD="$PW" /usr/local/mysql8/bin/mysql -h<IP> -P3308 -uroot --default-character-set=utf8 < $B/gmms_innodb.sql

# 4. 加索引
MYSQL_PWD="$PW" /usr/local/mysql8/bin/mysql -h<IP> -P3308 -uroot < docs/sql/2026-11-17_mysql8_innodb_indexes.sql

# 5. 核对：每张表 5.5 和 8.0 的行数一样，8.0 都是 InnoDB
for t in $(MYSQL_PWD="$PW" /usr/local/mysql/bin/mysql -h<IP> -P3306 -uroot -N -e "SHOW TABLES FROM gmms"); do
  a=$(MYSQL_PWD="$PW" /usr/local/mysql/bin/mysql -h<IP> -P3306 -uroot -N -e "SELECT COUNT(*) FROM gmms.$t")
  b=$(MYSQL_PWD="$PW" /usr/local/mysql8/bin/mysql -h<IP> -P3308 -uroot -N -e "SELECT COUNT(*) FROM gmms.$t")
  echo "$t $a $b $([ "$a" = "$b" ] && echo OK || echo DIFF)"
done

# 6. 停 5.5、8.0 换到 3306 和 /tmp/mysql.sock，再启动
MYSQL_PWD="$PW" /usr/local/mysql/bin/mysqladmin -h<IP> -P3306 -uroot shutdown
MYSQL_PWD="$PW" /usr/local/mysql8/bin/mysqladmin -h<IP> -P3308 -uroot shutdown
cp -p /etc/my8.cnf /etc/my8.cnf.migration
sed -i 's/^port = 3308/port = 3306/; s#^socket = /tmp/mysql8.sock#socket = /tmp/mysql.sock#' /etc/my8.cnf
nohup /usr/local/mysql8/bin/mysqld_safe --defaults-file=/etc/my8.cnf --user=root > /dev/null 2>&1 &
MYSQL_PWD="$PW" /usr/local/mysql/bin/mysql -h<IP> -P3306 -uroot -e "SELECT @@version, @@port"

# 7. 部署 a2p.jar（含 PMQ 分表用 InnoDB 的修改）后启动 a2p
/usr/local/a2p/bin/a2pStart.sh CoreEngine1 CoreEngine2 CoreEngine3 MultiSmppClient1 MultiSmppClient2 \
  MsgQueueMonitor1 MsgQueueMonitor2 MultiSmppServer1 MultiSmppServer2
```

**以后启动 MySQL 用 8.0 的命令**（`/etc/my.cnf` 仍是 5.5 的配置，留着回退用；不要再用 5.5 的启动命令，否则两个都抢 3306）：

```bash
nohup /usr/local/mysql8/bin/mysqld_safe --defaults-file=/etc/my8.cnf --user=root > /dev/null 2>&1 &
```

## 5. 切换后的检查

| 检查 | 命令 / 位置 | 应该是 |
|---|---|---|
| 版本、端口 | `SELECT @@version, @@port` | `8.0.40`、`3306` |
| 引擎 | `SELECT TABLE_NAME, ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA='gmms'` | 全部 InnoDB |
| 索引用上了 | `EXPLAIN SELECT * FROM WDQ WHERE ActionCode != 0 AND DeliveryReport = 1 AND Split < 2 AND (now() >= ExpiryDate) ORDER BY DateIn LIMIT 50` | `type=range`、`key=idx_ExpiryDate`（rdq 的行很少、大多到期时优化器会选全表扫描，属正常） |
| 应用连上了 | Core、MQM、Server 的日志 | 没有 `Communications link failure`、`Access denied`、`Unknown column`；Core 有 `[MT-REROUTE] DB has the RoutingLevel column: written and read` |
| MQM 定时任务 | MQM 日志 `taskExe:RMQ`、`taskExe:RDQ` | 照常查询，没有 `Error in executing` |
| 消息 | 服务器用例 RC001、RC010（SMQ）、RC014（RMQ）、DE001，DB 积压 PF001 | 全部 PASS；PF001 的表锁等待、慢语句比 5.5 时少 |
| 错误日志 | `/usr/local/mysql8-data/mysqld.err` | 没有 `[ERROR]` |

## 6. 回退（切换后发现问题时）

5.5 的数据目录和 `/etc/my.cnf` 都没动，切换之后写进 8.0 的数据不会回到 5.5（要保留的话先从 8.0 导出再导回 5.5，表的 ENGINE 改回 MyISAM 或保持 InnoDB 都可以）。

```bash
/usr/local/a2p/bin/a2pStop.sh ...（同 4.1）
MYSQL_PWD="$PW" /usr/local/mysql8/bin/mysqladmin -h<IP> -P3306 -uroot shutdown
cd /usr/local/mysql && nohup bin/mysqld_safe --defaults-file=/etc/my.cnf --user=root > /dev/null 2>&1 &
# a2p.jar：备份的 a2p.jar.20261117b 是切换前的版本（PMQ 分表用 InnoDB 的修改对 5.5 也没有坏处，可以不回退）
/usr/local/a2p/bin/a2pStart.sh ...
```

## 7. 测试服务器上的实测（2026-11-17）

| 步骤 | 结果 |
|---|---|
| 下载、解压、初始化 | 8.0.40，`lower_case_table_names=1`、`sql_mode=NO_ENGINE_SUBSTITUTION`、默认引擎 InnoDB |
| 用户 | `root@'%'`、`root@localhost` 用 5.5 的密码哈希；5.5 的客户端能连 8.0 |
| 导出 | 1 秒，51 MB（12 张 MyISAM、2 张 InnoDB） |
| 导入 | 5 秒；14 张表行数和 5.5 一致（rdq 1614、wdq 3207、rmq_back 65785、temp_rmq 13478、rdq_copy 11692，其余为空），全部 InnoDB |
| 索引 | wdq、rmq 的 MQM 查询走 `range`（`idx_ExpiryDate`、`idx_NextRetryTime`）；每条 DR 的 `DELETE FROM WDQ WHERE OutMsgID=` 走 `OutMsgID` |
| 切换 | 停 5.5、8.0 改到 3306 重启；部署新 a2p.jar、启动 a2p，各模块连 DB 正常 |
| 用例 | 见下面的“切换后的用例结果” |

切换前还发现：5.5 时 PF001 的 B 轮（rdq 50 万行）里，MQM 的 rdq 查询有 20 次因为 JDBC `socketTimeout=10000` 失败（`Communications link failure`，08:22～08:31），即查询被表锁卡住超过 10 秒，MQM 那一轮就没有取到要重发的 DR。

**切换后的用例结果**（MySQL 8.0.40、InnoDB、加了索引，新 a2p.jar）：

| 用例 | 结果 |
|---|---|
| RC001 路由 | PASS 7/7 |
| RC010 ROP 失败模式（进 SMQ、MQM 重发） | PASS，等待期间 `smq` 里有这条 |
| RC014 级别用完重试主路由（进 RMQ） | PASS 3/3，`rmq` 的 `RSsID`、`RoutingLevel` 正确 |
| DE001 DR 错误码 | PASS 10/10 |
| PF001 DB 积压（wdq、rdq 各 50 万行前后） | PASS，见下表 |
| 全量回归（72 个套件） | 1955 项通过（BindRateIT 单独跑），基线已写出（`features/99_全量回归/baseline.txt`） |

PF001 在 5.5（MyISAM、没有索引）和 8.0（InnoDB、加了索引）上的对比：

| | 5.5：A → B | 8.0：A → B |
|---|---|---|
| 客户提交 → 客户收到 DR，p95 | 706 → 719 ms | 711 → 717 ms |
| 通道回 DR → 客户收到，p95 | 134 → 139 ms | 134 → 136 ms |
| MySQL `Table_locks_waited` | 30 → **493** | 0 → **0** |
| 0.2 秒以上的慢语句 | 0 → **115** | 0 → **2** |
| 同时等锁的语句（最多） | 0 → 2 | 0 → 0 |
| MQM 查 rdq 超时（`Communications link failure`） | B 轮 20 次 | 0 |

8.0 的 B 轮和全量回归同时运行（机器更忙），延迟仍和 A 一样；表锁等待没有了，慢语句从 115 条降到 2 条。
