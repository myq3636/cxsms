# DB 积压性能测试设计

> 日期：2026-11-17。测试服务器 8.219.93.186，用例 `regression/routetest/cases/PF001_db_backlog`，由 `bin/run_dbbacklog.sh` 运行。

## 1. 要回答的问题

线上发现：DB 的 `wdq`（等 DR）和 `rdq`（DR 重发）里各有几十万行时，整个消息流程变慢。要确认两件事：

1. 有了这么多行以后，MT 和 DR 是否真的变慢、慢在哪一段；
2. 原因是什么。

## 2. 读代码得到的推测（测试来验证）

| 事实 | 位置 |
|---|---|
| 两张表是 **MyISAM**：表级锁，一个 SELECT 执行期间，这张表上的 INSERT / UPDATE / DELETE 都要等 | 测试服务器 `SHOW TABLE STATUS` |
| 索引只有 `ID`、`InMsgID`、`OutMsgID` | `SHOW INDEX` |
| MQM 每 10 秒查 rdq：`WHERE ActionCode != 0 AND now() > NextRetryTime AND O_A2P IN (...) ORDER BY DateIn LIMIT 500`（两条） | `MessageStoreManager.getResendDR`，`MQMTaskConfig` `RDQ.ReSendDRTask.Interval=10` |
| MQM 每 60 秒查 wdq：`WHERE ActionCode != 0 AND DeliveryReport = 1 AND Split < 2 AND ... ORDER BY DateIn LIMIT 50`（两条） | `getExpiredMessage`，`WDQ.ExpiredMessageTask.Interval=60` |
| 这些条件和排序的列都没有索引：表大了以后每次都是全表扫描加排序 | |
| **每条供应商 DR** 都执行一次 `DELETE FROM WDQ WHERE OutMsgID=...`（不管这条 MT 在不在 wdq） | `handleOutDeliveryReportReq` 开头的 `clearMessage(message, WDQ)` |
| INSERT / UPDATE 走异步批量写库（`enqueueAsyncSql`），**DELETE 是同步的**，在处理 DR 的线程上执行 | `clearMessage` → `doDelete` |

推测：MQM 扫一次几十万行的表要几秒，MyISAM 表锁期间 Core 处理 DR 的线程卡在 DELETE 上，DR 回给客户变慢；异步写库线程也被挡住，写库队列积压。

## 3. 怎么测

### 3.1 流量（同一份配置跑两次）

测试工具的压测模式（同内存泄漏测试），50 TPS、6 分钟（前 1 分钟预热不计入），混合：

| 比例 | 路径 |
|---|---|
| 70% | 普通 MT + DR |
| 10% | 主路由通道 submit 失败，改道 |
| 10% | 3 段长短信 |
| 10% | 客户不回 deliver_sm_resp：DR 发送超时进 rdq，MQM 重发 |

每条 MT 记三段延迟：客户提交 → 通道收到（`latMt`）、通道回 DR → 客户收到（`latDr`）、客户提交 → 客户收到 DR（`latFull`），给出 p50 / p95 / p99 / max。

### 3.2 步骤（`bin/run_dbbacklog.sh`）

1. 删掉上次留下的测试行（`MsgID LIKE 'RTBL%'`）；
2. **A**：表里只有现有的数据，跑一次流量；
3. 往 wdq、rdq 各插 50 万行（`bin/dbbacklog_load.sh`）：复制一行现有数据，换掉 id，`ActionCode=1`，`NextRetryTime`、`ExpiryDate` 在 7 天后（MQM 会扫到但不会发出），`OSsID`、`RSsID` 9999（没有这个账户）；对 MQM 的查询做 EXPLAIN，量一次扫描的时间；
4. **B**：同样的流量再跑一次；
5. 删掉插入的行，恢复慢查询日志的设置。

每次流量期间：

- `bin/dbwatch.sh` 每 5 秒采 MySQL：`Questions`、`Slow_queries`、`Table_locks_waited`、正在等锁的语句数、最长的语句，以及等锁或执行超过 1 秒的语句全文；
- 临时打开慢查询日志（`long_query_time=0.2`），结束后按语句形状汇总（次数、总时间、最长）；
- 流量的中间对 3 个 Core 做 `jcmd Thread.print`，统计停在 MySQL 驱动里的线程、在执行哪个方法的 SQL。

### 3.3 判断

`report.txt` 最后一行：B 的 `latFull` 和 `latDr` 的 p95 都不超过 A 的 max(2 倍, +300 ms) 为 `RESULT: PASS`（没有变慢），否则 `RESULT: FAIL`（变慢）。报告同时列出原因的证据：表锁等待次数、B 里最慢的语句、等锁的语句、Core 线程停在哪个 SQL 上、EXPLAIN（`type=ALL` 全表扫描、`Using filesort`）和一次扫描的时间。

## 4. 输出

`cases/PF001_db_backlog/report/{时间}/`：`report.txt`（对比和结论）、`A/`、`B/`（每次的 `summary.txt`、`soak.txt`、`db.csv`、`processlist.log`、`slow.log`、`slow_top.txt`、`threads_*.txt`）、`explain.txt`、`load.txt`、`steps.txt`。每次流量的完整结果（含证据）还在用例的 `result/` 下。

## 5. 结果和原因

（执行后补充）
