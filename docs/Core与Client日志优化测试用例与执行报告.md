# Core 与 Client 日志优化：测试用例与执行报告

> 设计见《Core与Client日志优化设计》。同时对此前所有批次做了全量回归。

## 1. 测试环境

本地 Redis 7.2.5，JDK 17。Core 用例在测试进程里启动真实的 MT、结果、供应商 DR 消费者；Client 用例在测试进程里启动真实的 `MultiSmppServer` 当作供应商，再用真实的 `MultiSmppSession`（客户端模式）连接它，走真实的 SMPP 报文。

为本批新增的客户配置（cm.cfg）：

| 客户 | 配置 | 用途 |
|---|---|---|
| SMPPCUST42 | `IncomingConnection`（c42/p42） | 本进程 Server 上的"供应商账号" |
| SMPPVEND41 | `OutgoingConnection` 指向 127.0.0.1:17781，`BufferTimeout: 3` | Client 侧的 SMPP 供应商通道 |

## 2. 用例

### 2.1 Core（`LogChainCoreIT`）

| ID | 场景 | 预期 / 实测 | 级别 |
|---|---|---|---|
| L-C1 | 一条 MT 经真实消费者路由到通道 8 | 依次出现 `[STREAM-GET]` → `[CDR] type=IN_SUBMIT` → `[MT-ROUTE] ossid=8888 rssid=8` → `[STREAM-PUT]` → `[MT-TO-CLIENT] rssid=8` → `[STREAM-ACK]`；没有 WARN/ERROR | P0 |
| L-C2 | 该 MT 的 submit_resp | `[STREAM-GET]` → `[RESULT] ... outMsgId=OUT-LC1 outbox=FIRST` → `[CDR] type=OUT_SUBMIT` → `[STREAM-ACK]` | P0 |
| L-C3 | 供应商 DR | `[STREAM-GET]` → `[VDR-MATCH] outMsgId=OUT-LC1 ... -> msgId=8888_LC1 ossid=8888 (redis)` → `[CDR] type=OUT_DELI_REP` → `[DR-TO-SERVER]` → `[STREAM-ACK]`；没有 WARN（bind 注册表兜底为 INFO） | P0 |
| L-C4 | 匹配不到原消息的供应商 DR | 一行 INFO `[VDR-MATCH] ... not in redis, looking up the DB`；不再出现带短信内容的 `TMP-DRLOOKUP` WARN | P0 |
| L-C5 | 重复的 submit_resp | INFO `[RESULT-DROPPED]`；没有第二条 OUT_SUBMIT CDR | P1 |

### 2.2 Client（`LogChainClientIT`）

| ID | 场景 | 预期 / 实测 | 级别 |
|---|---|---|---|
| L-S0 | 供应商 41 的 Client 会话 bind 到本进程 Server | bind 成功 | — |
| L-S1 | 发一条 MT | `[MT-OUT] rssid=41 session=SMPPVEND41_c1_0 seq=1` → `[SUBMIT-RESP] seq=1 pduStatus=0 status=0 outMsgId=...` → `[STREAM-PUT] flow=result->core`；没有 WARN/ERROR，旧文本 `SubmitSM receivies response` 不再出现 | P0 |
| L-S2 | Server（供应商）发回 DR | `[VDR-IN] outMsgId=... status=10000 seq=1` → `[VDR-TO-CORE] ... deliver_sm_resp ROK`；DR 写入 `stream:core:inbound-dr:41`；不再出现 `Received DR,outmsgid` 和 `send response ok.` | P0 |
| L-S3 | 供应商只回 bind_resp，不回 submit_sm_resp（测试里的假供应商） | 3 秒后 WARN `[RESP-TIMEOUT] no response from session=SMPPVEND41_c1_2 seq=1 in time, result type=Submit Resp status=2700`；失败结果写给 Core | P0 |

## 3. 测试中发现并修复的问题

| 问题 | 处理 |
|---|---|
| L-S3 超时时还多打两行 INFO（`put message into exMsgQueue successfully.`、`SMPP pending submit timeout result produced`），与 `[RESP-TIMEOUT]` 重复 | 都改为 DEBUG；redis pending 模式下原来的 WARN 改为同样的 `[RESP-TIMEOUT]` |
| `CoreE2EIT` E2E-06a、`CoreDrFlowIT` 两项、`Batch6IT` B6-03 按旧日志文本断言 | 改为按新标签断言（`[RESULT-DROPPED]`、`[DR-TO-SERVER]`、`not in redis, looking up the DB`） |
| 测试代码：Client 用例第一版 bind 被拒 | cm.cfg 里供应商通道的账号与 Server 侧客户账号相同，共用的客户表把 c42 映射到了 41。改为在测试里设置 bind 账号 |
| 测试代码：Client 用例第一版 submit_sm 被 Server 限流（pduStatus=88） | 与 Server 批次的用例一样，测试开始时关闭 Server 限流 |

## 4. 不在本地范围的项

| ID | 项 | 建议做法 |
|---|---|---|
| X-L1 | 线上日志量 | 上线后对比一个节点每小时的 INFO / WARN 行数：每条消息多出约 2 到 4 行带标签的 INFO；每条 DR 的 WARN（bind 注册表兜底）和 `TMP-DRLOOKUP` 应基本消失 |
| X-L2 | 按旧文本 grep 的脚本、告警 | 按设计文档第 8 节的对照表修改 |
| X-L3 | 匹配不到 DR 时查 DB | 本地没有数据库，只验证了 `not in redis, looking up the DB` 一行；DB 查到时的 `[VDR-MATCH] ... (DB)`、查不到时的 `[VDR-STORE]` 需在测试环境确认 |
| X-L4 | HTTP 通道的 `[HTTP-OUT]` / `[HTTP-RESP]` | 在有 HTTP 供应商的测试环境确认一条消息的完整链路 |

## 5. 回归结果

执行时间 2026-10-30 15:39（AEDT）；`run-all.sh` 从头全量编译后执行 22 个套件。第一次执行中 Batch6IT 的 B6-03 因按旧日志文本断言失败，修改测试后单独重跑 Batch6IT 22 项全部通过。

| 套件 | 检查数 | 通过 | 失败 |
|---|---|---|---|
| Batch1Check | 10 | 10 | 0 |
| InlineDrCheck | 7 | 7 | 0 |
| DrShardCheck | 10 | 10 | 0 |
| CoreDrFlowIT | 26 | 26 | 0 |
| Batch3IT | 32 | 32 | 0 |
| CoreE2EIT | 23 | 23 | 0 |
| ChannelRateIT | 40 | 40 | 0 |
| StartupIT | 2 | 2 | 0 |
| DoorbellRaceIT | 3 | 3 | 0 |
| ConfigReloadIT | 68 | 68 | 0 |
| Batch4IT | 25 | 25 | 0 |
| Batch5IT | 19 | 19 | 0 |
| Batch6IT | 22 | 22 | 0（重跑） |
| Batch7IT | 39 | 39 | 0 |
| ClientBatch1IT | 19 | 19 | 0 |
| ClientBatch2IT | 12 | 12 | 0 |
| ServerBatch1IT | 16 | 16 | 0 |
| ServerBatch2IT | 21 | 21 | 0 |
| ServerBatch3IT | 10 | 10 | 0 |
| ShardGroupIT | 8 | 8 | 0 |
| **LogChainCoreIT（新增）** | 12 | 12 | 0 |
| **LogChainClientIT（新增）** | 11 | 11 | 0 |
| **合计** | **435** | **435** | **0** |
