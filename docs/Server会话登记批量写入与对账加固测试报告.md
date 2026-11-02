# Server 会话登记批量写入与对账加固：测试用例与执行报告

> 设计见《Server会话登记批量写入与对账加固设计》。

## 1. 测试环境

本地 Redis 7.2.5，JDK 17。测试进程内启动真实的 `MultiSmppServer`，客户端走真实 SMPP 连接。Redis 命令数用 `INFO commandstats` 统计，不靠推算。

## 2. 用例（`RegistryBatchIT`，模块 `MultiSmppServer1`）

| ID | 场景 | 预期 / 实测 |
|---|---|---|
| RB-00a～b | 启动时登记关闭，之后热加载打开 | 关闭时没有登记；打开后下一轮对账写入（之前根本不会创建对账任务） |
| RB-01a～d | 一次 bind | 1 个 MGET，加 pipeline 里的 2 个 SETEX、5 个 ZADD，没有单独的 GET；详情字段和有效期正常；module、active、conn、node、slot 索引都有；Core 的查询能找到 |
| RB-02 | 断开连接 | 登记被删除 |
| RB-03a～d | 55 条连接的对账 | 批大小 200：每轮 1 个 MGET、110 个 SETEX，没有单独的 GET；热加载改成 10：每轮 6 个 MGET；每轮都打印统计行 |
| RB-04 | 每 100ms 一个 enquire_link，兜底间隔 4 秒，对账每秒一轮 | PDU 路径完全不写 Redis |
| RB-05a～d | 停掉对账，删除登记，继续发 PDU | 下一个超过间隔的 PDU 恢复登记；3 秒 60 个 PDU、间隔 2 秒时只写 1 次；打 WARN |
| RB-05e | 同上，但连接没有 PDU | **已知限制**：对账停着时不恢复 |
| RB-05f | 对账超过 5 个间隔没有结束 | PDU 路径打 ERROR |
| RB-05g | 恢复对账 | 没有 PDU 的连接也恢复登记 |
| RB-06a～d | `CLIENT PAUSE` 让 Redis 超过 socket 超时 | 记录批量失败；对账继续；登记还在；没有客户连接断开；之后恢复正常 |
| RB-07a～b | 对账中抛 `OutOfMemoryError` | 打 ERROR，之后继续跑（之前定时任务会被永久取消） |
| RB-08a～c | 某个客户的连接读取失败 | 这一轮继续，跳过清理，其他客户照常刷新 |
| RB-09a | 恢复后的下一轮 | 残留的失效登记被删除 |
| RB-10a～b | 不在存活集合、但在本轮开始后写入的登记 | 保留（之前会被删除）；存活连接的登记被清空后，下一轮能恢复 |
| RB-11 | 955 条连接的对账耗时 | 135、171、190 ms（见第 4 节） |

本批新增用例都是单进程。`RB-05`、`RB-07`、`RB-08` 通过反射停止对账、替换内部集合来制造故障，产品代码里没有测试钩子。

## 3. 测试中发现并修复的问题

| 问题 | 处理 |
|---|---|
| 失败后的重试节流固定 30 秒，兜底间隔配得比 30 秒小时，新 bind 后要等够 30 秒才能自恢复（RB-05b 暴露） | 改为不超过兜底间隔 |
| 测试中连接数从 61 掉到 55 | 不是产品问题：`SMPPCUST30` 配了 `ConnectionSilentTime: 3`，3 秒静默就断开，测试改用其他客户 |
| 命令统计增量恒为 0 | 测试脚本的正则匹配到了 `rejected_calls`，修正后用例才真正生效 |
| 第一次测清理上限时三档结果一样 | 探针以 Core 模块身份运行，没有匹配到 Server 的 key，那组数据作废，改用 Server 模块身份重测 |

## 4. 性能实测

**对账耗时（本机）：**

| 方式 | 连接数 | 一轮耗时 | 每条连接 |
|---|---|---|---|
| 修改前（逐条同步） | 3731 | 约 30 秒 | 约 8 ms |
| 修改后（每批 200） | 955 | 135～190 ms | 约 0.2 ms |

连接数不同，只能看每条连接的耗时。按每条 0.2 ms 线性估算，20000 条连接一轮约 4 秒。没有实测 20000 条，见第 5 节。

**清理上限的影响**见设计文档第 6.1 节：2000、5000、10000 三档下，其他客户端的 PING p99 都在几毫秒内，5000 这一档一轮约 0.4 秒。

## 5. 不在本地范围的项

| ID | 项 | 建议做法 |
|---|---|---|
| X-RB1 | 20000 条连接的真实对账耗时 | 测试环境看日志 `reconcile finished ... costMs=` |
| X-RB2 | 线上 Redis 的命令量下降 | 对比上线前后 state Redis 的 `instantaneous_ops_per_sec`。客户普遍发 enquire_link 时应明显下降 |
| X-RB3 | 多模块 | 用例是单模块。客户连在多个模块时，确认 DR 仍发往有连接的模块 |
| X-RB4 | TLS 模块 | `SslSmppServer` 没有对账，靠兜底刷新续期，代码上已考虑，但本批没有测 |
| X-RB5 | portal 是否读取 `lastSeen` | 刷新频率从约 30 秒变为约 1 分钟 |

## 6. 回归结果

`run-all.sh` 从头全量编译后执行 33 个套件，全部通过。回归之后只改了文档，没有再改产品代码。

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
| Batch6IT | 22 | 22 | 0 |
| Batch7IT | 39 | 39 | 0 |
| ClientBatch1IT | 19 | 19 | 0 |
| ClientBatch2IT | 12 | 12 | 0 |
| ServerBatch1IT | 16 | 16 | 0 |
| ServerBatch2IT | 21 | 21 | 0 |
| ServerBatch3IT | 10 | 10 | 0 |
| ShardGroupIT | 8 | 8 | 0 |
| LogChainCoreIT | 12 | 12 | 0 |
| LogChainClientIT | 11 | 11 | 0 |
| DrLivenessIT | 9 | 9 | 0 |
| GmmsReloadIT | 23 | 23 | 0 |
| ConfigFileIT | 15 | 15 | 0 |
| **RegistryBatchIT（新增）** | 32 | 32 | 0 |
| IncrementalRoutingIT | 28 | 28 | 0 |
| RenameReloadCore | 30 | 30 | 0 |
| RenameReloadServer | 30 | 30 | 0 |
| RenameReloadClient | 10 | 10 | 0 |
| RapidReloadCore | 21 | 21 | 0 |
| RapidReloadServer | 6 | 6 | 0 |
| RapidReloadClient | 4 | 4 | 0 |
| **合计** | **643** | **643** | **0** |
