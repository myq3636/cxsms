# Core 第 5 批：测试用例与执行报告（出向结果 stream 分片）

> 设计见《Core第5批优化设计》。前四批与通道限速全部套件同时做回归。第 1 到 3 节是用例设计，第 4 节是执行结果。

## 1. 测试环境

与之前的报告相同：本地 Redis 7.2.5、JDK 17、真实的 Core 初始化、没有数据库。`TotalShards=2`。

新增测试套件 `Batch5IT`；`ConfigReloadIT` 增加了 `CoreResultStreamSharded` 的热加载检查。

## 2. 用例

| ID | 场景 | 步骤 / 前置 | 预期 | 套件：检查 | 级别 |
|---|---|---|---|---|---|
| R5-01a | submit_resp 的 stream 名 | rssid=8 | `stream:core:results:8:{0或1}` | Batch5IT | P0 |
| R5-01b | DR_resp 的 stream 名 | ossid=8888 | `stream:core:results:8888:{0或1}` | Batch5IT | P0 |
| R5-01c | 没有 rssid | rssid=-1 | 改用 ossid | Batch5IT | P1 |
| R5-01d | 拆分短信 | 同一个 msgId 的第 1 段和第 3 段 | 落在同一个 stream | Batch5IT | P0 |
| R5-01e | 分片分布 | 同一个通道 50 个不同的 msgId | 分布到 2 个分片 stream | Batch5IT | P1 |
| R5-01f | 没有 msgId | 只有 inMsgID | 用 inMsgID 计算分片 | Batch5IT | P2 |
| R5-02 | 开关回退 | `CoreResultStreamSharded=false` | 写回旧的 `stream:core:results` | Batch5IT | P0 |
| R5-03 | 新旧并存 | 旧 stream 里 3 条结果、分片 stream 里 3 条 submit_resp 加 1 条 DR_resp | 全部被处理，各自写 OUT_SUBMIT CDR | Batch5IT：`R5-03a`、`R5-03b` | P0 |
| R5-04 | 并行处理 | 先按旧方式写 1200 条结果，再按分片方式写 1200 条（2 个通道 + 2 个客户） | 分片方式由更多线程同时处理，总耗时更短 | Batch5IT：`R5-04a`、`R5-04b` | P0 |
| R5-05 | 公平性 | 通道 8 积压 3000 条结果，客户 7001 有 1 条 | 客户 7001 的结果很快被处理，此时通道 8 仍有积压；积压最终处理完 | Batch5IT：`R5-05a` 到 `R5-05c` | P0 |
| R5-06 | 启动时重新挂门铃 | 写 5 条结果到分片 stream，删掉门铃，再启动消费者 | 用 SCAN 找到这些 stream 并处理完 | Batch5IT：`R5-06a` 到 `R5-06c` | P0 |
| R5-07 | 自动重投覆盖分片 stream | 一条结果被已挂掉的 Core 读走，没有确认消费 | 由自动重投接手并处理 | Batch5IT：`R5-07` | P0 |
| R5-09 | 日志流向 | 查看分片 stream 的写入日志 | `flow=result->core` | Batch5IT：`R5-09` | P2 |
| CFG-01（补充） | 开关热加载 | 热加载把 `CoreResultStreamSharded` 改成 false | 新值生效 | ConfigReloadIT | P1 |
| 回归 | outbox 超时结果、迟到结果的丢弃、DR 全链路 | 前几批全部套件 | 全部通过；超时结果写入分片 stream 后仍按原规则处理 | Batch3IT、CoreE2EIT 等 | P0 |

## 3. 不在本地范围的项

| ID | 项 | 建议做法 |
|---|---|---|
| X-51 | 多个 Core 同时处理结果 | 测试环境部署 2 个以上 Core，压测时观察每个 Core 都有结果处理线程在工作，结果 stream 的积压明显低于改动前 |
| X-52 | 滚动升级的顺序 | 如果不是全新部署：先升级 Core；或者先在 Server、Client 上把 `CoreResultStreamSharded` 设为 false，等 Core 升级后再打开 |
| X-53 | 供应商 DR 先于 submit_resp 到达的情况是否减少 | 压测时对比改动前后 SDQ 的写入量（`TMP-DRLOOKUP inbound DR redis miss` 日志的条数） |

## 4. 执行结果

执行时间 2026-10-28 20:40（AEDT）；Redis 7.2.5、OpenJDK 17.0.21；`run-all.sh` 从头全量编译后执行 12 个套件。

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
| ConfigReloadIT | 34 | 34 | 0 |
| Batch4IT | 25 | 25 | 0 |
| **Batch5IT（新增）** | 19 | 19 | 0 |
| **合计** | **231** | **231** | **0** |

### 4.1 关键数据

| 指标 | 旧的单个 stream | 分片 stream |
|---|---|---|
| 处理 1200 条结果的耗时（两次执行） | 10.55 秒 / 11.07 秒 | 4.62 秒 / 5.78 秒 |
| 参与处理的线程数 | 5 | 10 |
| 通道积压时，另一个客户的结果多久被处理 | — | 111 毫秒（此时通道 8 仍有约 930 条积压） |

说明：

- 旧方式下也出现了 5 个线程，是因为第 4 批让任务处理一定量后让出 stream，同一个 stream 会**轮流**由不同线程接手，但同一时刻只有一个线程在处理它。分片后，多个 stream 是**同时**处理的，所以耗时缩短。
- 本地是单机加单个 Redis，倍数只有约 2 倍。线上可以并行的程度取决于 Core 节点数、结果工作线程数和有消息的 stream 数。

### 4.2 执行过程中的调整

第一次全量回归中，Batch3IT 有 4 项失败（E1 到 E4）。原因是测试代码只读取旧的 `stream:core:results`，而改动后超时结果写入了分片 stream。这是测试代码的问题，修改为读取所有 `stream:core:results*` 后通过。

同时发现 CoreE2EIT 的 E2E-06c 只检查旧 stream，结果为空所以总能通过，检查已经失去意义；同样改为检查所有结果 stream。
