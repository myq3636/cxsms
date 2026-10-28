# Core 第 4 批：测试用例与执行报告

> 设计见《Core第4批优化设计》。范围：
> - A 重投次数上限与死信队列
> - B 用 SCAN 代替 KEYS
> - C 消费线程公平性与分发线程不被卡住
> - D 分片配置校验
> - E 停机时把 CDR 写完
> - 新增配置的热加载
>
> 同时对之前所有批次做全量回归。第 1 到 4 节是用例设计，第 5 节是执行结果。

## 1. 测试环境

与《Core通道限速放行测试用例与执行报告》第 1 节相同：本地 Redis 7.2.5、JDK 17、真实的 Core 初始化、没有数据库、由测试程序扮演 Client 和 Server。

新增测试套件 `Batch4IT`。为了让测试在秒级完成，测试里缩短了下列参数：

| 参数 | 测试值 | 线上默认值 |
|---|---|---|
| `RedisStreamMaxDeliveries` | 3 | 10 |
| `RedisStreamAutoClaimIdleMs` | 300 毫秒 | 60 秒 |
| `RedisStreamAutoClaimIntervalMs` | 700 毫秒 | 5 分钟 |
| `RedisStreamConsumer.CoreMT.*` | 1 个工作线程，队列 20，每批 50 条 | — |
| `CoreStreamTaskMaxBatches` | 2 | 10 |

`ConfigReloadIT` 增加了本批 3 个新配置的热加载检查。

## 2. 用例

### 2.1 A：重投次数上限与死信队列

| ID | 场景 | 步骤 / 前置 | 预期 | 套件：检查 | 级别 |
|---|---|---|---|---|---|
| DLQ-01 | 没到上限时照常领回 | 一条 MT 被读出后不确认消费，调用 `recoverPending` 两次 | 两次都被领回（第 2、3 次投递） | Batch4IT：`DLQ-01` | P0 |
| DLQ-02 | 达到上限后进死信队列 | 再调用一次（已投递 3 次，上限 3） | 不再领回，写入 `stream:dlq:submit` | Batch4IT：`DLQ-02` | P0 |
| DLQ-03 | 死信记录的内容 | 读出死信条目 | 包含 reason=max_deliveries、来源 stream、来源消费组、投递次数和原始消息体，消息体能还原出原 msgId | Batch4IT：`DLQ-03` | P0 |
| DLQ-04 | 从原 stream 移除 | 查原 stream 的长度和待确认列表 | 两者都为 0 | Batch4IT：`DLQ-04` | P0 |
| DLQ-05 | 告警日志 | 查日志 | 有 `[STREAM-DLQ] moved to stream:dlq:submit after 3 deliveries. msgId=...` | Batch4IT：`DLQ-05` | P1 |
| DLQ-06 | 上限为 0 表示不限次数 | `RedisStreamMaxDeliveries=0`，重投 6 次 | 每次都被领回，不进死信队列（原来的行为） | Batch4IT：`DLQ-06` | P1 |
| DLQ-07 | 真实消费者加自动重投 | 一条每次处理都会失败的供应商 DR，启动真实的 `InboundDRStreamConsumer` | 投递 3 次后，由自动重投线程移入 `stream:dlq:report`，入向 DR stream 被清空 | Batch4IT：`DLQ-07` | P0 |
| DLQ-08 | 不回失败 DR | 同上 | `stream:dr:pending:*` 里没有任何 DR | Batch4IT：`DLQ-08` | P0 |

### 2.2 B：SCAN 代替 KEYS

| ID | 场景 | 步骤 / 前置 | 预期 | 套件：检查 | 级别 |
|---|---|---|---|---|---|
| SCAN-01 | 能列全 | 先写入 2 万个 stream，再调用 `listResponsibleStreams` | 返回全部 2 万个 | Batch4IT：`SCAN-01` | P0 |
| SCAN-02 | 不再使用 KEYS | 执行前清零 Redis 命令统计，执行后查看 | 统计里有 `scan`，没有 `keys` | Batch4IT：`SCAN-02` | P0 |

### 2.3 C：消费线程公平性与分发线程不被卡住

| ID | 场景 | 步骤 / 前置 | 预期 | 套件：检查 | 级别 |
|---|---|---|---|---|---|
| FAIR-01、FAIR-02 | 积压大的 stream 不独占线程 | 只有 1 个工作线程；先往一个 stream 写 1500 条，再往另一个 stream 写 1 条 | 小 stream 很快被处理（实测约 1 秒），此时大 stream 还剩 1400 条左右 | Batch4IT：`FAIR-01`、`FAIR-02` | P0 |
| FAIR-03 | 让出后还会接着处理 | 同上 | 大 stream 最终全部处理完（每次让出都会重新挂门铃） | Batch4IT：`FAIR-03` | P0 |
| FAIR-04、FAIR-05 | 线程池满时不出错 | 1 个工作线程、队列 20，同时有 60 个 stream | 全部被处理，分发线程没有报错 | Batch4IT：`FAIR-04`、`FAIR-05` | P1 |
| FAIR-06 | **分发线程不再自己执行任务** | 先停掉消费者，写好 30 个 stream、每个 100 条（共 3000 条），再启动，线程池必然饱和 | 全部处理完；**没有一条 MT 是在分发线程上处理的** | Batch4IT：`FAIR-06a`、`FAIR-06b` | P0 |

### 2.4 D：分片配置校验

| ID | 场景 | 步骤 / 前置 | 预期 | 套件：检查 | 级别 |
|---|---|---|---|---|---|
| SHARD-01 | 各模块 TotalShards 不一致 | server 登记 TotalShards=1，其他节点登记 16 | ERROR，列出各节点的值 | Batch4IT：`SHARD-01` | P0 |
| SHARD-02 | core 节点之间分片重叠 | 两个 core 节点都负责 6、7 号分片 | WARN，列出重叠的分片和节点 | Batch4IT：`SHARD-02` | P1 |
| SHARD-03 | client 没有覆盖全部分片 | client 节点只负责 0 到 7 号分片 | ERROR，列出 8 到 15 号分片无人消费 | Batch4IT：`SHARD-03` | P0 |
| SHARD-04 | 日志 | 查日志 | 出现 `[SHARD-CHECK]` | Batch4IT：`SHARD-04` | P1 |
| SHARD-05 | 配置正确时不报错 | 改成一致、不重叠、全覆盖 | 没有任何问题 | Batch4IT：`SHARD-05` | P0 |
| SHARD-06 | 节点登记自己的分片 | 调用 `publish` | 写入 `system:shards:core:{模块}:{节点}` = `2|0,1`，并带过期时间 | Batch4IT：`SHARD-06` | P1 |

### 2.5 E：停机时把 CDR 写完

| ID | 场景 | 步骤 / 前置 | 预期 | 套件：检查 | 级别 |
|---|---|---|---|---|---|
| CDR-01 | 前置条件 | 停掉后台写 CDR 的线程，再写入 50 条 CDR | 50 条都留在队列里 | Batch4IT：`CDR-01` | — |
| CDR-02 | 收到停机事件后写完 | 触发 `ShutdownEvent` | 50 条全部写进 CDR 文件，队列清空 | Batch4IT：`CDR-02` | P0 |

### 2.6 配置热加载

| ID | 场景 | 预期 | 套件：检查 | 级别 |
|---|---|---|---|---|
| CFG-01（补充） | `RedisStreamMaxDeliveries`、`CoreStreamTaskMaxBatches`、`CoreStreamTaskMaxMs` 热加载 | 新值生效 | ConfigReloadIT：`CFG-01`（新增 3 项） | P1 |

## 3. 回归

前几批全部套件：Batch1Check、InlineDrCheck、DrShardCheck、CoreDrFlowIT、Batch3IT、CoreE2EIT、ChannelRateIT、StartupIT、DoorbellRaceIT、ConfigReloadIT。

## 4. 不在本地范围的项

| ID | 项 | 建议做法 |
|---|---|---|
| X-41 | Server 和 Client 的自动重投进死信队列（共用 `RedisStreamAutoClaimRunner`） | 测试环境里让 Client 外发某条消息一直失败，确认它进入 `stream:dlq:submit`，并且 Core 的 outbox 在 3 分钟后回 2700 |
| X-42 | 多节点同时运行时的分片校验 | 测试环境故意让两个 Core 的 `CoreShard` 重叠，确认双方的日志都出现 `[SHARD-CHECK] WARN` |
| X-43 | 通过真实 stop 命令停机时 CDR 写完 | 发送高峰期执行 stop，对比确认消费的条数和 CDR 文件里的条数 |
| X-44 | 死信消息的人工处理 | 目前只停放、告警，没有重放工具；按需另行开发 |

## 5. 执行结果

执行时间 2026-10-28 20:14（AEDT）。本地 Redis 7.2.5，OpenJDK 17.0.21 (Temurin)。`run-all.sh` 从头全量编译后依次执行 11 个套件；`ConfigReloadIT` 加入 3 个新配置后单独复跑。

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
| ConfigReloadIT | 32 | 32 | 0 |
| **Batch4IT（新增）** | 25 | 25 | 0 |
| **合计** | **210** | **210** | **0** |

本批所有用例（DLQ-01 到 DLQ-08、SCAN-01 和 SCAN-02、FAIR-01 到 FAIR-06、SHARD-01 到 SHARD-06、CDR-01 和 CDR-02、CFG-01 补充）全部通过。

### 5.1 用例有效性验证（FAIR-06）

FAIR-04 只能证明"线程池满时最终都处理完了"，旧代码（CallerRunsPolicy，任务由分发线程执行）也能满足，所以区分不出新旧行为。为此新增了 FAIR-06，并用旧代码做了对照：

| 代码 | 第一版 FAIR-06 | 修正后的 FAIR-06 |
|---|---|---|
| 新代码（AbortPolicy，把 stream 放回队列） | 0 条在分发线程上处理，PASS | 0 条，PASS |
| 旧代码（CallerRunsPolicy） | 0 条，PASS，**用例无效** | **500 条**在分发线程上处理，**FAIL**，用例有效 |

第一版用例无效有两个原因：
1. 消费者是一边写入一边消费的，线程池并没有真正饱和。
2. 开启 MT 处理去重后，确认消费走 `MtAttempt.finish`（Lua），不会打印 `STREAM-ACK` 日志，所以统计的日志行数永远是 0。

修正方法：先停掉消费者、写好 30 个积压的 stream 再启动；改为统计每条消息必然打印的处理日志行。
