# StreamAttempt 按批处理设计（方案 a）

> 日期：2026-11-17。依据《Redis CPU瓶颈分析》第 10 节的方案 a。要求：**正常消息的处理结果不变**；只有在系统或 Redis 出现异常时（Core 崩溃、Redis 报错、处理超时）行为才可能不同，这是可以接受的。本文是改代码前的设计，还没有改代码。

## 1. 现状

### 1.1 谁在用

| 消费者 | stream | 标记 | `_done` | 现在每个条目的 Redis 调用 |
|---|---|---|---|---|
| `MTStreamConsumer` | `stream:mt:pending:*`（submit MQ） | `core:mt:{msgId}`（`begin`） | 不写 | CLAIM（Lua）→ 处理（副作用标记随写 `mt:routed` 的 Lua 一起写）→ FLUSH（只在还有没写的标记时）→ ACK（Lua） |
| `ResultStreamConsumer` | `stream:core:results*`（report MQ） | `core:att:{stream}:{id}`（`beginEntry`） | 写 | CLAIM → 处理 → FLUSH（写 `_done`，Lua）→ `afterDone` 动作 → ACK |
| `InboundDRStreamConsumer` | `stream:core:inbound-dr:*`（report MQ） | 同上 | 写 | 同上 |

`CoreMQMStreamConsumer` 不用 StreamAttempt，不在这次范围内。

8388（report MQ）上每条 MT 约有 CLAIM 5.3 次、FLUSH 5.4 次、ACK 5.3 次（《Redis CPU瓶颈分析》3.3 节）；8387 上 MT 的 CLAIM、ACK 各约 1.2 次。

### 1.2 StreamAttempt 的结构

- `claim()`：`CLAIM_LUA`（`_done` → DONE；租约新鲜且不是自己 → BUSY；否则写租约，返回全部已记录的字段 → RUN），然后把 attempt 放进 ThreadLocal `current`；
- 处理代码里的副作用都通过 `current` 走：`once` / `forget`（CDR、DR、写库、计数）、`markDoneNow`（改道等，马上写）、`pendingFlags` / `produced` / `currentMarkerOn`（写 stream 的 Lua 顺带写标记）、`afterDone`（`_done` 写入之后才做的清理，例如删除 DR 查找记录）；
- `finish()`：`flush(done)`（`FLUSH_LUA`：HSET 剩下的标记，含 `_done`）→ 执行 `afterDone` → `ACK_LUA`（XACK、XDEL、删除标记）；
- `abandon()`：`flush(releaseLease)`（HSET 已做的标记 + HDEL `_lease`）；
- `entryGoneAfterBegin()`：读出来已经超过自动认领空闲时间的条目，开始后再确认一次它还在 stream 里（XRANGE）；
- `ackDone()`：DONE 的条目只确认。

## 2. 改动

### 2.1 总体流程（每个消费者的批循环）

```
messages = consumeBatch(...)                              // 不变
claims  = StreamAttempt.claimBatch(markers, redis, owner) // 一次 Lua，替代 N 次 CLAIM
for each entry (按 stream 顺序，不变):
    claim BUSY  -> 留在 pending（同现在）
    claim DONE  -> 记入"待确认"（现在是马上 ackDone）
    claim RUN   -> 必要时续租（2.3）-> bind(claim) 放进 ThreadLocal
                   -> 处理（不变，副作用代码一行都不改）
                   -> 成功: finishEntry()：FLUSH（普通命令，2.4）+ afterDone，记入"待确认"
                   -> 失败: abandon()（同现在，只影响这一个条目）
ackBatch(待确认的条目)                                      // 一次 Lua，替代 N 次 ACK
```

- 处理顺序、副作用、写入的标记和它们的时机都不变；**变的只有三类 Redis 调用的打包方式**，以及条目被确认的时间（推迟到这一批处理完，约几到几十毫秒）。
- 自动认领（`RedisStreamAutoClaimRunner`）一条一条地交给消费者，量很小，**继续用现在的逐条流程**（`begin` / `finish`），不改。

### 2.2 批量 CLAIM（`StreamAttempt.claimBatch`）

- 新 Lua `CLAIM_BATCH_LUA`：KEYS = 这一批的标记 key，ARGV = owner、nowMs、leaseMs、ttl；对每个 key 执行和 `CLAIM_LUA` **完全相同的判断**，返回数组：`{状态, 字段1, 值1, ...}`。
- `claimBatch` 返回每个条目的 `Claimed`（状态、标记 key、已记录的字段、claim 的时间），**不放进 ThreadLocal**；处理到这个条目时 `bind(claimed)` 才放进去（和现在 `claim()` 之后的状态一样）。
- **同一批里标记 key 重复**（MT 的标记按 msgId，正常情况下一批里不会有两个相同的 msgId）：只有第一个参加批量 CLAIM，后面重复的条目等这一批确认之后，按现在的逐条流程单独处理。这样它们看到的标记和现在完全一样（前一个确认时标记已删除）。
- 日志：RUN 且已有记录时的 `Redelivered, skipping side effects already done`、DONE 时的 `Redelivered entry already processed completely, only acked` 照旧逐条打印。

### 2.3 租约

现在每个条目在开始处理前才拿租约；批量以后，**租约从这一批 CLAIM 时算起**，排在后面的条目开始处理时租约已经过了一段时间。

- 规则：处理每个条目前，如果离 claim 已经超过 `CoreMtLeaseMs / 2`（默认 30 秒），先对这个条目单独再 CLAIM 一次（现在的 `claim()`：同一个 owner 会刷新租约），按新的状态处理（BUSY 跳过、DONE 只确认）。
- 正常一批 100 条在几毫秒到几百毫秒内处理完，用不到续租；只有 Redis 或下游很慢时才会走到。

### 2.4 FLUSH 改为普通命令（`flush()`）

| 场合 | 现在 | 改为 |
|---|---|---|
| `finish` / `finishEntry`（成功，可能带 `_done`） | `FLUSH_LUA`：HSET 字段 | **一条 `HSET key f1 v1 f2 v2 ...`** |
| `markDoneNow`（马上写一个标记） | `FLUSH_LUA` | 一条 `HSET` |
| `abandon`（失败，释放租约） | `FLUSH_LUA`：HSET + HDEL `_lease` | pipeline：`HSET`（有字段时）+ `HDEL _lease`，顺序不变 |

- `FLUSH_LUA` 只有这两步、没有条件判断，不需要原子性：HSET 成功、HDEL 失败时，租约要等过期（60 秒）别的消费者才能接手，和现在整段 Lua 失败的结果相同。
- 写入的字段、时机和现在一样：**`_done` 仍然在每个条目处理完时写**，不等整批（这是方案 a 和方案 b 的区别）。

### 2.5 批量 ACK（`StreamAttempt.ackBatch`）

- 新 Lua `ACK_BATCH_LUA`：KEYS = stream + N 个标记 key，ARGV = group + N 个条目 id；对每个条目执行和 `ACK_LUA` 相同的操作（XACK；确认成功则 XDEL；删除标记），返回每个条目的 XACK 结果。
- 调用方按条目逐个记 `traceAck`（`[STREAM-ACK]` 日志）、确认失败的打 WARN，和现在一样。
- DONE 的条目也放进这一批确认（现在是 `ackDone` 单独一次）。
- 没有 attempt 的情况（开关关闭）仍然走现在的 `ackById`，不变。

### 2.6 ThreadLocal 和 `afterDone`

- `bind(claimed)`：创建 `StreamAttempt` 对象并放进 `current`，之后处理代码里所有 `StreamAttempt.current()` 的调用和现在完全一样；
- `finishEntry()`：`flush(false, trackDone)`（2.4），执行这个条目的 `afterDone` 动作，`current.remove()`，返回要确认的 (id, 标记 key)。`afterDone` 仍然在这个条目的 `_done` 写入之后马上执行，不等整批；
- 处理抛异常、返回失败：`abandon()`，和现在一样，只影响这一个条目。

### 2.7 开关

新增 GmmsConfig `CoreStreamAttemptBatch`（默认 `true`，热加载），false 时三个消费者回到现在的逐条流程。按要求写入《配置项说明》，并加入 `GmmsUtility` 的热加载白名单。

## 3. 正常和异常时的行为对比

| 情况 | 现在 | 改后 |
|---|---|---|
| 正常处理 | 每个条目 CLAIM → 处理 → FLUSH → ACK | 处理和副作用完全相同；条目在这一批处理完才确认（晚几到几十毫秒），下游不受影响 |
| 被别的消费者占着（BUSY） | 留在 pending | 相同 |
| 之前已处理完、只是没确认（DONE） | 马上单独确认 | 在这一批末尾和别的条目一起确认 |
| 处理失败 | abandon，留给重投 | 相同（只影响这一个条目） |
| **Core 在一批中间崩溃** | 正在处理的那一个条目重投；它没来得及写的标记对应的副作用可能重复 | 正在处理的条目：相同。这一批里**已处理完、还没确认**的条目也会重投：结果、通道 DR 已写 `_done`，重投只确认；MT 不写 `_done`，但它的副作用（写 `mt:routed`、CDR、计数）都有标记，重投不重复 |
| **批量 CLAIM 时 Redis 报错** | 只有这个条目这一次没处理 | 这一批都没处理，留在 pending，由自动认领（空闲 60 秒后）接手：**这一批延迟约 1 分钟** |
| **批量 ACK 时 Redis 报错** | 这个条目没确认，自动认领后只确认 | 这一批都没确认，自动认领后：结果、DR 只确认（`_done`），MT 走重投（有标记，不重复） |
| 一批处理很慢（Redis 或下游很慢） | 每个条目自己的租约 | 超过租约一半时单独续租（2.3）；不续租的话，另一个消费者可能并发处理同一个条目 |

即：**正常消息不受影响；只有 Core 崩溃、Redis 报错、处理特别慢时，影响的范围从"一个条目"变成"一批条目"**，结果仍然是不丢、不重复（MT 依靠已有的副作用标记），只是可能多延迟一个自动认领周期。

## 4. 改动清单

| 文件 | 改动 |
|---|---|
| `StreamAttempt` | 新增 `claimBatch`、`Claimed`、`bind`、`finishEntry`、`ackBatch`、`CLAIM_BATCH_LUA`、`ACK_BATCH_LUA`；`flush()` 改为 HSET / pipeline；`batchEnabled()`（开关）。现在的 `begin`、`beginEntry`、`finish`、`abandon`、`ackDone` 保留（自动认领、开关关闭、重复标记的条目继续用） |
| `MTStreamConsumer`、`ResultStreamConsumer`、`InboundDRStreamConsumer` | `FetchAndProcessTask` 的批循环按 2.1 改；`processMessage` / `processResult` 拆出"已经 claim 过"的处理部分，逐条流程和批量流程共用 |
| `RedisConnection` | 如需要，加 `hset(key, Map)`（或直接用 `runPipeline`） |
| `GmmsUtility`、`docs/配置项说明.md` | `CoreStreamAttemptBatch` |

处理业务的代码（`InternalCoreEngineSession`、`MessageStoreManager`、`CDRManager` 等里的 `once`、`markDoneNow`、`afterDone`）**不改**。

## 5. 测试

| 测试 | 内容 |
|---|---|
| 已有：Batch6IT | 重投、并发认领、`_done` 之后确认失败只确认 |
| 已有：CoreFailoverIT、GracefulStopIT、ShardZeroIT | 压力下杀 Core、换节点：每条 MT 只处理一次 |
| 已有：Batch4IT、CoreDrFlowIT、CoreDrBizIT、CoreRetryIT | 死信、DR 流程、重试 |
| 新增 | 一批里同时有 RUN、BUSY、DONE 的条目；同一批里重复的标记；处理中途杀 Core（一批里已处理完没确认的条目重投后不重复 CDR、DR、写库）；批量 CLAIM / ACK 时 Redis 报错；处理时间超过租约一半时续租；开关关闭回到逐条流程；Redis 调用次数（每批 1 次 CLAIM、1 次 ACK，FLUSH 是 HSET） |
| 性能 | 1000 TPS × 60 秒每条 MT 的 Redis 主线程 CPU（《Redis CPU瓶颈分析》第 2 节的方法），预期 8388 从约 630 µs 降到约 430～480 µs；3 分钟 5000 TPS 看 Core 处理 MT 的速度 |
| 全量回归 | 73 个套件和基线比较 |

## 6. 收益估算

| 实例 | 每条 MT 的 Lua（现在 → 改后） | 增加的普通命令 |
|---|---|---|
| 8388 report | CLAIM 5.3 + FLUSH 5.4 + ACK 5.3 ≈ 16 → 约 0.3（每批 2 次） | HSET 约 5.4 |
| 8387 submit | CLAIM 1.2 + ACK 1.2 ≈ 2.4 → 约 0.05 | — |

按《Redis CPU瓶颈分析》第 8 节的单次开销（每次 EVAL 约 13 µs + 约 4 µs 网络读写，普通 HSET 约 1.3 µs + 4 µs）估算，8388 每条 MT 少约 150～200 µs（25%～30%），8387 少约 30～40 µs。这是估算，需要实测。

## 7. 实施和实测（2026-11-17～18）

### 7.1 实施中发现的问题

1. **按读出时的 id 确认**：处理过程中，写 stream 会把新条目的 id 设到同一个消息对象上（MT 写进 `stream:mt:routed` 后，`msg.getRedisStreamID()` 变成 routed 的 id）。第一版的批量确认用的是确认时的 id，XACK 返回 0，而同一段 Lua 照样删了标记，Core 重启后条目被重投、当成新消息又发了一次（CoreFailoverIT 1500 条里重复 492 条）。改为 `Batch` 在认领时按对象记下读出时的 id，之后都用它；AttemptBatchIT 加了 AB-06。
2. **Batch4IT FAIR-02 的余量变小**：前面的 SCAN 检查直接删掉 2 万个 stream，没有从 `idx:streams` 去掉，MT 消费者启动时重按门铃要逐个检查它们。测试改为删 stream 时同时 `SREM`（真实情况下全量扫描会清掉），FAIR-02 回到 104 ms、大 stream 还剩 1400 条。

### 7.2 回归

全量回归 74 个套件（新增 AttemptBatchIT 23 项）：1996 项通过，唯一失败的 FAIR-02 见 7.1 第 2 项，改测试后单独重跑通过。

### 7.3 性能

**1000 TPS × 60 秒**，每条 MT 的 Redis 主线程 CPU（`used_cpu_*_main_thread`，扣除空闲基线；"之前"是 11-17 15:50 的测量，那时还没打开共享收包池）：

| 实例 | 主线程 CPU（之前 → 之后） | 每条 MT 的 Lua | 每条 MT 的命令 |
|---|---|---|---|
| 8386 | 195 → 240 µs（+23%） | 2.5 → 2.5 | 18.7 → 18.4 |
| 8387 | 513 → 476 µs（−7%） | 10.5 → 8.6 | 55.0 → 52.2 |
| 8388 | 632 → 546 µs（−14%） | 17.1 → **8.4** | 75.4 → 66.2 |

8388 的 Lua 减半，CPU 降 14%，比估计的 25%～30% 少：1000 TPS 时条目陆续到达，每批只有几条，按批省下的有限。8386 没有用到 StreamAttempt，它的上升要另外核实（基线是在打开共享收包池之前测的）。

**5000 TPS × 3 分钟**（和打开共享收包池后、按批之前的 19:02 那次比）：

| | 按批之前 | 按批之后 |
|---|---|---|
| 客户收到的 DR | 32.3 万 | **42.3 万（+31%）** |
| 通道回 DR → 客户收到，中位数 / p95 | 263 ms / 14.3 s | **101 ms / 6.1 s** |
| 结束时没收到 DR 的 MT（共 90 万） | 56.4 万 | 51.8 万 |
| 负载中 `dr:pending`、`core:inbound-dr`、`core:results` | — | 几十到几百条（DR 那一路跟得上） |
| `mt:pending` | 积压 | 每秒增加约 2800 条：Core 每秒处理约 3200 段 MT |
| Redis CPU 8387 / 8388 | 95% 以上 | 92%～97% / 86%～94% |

**结论**：通道 DR 和结果这一路不再积压，DR 更快送达；**剩下的瓶颈是 Core 处理 MT（8387）**。客户提交 → 通道收到的中位数这次是 60 秒以上（上次 0.57 秒），两次的采样方式不同（上次没有采 stream 积压），需要用同样的采样重测按批之前的版本再比较。
