# Core 第 7 批优化设计：DB 兜底同步执行、长短信默认 Redis 合并、过期扫描放到 Core

## 1. 问题

### 1.1 路由失败的兜底：交给线程池就确认消费

`SyncSubmitRouter` 有 6 处在调用 `dbBackup.putMsg(message)` 之后立即返回 true，stream 随即确认消费：

- 路由失败
- rssid 为空
- RA2P 无效
- 需要租号（租号分支已停用）
- 长短信组装失败

`DBBackupHandler` 是一个内存线程池，真正的处理 `handleMessageError` 要等池里的线程执行：写 OUT_SUBMIT CDR，然后按重试策略写 RMQ/SMQ，或者给客户回失败 DR。

| 情况 | 后果 |
|---|---|
| 消息进池后、执行前，Core 崩溃 | 消息丢失：没有 CDR，客户收不到任何结果 |
| 在池里排队超时 | `timeout` 按 COMMUNICATION_ERROR 处理，原来的失败原因被改写 |
| 第 3 批的 `db` 标记在"进池"时就写下 | 进池后、确认消费前崩溃：重投时看到"已进池"就跳过，但池里的那份已经丢了 |

### 1.2 长短信默认在内存中合并

`CsmReassembleMode` 默认是 `legacy`。分段交给 `csmHandler` 线程池，在内存缓存里合并，交出去就确认消费。

- 可靠性：Core 重启、分片换节点、分段分散到不同 Core 处理，这三种情况都会丢失已经收到的分段。
- 性能：每个分段同步 INSERT 一次 CSMQ 表（长短信都带 UDH，走同步 `PreparedStatement`），每条长短信第一次出现时还要 SELECT 一次 CSMQ，收齐后再 DELETE 一次。这些数据库操作都在一把全局锁 `synchronized (sync)` 里执行，所有长短信是串行处理的。

`redis` 模式（`RedisCsmReassembler`）已经实现，但存在以下问题：

| # | 问题 | 后果 |
|---|---|---|
| C1 | 过期扫描 `CsmExpiryTask` 只在 MQM 的任务配置里配了才会运行，仓库中没有这项配置 | 一直没收齐的长短信，分段在 Redis 里静默过期，客户收不到任何 DR |
| C2 | `CsmExpiryTask` 的顺序是"从索引领走 → 删除分段 → 交给 MQM 的内存发送队列" | 中途崩溃，过期 DR 就丢了 |
| C3 | 收齐后删除分段。这时如果另一个分段被重投（它当初已经存入，但确认消费失败或 Core 崩溃），会新建一个只有 1 段的合并缓冲 | 1 小时后给一条其实已经发出的长短信回过期 DR |
| C4 | 最后一段正在组装外发时，另一个分段被重投：哈希仍是集齐状态，会再组装一次 | 同一条长短信外发两次 |

## 2. 方案

### 2.1 DB 兜底同步执行

`SyncSubmitRouter` 的 6 处兜底改为调用 `dbFallback(message)`，规则如下：

- `CoreDbFallbackInline=true`（默认）：在当前 worker 线程上直接执行 `handleMessageError`，执行完才返回 true，然后确认消费。
- 执行时有数据库写入失败或抛出异常：返回 false，不确认消费，留给自动重投。
  - 只有同步写库的失败能被发现（带 UDH 的消息、或异步 SQL 队列满了改走同步写）。
  - 失败的那次写入，对应的去重标记会被撤销（`StreamAttempt.forget`），重投时会重新写入。
  - CDR、失败 DR 这些已经成功的副作用，由第 3 批和第 6 批的检查点跳过。
- `CoreDbFallbackInline=false`：恢复原来的线程池方式。
- `SyncSubmitRouter` 以外的调用（旧的 TCP 入口、DeliveryRouterThread 等）不变，仍然进线程池。

同步执行后，`DBBackupHandler.putMsg` 外层的 `db` 标记就不需要了，由内部各个检查点去重。

还有一个很小的窗口：MT 的去重标记在 submitMq 上，失败 DR 写在 reportMq 上，两者不在同一段 Lua 里。这些标记要等到 `finish` 时才写入，如果恰好在"失败 DR 已写出、`finish` 还没写标记"的几毫秒内崩溃，重投时会再回一次 DR、再写一次 CDR。这是"至少一次"，比原来直接丢失要好；如果标记已经写入、只是确认消费失败，重投时不会重复（测试 B7-A2）。

> RMQ/RDQ 等不带 UDH 的写库，原本就走 `AsyncSqlWriter` 的内存批量队列（`AsyncSqlWriterEnable`，默认 true），和其它所有写库共用，不在本批范围内。本批保证的是：CDR 和失败 DR 在确认消费之前已经写出，失败 DR 是写到 Redis stream 里的。

### 2.2 长短信默认改为 redis 模式

`CsmReassembleMode` 的默认值从 `legacy` 改为 `redis`，`legacy` 仍然可以配置。Core 的 MT 路径和 MQM 重发路径两处都随之改变。

Redis 里的 key（都在 state Redis 上）：

| key | 类型 | 内容 | TTL |
|---|---|---|---|
| `csm:{ossid}:{sender}:{recipient}:{ref}:{total}` | hash | 分段号 → 序列化后的分段 | 过期时间 + 600 秒 |
| `csm:ids:{同上}` | hash | 分段号 → msgId（**新增**） | 同上 |
| `csm:asm:{同上}` | string | 正在组装外发的占用：owner\|时间（**新增**） | 同上 |
| `csm:done:{同上}` | hash | 已完成的组：分段号 → msgId（**新增**） | 同上 |
| `csm:index` | zset | 合并缓冲 key → 到期时间（毫秒） | — |

存分段改为一段 Lua `csmStore`，依次判断：

1. **已完成**：`csm:done` 存在，并且本分段号对应的 msgId 与本次相同，说明是已完成的组里某个分段被重投 → 返回 `DONE`，调用方只补写 IN_SUBMIT CDR（受去重检查点控制），然后确认消费（解决 C3）。
   - msgId 不同，说明客户复用了同一个参考号发送了一条新的长短信 → 删除旧的完成记录，按新消息处理。不会误吞新消息。
2. **存入分段**：HSET 分段和 msgId。如果是这组的第一个分段，就设置 TTL 并登记到过期索引。
3. **没收齐**：返回 `WAIT`，调用方确认消费。
4. **收齐了**：检查组装占用 `csm:asm`。
   - 另一个消费者持有占用，并且还在 `CoreMtLeaseMs`（默认 60 秒）以内 → 返回 `BUSY`，调用方不确认消费，稍后由自动重投再处理（解决 C4）。
   - 否则写入自己的占用，返回所有分段，由调用方组装外发。

外发成功后，调用 `csmFinish` 收尾（一段 Lua）：删除分段 hash，从过期索引移除，删除占用，把 `csm:ids` 改名为 `csm:done`（保留 TTL）。

最后一段的组装外发如果中途崩溃：占用过期后，这个最后分段会被自动重投，重新组装；第 3 批的外发标记保证不会重复外发。

剩下两种极少见的情况可能重复外发一次：
- 最后一段崩溃后，先被重投的是同组的**另一个**分段（它的去重标记里没有外发记录）。
- 外发成功后收尾失败（Redis 出错）。这时组仍然是集齐状态，占用过期后如果有分段被重投，会再外发一次。没有分段被重投的话，到期时由过期扫描收尾，不回 DR。

### 2.3 过期扫描放到 Core

新增 `CsmExpirySweeper`，在 Core 启动时运行（`CoreCsmExpirySweep`，默认 true），仅在 redis 模式下工作。

每 `CoreCsmExpiryIntervalSec`（默认 60）秒执行一次：

1. **领取**（一段 Lua）：从 `csm:index` 取出最多 `CoreCsmExpiryBatch`（默认 200）个已到期的 key，把它们的分数改为"现在 + `CoreCsmExpiryLeaseMs`（默认 300000，即 5 分钟）"。其它 Core 在这 5 分钟内看不到这些 key，所以多个 Core 同时运行也不会重复处理。
2. **逐组处理**：
   - 分段已经不存在：从索引移除。
   - 分段数已经收齐：外发已经完成，只是没来得及收尾。执行 `csmFinish`，不发 DR。
   - 没收齐：用已收到的分段构造过期 DR。客户支持过期 DR 时状态为 EXPIRED，否则为 UNDELIVERABLE。只有当某个分段要求了 DR 时才回 DR，这和 legacy 中 `DeliveryReport=1` 的筛选条件一致。DR 按分段拆开，同步写入 `stream:dr:pending`（走第 2 批的同步 DR 发送）。成功后执行 `csmFinish`。
   - 发送失败（有写库失败或抛出异常）：不收尾，5 分钟后被重新领取。
3. 中途崩溃：已领取但还没收尾的 key，5 分钟后会被任意一个 Core 重新领取，继续处理。如果崩溃恰好发生在 DR 已写出、还没收尾的那一刻，这组的过期 DR 会重发一次。选择"至少一次"，是为了不丢过期 DR。

MQM 的 `CsmExpiryTask` 保留，并改用同样的领取和收尾逻辑。两边可以同时运行，不会重复处理。

### 2.4 开关和配置项

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `CoreDbFallbackInline` | true | 2.1 的开关。false 时恢复使用线程池 |
| `CsmReassembleMode` | **redis**（原为 legacy） | 长短信合并方式 |
| `CoreCsmExpirySweep` | true | Core 是否执行长短信过期扫描 |
| `CoreCsmExpiryIntervalSec` | 60 | 扫描间隔（秒） |
| `CoreCsmExpiryBatch` | 200 | 每次领取的组数上限 |
| `CoreCsmExpiryLeaseMs` | 300000 | 领取后的占用时间（毫秒） |

以上配置都在 GmmsConfig 中，支持热加载。过期时间沿用 `CsmIntegrityCacheTimeout`，默认 1 小时，从第一个分段到达时开始计算。

## 3. 性能

- **DB 兜底**：只在失败分支执行，量很小。数据库慢时只会拖住所在的那个分片 stream。
- **长短信合并**：每个分段 1 次 Lua（原来要同步写 1 次数据库，并持有全局锁），收齐后再 1 次 Lua 收尾。比原来多出的只有两个小 key（`csm:ids` 和 `csm:asm`）。
- **过期扫描**：每个 Core 每分钟 1 次领取 Lua，耗时与客户数量无关；只有到期的组才会产生后续操作。

## 4. 不在本批范围

- `AsyncSqlWriter`、CDR 写文件这两个内存缓冲，在 kill -9 时会丢失最后一批数据（R5，单独评估）。
- 客户复用参考号，同时（未完成时）发送两条同 key 的长短信，分段会互相覆盖。legacy 模式下同样存在这个问题。
