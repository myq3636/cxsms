# DR 早到重试与 outbox 应答超时改进设计

背景：PF006（5 小时 4000 TPS）跑到 1 小时 28 分钟时，状态 Redis 8386 用到 90%，保护停止了压测。链条是：

1. 营销 MT 在 `mt:routed` 流里排了 5 分钟以上。
2. Core outbox 从 `createdAt` 开始算应答超时，这些 MT 一发出就"超过 180 秒"，被判 2700，真结果到了按迟到（`STALE`）丢弃。
3. 通道回来的真 DR 找不到原始消息，在早到 DR 延迟队列里试满 15 次。积压时每次"2 秒后重试"实际排了几十分钟，一条 DR 用了 1 小时，最后写进 SDQ 文件。
4. 积压时，正常 DR 也因为结果没处理而大量变成早到 DR，DR 入站积压到 293 万。DR 处理不掉，映射 key 也删不掉，状态 Redis 被撑爆。

另外，SDQ 文件没有程序导入数据库。这次的 63,872 条 DELIVRD DR 还留在 `queue/sdq/link/`，从来没有被匹配过。

本文共 5 项，第 2 项决定不做。

## 1. outbox 应答超时从发出时刻算，并按通道取值

**现状**：`CoreOutboundOutboxManager.checkTimeout` 对"已发出、等结果"（`waiting_result`）的条目，按 `now - createdAt ≥ CoreOutboundRespTimeoutMs`（180 秒）判 2700。`createdAt` 是 Core 放进 outbox 的时间，所以在流里排队的时间也被算进去了。所有通道都用同一个值，Client 实际等多久（cm 里每个通道的 `BufferTimeout`）不在考虑之内。

**改为**：

- 扫描器**第一次**看到某条处于 `waiting_result` 时，不判超时，而是把当前时间作为 `sentSeenAt` 写进它的 meta（`core:outbox:...:meta`，现在是 `stream\nid\ncreatedAt`，再追加一段）。然后推迟到 `sentSeenAt + 等待时长` 再检查，之后按 `now - sentSeenAt` 判断。
  - 扫描器看到的时刻一定不早于真正发出的时刻，所以只会晚判，不会早判。
  - 正常消息的结果在毫秒级就被处理了，扫描器碰不到它们，Redis 访问不增加。只有积压或异常时，每条受影响的消息多一次 `SET`。
  - 旧格式的 meta 没有这一段，按现在的方式处理。
- 等待时长按这条 MT 的通道（`rssid`）取：

  ```
  Client 会等多久 = 通道的 SMPPSubmitPendingRedisTimeoutSeconds > 0 ? 它 : 通道的 BufferTimeout
  Core 等待时长   = max(CoreOutboundRespTimeoutMs, Client 会等多久 + 60 秒)
  ```

  "Client 会等多久"抽成 `A2PCustomerInfo.getSubmitRespTimeoutMs()`，Client（`SmppPendingSubmitManager.timeoutMsFor`）和 Core 共用。HTTP 通道的应答超时要确认取值，也放进这个方法。
- `in_stream`（Client 停顿 2 分钟、硬超时 3 小时）和 `in_pel` 的判断不变。
- `CoreOutboundOutboxRecheckMs`：服务器上配的 300000 改回代码默认的 60000，配置不改代码。扫描器每秒每个主库最多查 `CoreOutboundOutboxScanBatchSize`（100）条，积压时的额外负载有上限。
- 文档：`docs/配置项说明.md` 第 820 行改正。`SMPPSubmitPendingRedisTimeoutSeconds` 在所有模式下都生效，是 `BufferTimeout` 的覆盖值，不只是 redis 模式下的超时。

## 2. 真结果迟到被丢弃时补写 outMsgId 映射：不做

已经回过 2700 了，不再匹配真 DR。

## 3. Redis 模式下 WDQ 是否还在写（调查结论：在写，DB 查找保留）

原始消息（DR 匹配用）在 Redis 模式下的存放方式（`MessageStoreManager`，处理 submit 结果时）：

| 情况 | 存放位置 |
|---|---|
| 正常 | 状态 Redis：`{outMsgId}` → 原始消息，TTL = 消息有效期（不足 1 小时按 1 小时，没有有效期按 24 小时）；同时把 outMsgId 加进按 DateIn 秒分组的 `DR{秒}` 集合 |
| 消息有效期不足 10 分钟、序列化失败、写 Redis 失败 | **直接写 WDQ** |
| 超过 `Redis_RetrieveRedisInterval` 分钟还没等到最终 DR | MQM 的 `RetrieveRedisDRThread` 按 `DR{秒}` 扫描，把还在的原始消息**挪进 WDQ**，并删除 Redis key |

- `Redis_RetrieveRedisInterval` 默认 30 分钟，服务器上配的是 1000 分钟（约 16.7 小时），所以这次压测里没有挪过。
- 结论：WDQ 仍然是"DR 迟到很久"和"写 Redis 失败"时的存放位置，DR 在 Redis 里查不到时**查一次 WDQ 是必要的**。
- 问题只在于"每次重试都查 DB"：每条早到 DR 要查 15 次。第 4 项把重试降到 2 次以后，每条早到 DR 最多查 3 次（首次 + 2 次重试）。

## 4. 早到 DR：只重试 2 次，按时间算，找不到就丢弃

**现状**：`EarlyDrQueue` 每次延迟 `EarlyDr.DelayMs`（2 秒），最多 `EarlyDr.MaxTries`（15）次，按"过了几遍"计数，试满就存 SDQ 文件。积压时每次重试都要在入站流里重新排队，15 遍可能要 1 小时；SDQ 文件又没有程序导入，等于白存。

**改为**：重试时的查找放在 MQM 做，找不到的 DR 不再写回 Core 的入站流；是否再试只按时间判断。

**用到的量**

- `t0`：Client 收到这条 DR 的时间（DR 的 `inTimeMs`，Client 收到时设置）；没有时用 Core 第一次处理它的时间；比当前时间还晚时（时钟偏差）用当前时间。
- `T`：DR 所属通道（`rssid`）的应答超时 `getSubmitRespTimeoutMs()`，和第 1 项同一个方法，默认 60 秒。取不到通道配置时用 60 秒。
- 查找：状态 Redis `GET {outMsgId}`，没有再查 WDQ（`OutMsgID` 有索引）。两处都没有才算找不到。**Core 对还没到 `t0+T` 的 DR 不查 WDQ**，Redis 里没有就直接放进延迟队列，WDQ 由 MQM 每批一条 `SELECT ... IN (...)` 查（PF007 时早到 DR 每分钟 20 万条以上，Core 逐条查 WDQ 把 mysqld 打到 100%）；已过 `t0+T` 的 DR Core 仍自己查一次 WDQ。代价：原消息本来在 WDQ 的 DR（有效期不足 10 分钟、写 Redis 失败、超过 `Redis_RetrieveRedisInterval` 被挪进 WDQ）晚 `T/2` 才匹配。

**流程**

```
Core 收到 DR（入站流）
  ├─ 查找命中 → 正常处理（同现在）
  └─ 找不到 → now < t0+T/2 → 进延迟队列，到期 t0+T/2
              now < t0+T   → 进延迟队列，到期 t0+T
              否则          → 丢弃，[VDR-DROP]
     DR 从入站流确认（ack）

MQM（active 的那个），每 EarlyDr.IntervalMs 取出到期的：
  ├─ 批量查找（Redis → WDQ）
  │    命中 → 写回 stream:core:inbound-dr，Core 照常处理
  │    找不到、now < t0+T → 到期改成 t0+T
  │    找不到、now ≥ t0+T → 丢弃，[VDR-DROP]
  └─ 删除这一条
```

- 最多查 3 次：Core 第一次，MQM 在 `t0+T/2`、`t0+T` 各一次。
- 积压时自动减少：DR 在入站流里排到 `t0+T` 之后才被 Core 处理，Core 找不到就直接丢弃。
- 重复投递（auto-claim）算出来的到期时间相同，延迟队列的 key 还是 `dr:early:{outMsgId}:{状态}:{msgId}`，同一条 DR 只有一个条目；不再需要 `tries` 计数。
- 找不到的 DR 不再进 Core 的入站流，重试时间点不受入站流积压影响。代价是匹配上的早到 DR 多查一次 Redis（MQM 一次、Core 一次）。
- 写延迟队列失败（Redis 异常）：不确认，DR 留在入站流的 pending 里，Redis 恢复后由 auto-claim 处理。不再存 SDQ。
- `ctm.isInDRStoreMode` 那条存 SDQ 的路径（DR 存储模式）是另一个功能，不在这次范围内。

**数据结构**（状态 Redis）

| key | 类型 | 内容 |
|---|---|---|
| `zset:dr:early` | zset | 等待中的条目，score 是下次到期时间（毫秒） |
| `zset:dr:early:processing` | zset | MQM 取走还没处理完的，score 是认领到期时间（取走时间 + `EarlyDr.ClaimTimeoutMs`） |
| `dr:early:{outMsgId}:{状态}:{msgId}` | hash | `body`：DR；`end`：`t0+T`（Core 写入，MQM 不用加载通道配置）。TTL 至少 1 小时（处理完就删，TTL 只是兜底；PF007 验证时按 `T + EarlyDr.ClaimTimeoutMs + 60 秒` 设，MQM 处理不过来时条目先过期，丢了 37 万条 DR，已改） |

**MQM 一轮**（`EarlyDrDispatcher`，只在 active 的 MQM 上；取满 `EarlyDr.BatchSize` 条时马上再取）

1. 放回认领超时的：`zset:dr:early:processing` 里 score ≤ now 的挪回 `zset:dr:early`（MQM 取走后挂掉）。
2. 取出到期的（一个 Lua）：`ZRANGEBYSCORE zset:dr:early -inf now LIMIT 0 batch`，逐个挪进 processing，返回 key、`body`、`end`。取出和挪动在同一个 Lua 里，两个 MQM 同时跑也不会取重。
3. 批量查找：Redis pipeline `GET` 一次；没命中的用一条 `SELECT ... FROM WDQ WHERE OutMsgID IN (...)`。
4. 按结果处理：命中的写回入站流，成功后删除条目；找不到、没到 `end` 的改到期为 `end`；找不到、过了 `end` 的删除并 `[VDR-DROP]`。后两种和删除在一个 Lua 里完成。写回失败的留在 processing，认领超时后重取。

一轮 500 条的开销：Redis 4 次往返加写回的条数，最多 1 次 SQL。没有到期条目时只有两次 `ZRANGEBYSCORE`。写回的 XADD 由 `EarlyDr.WriteBackThreads`（默认 8）个线程并行做：PF007 时单线程写回每秒约 230 条，跟不上结果流积压时每分钟几万条的早到 DR。

**PF007 验证中发现、已修**：outbox 扫描器读到条目后、追加"看到已发出"之前，真结果正好处理掉了条目和 meta，扫描器退回旧逻辑又报了一次 2700（随后作为迟到结果丢弃，客户不受影响，45 分钟约 3000 次）；改为追加失败时先看条目还在不在，不在就跳过。

**配置项**：删掉 `EarlyDr.DelayMs`、`EarlyDr.MaxTries`、`EarlyDr.TtlSeconds`（代码、`docs/配置项说明.md`、`GmmsUtility` 热加载白名单同步删除）。保留 `EarlyDr.Enable`、`EarlyDr.BatchSize`、`EarlyDr.ClaimTimeoutMs`、`EarlyDr.IntervalMs`。

**效果估算**（这次的场景）：被误判 2700 的那批 DR，第 1 项修好后基本不会再产生；就算产生，每条最多查 3 次，`t0+T`（默认 60 秒）时丢弃，不进 Core 入站流。现在是 15 遍、约 130 次 Redis 操作加 15 次 SQL，最长 1 小时的 dr:early 占用，最后写一个没人导入的 SDQ 文件。

**边界**：submit 结果流积压超过 `T` 时，这些消息的真 DR 会被丢弃。这只在系统异常（结果处理严重积压）时出现，符合"只有系统或 Redis 异常时才受影响"的验收标准。已经回过 2700 的 DR 没法在第一次就识别出来（DR 只带 `outMsgId`，回过 2700 的 attempt 没有记录它），仍要等到 `t0+T`。

## 5. 日志改进

这次一小时里，3 个 Core 有约 140 万行"找不到 WDQ"的 WARN，Core 日志每小时滚动好几个 1 GB 文件。

| 日志 | 现状 | 改为 |
|---|---|---|
| `MessageStoreManager.getGmmsMessageByOutMsgID` 查不到时的 WARN `Can't find the message from WDQ by outmsgid` | 每次查不到都打 WARN | DEBUG：查不到在 DR 匹配里是正常分支，结果由调用方记录 |
| `[VDR-MATCH] ... not in redis, looking up the DB` | 每次 INFO | DEBUG，结果由下面那一行记录 |
| DB 查找失败（没有数据库、连接异常） | 两行不带消息 ID 的 ERROR，按 msgId 搜不到 | 一行带消息的 ERROR：`[VDR-MATCH] outMsgId= status= rssid= not in redis, DB lookup failed: left pending`（DR 留在 pending 里重试） |
| `[VDR-EARLY] ... has no original yet, try N again in M ms` | 每遍 INFO | 每条早到 DR 只在第一次打一行 INFO：`[VDR-EARLY] outMsgId= status= rssid= retry at +T/2 and +T`。重试时匹配上的，在 `[VDR-MATCH]` 里带上 `try=1/2`、`waitedMs` |
| 丢弃 | 没有（原来是 `[VDR-STORE]`） | 每条一行 WARN：`[VDR-DROP] outMsgId= status= rssid= waitedMs= reason=no original after 2 tries`。另外每分钟一行汇总 `[VDR-EARLY-STATS] queued= matchedTry1= matchedTry2= dropped=`，大量丢弃时看汇总就够了 |
| `RetrieveRedisDRThread` 的 `TMP-DRLOOKUP ...` | 每个 key 4 行 WARN（临时排查用的） | DEBUG；每轮汇总一行 INFO：`moved N to WDQ` |
| `MessageStoreManager` 写原始消息的 `TMP-DRLOOKUP submit cache ...` | WARN 和 DEBUG 混用 | 写 Redis 失败、直接写 WDQ 的保留 WARN，其余 DEBUG |
| outbox 超时 `Core outbound outbox timeout result produced` | 每条 WARN | 保留每条（真实失败），加上 `ageMs`、`sentSeenAgoMs`、`waitMs`，便于区分是排队还是通道不回 |

## 回归

- outbox：MT 在流里排队超过 180 秒才发出，发出后 1 秒内收到应答 → 不判 2700。
- outbox：通道配 `BufferTimeout=300` → Core 在 360 秒前不判；Client 先判 2700 并写回结果，Core 照常处理。
- outbox：发出后真的没有结果（停掉通道模拟器的应答） → 在 `sentSeenAt + 等待时长` 判 2700。
- 早到 DR：结果晚到 `T/2` 以内 → 第 1 次重试匹配；晚到 `T/2～T` → 第 2 次重试匹配；超过 `T` → `[VDR-DROP]`，不写 SDQ 文件。
- 早到 DR：入站流积压超过 `T` 时，处理一次就丢弃，不会多次重试。
- 早到 DR：原始消息在 WDQ（例如消息有效期不足 10 分钟） → 匹配。
- 早到 DR：写延迟队列失败（Redis 停掉） → DR 留在 pending 里，恢复后由 auto-claim 处理。

改完后在服务器上用 4000 TPS 重跑：过载时只应表现为积压和延时变大，不应再有 2700 误判、DR 无主和状态 Redis 暴涨。之后再按 3500 TPS 跑 5 小时。
