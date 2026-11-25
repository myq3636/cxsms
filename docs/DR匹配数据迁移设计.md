# DR 匹配数据从状态 Redis 迁到 report Redis（设计）

> 2026-11-24。问题：submit 应答回到系统后，数据放在状态 Redis，放到 DR（report）Redis 是否更合理。

**实施状态**：已实现，**固定放在 report Redis**：第一步（DR 匹配数据、早到 DR 队列，`DrLookup`、`EarlyDrQueue`）、第二步（延时 DR、假 DR 超时，3.8）。商用都是全新部署，没有旧版本的数据要迁，所以不要切换开关和回查旧位置（3.6 原来设计的 `DrLookupStore`、`DrLookupLegacyRead` 已去掉），MQM 也不再扫状态 Redis 的 `DR{秒}`。和设计的不同见 3.2、3.4 的"实现"。

## 1. 结论

- **submit 应答本身已经在 report Redis**：Client 把通道的 submit 结果写进 `stream:core:results:{rssid}:{shard}`（`StreamQueueManager.produceResult`，`isSubmitMq=false`，即 report MQ，测试机 8388 / 8398）。
- **放在状态 Redis 的是"DR 匹配数据"**：Core 处理 submit 结果（成功、要 DR）时写的"通道消息 ID → 原消息"，等通道 DR 来时找原消息（`MessageStoreManager` 的 `setPipeline`）。它的读写都在 DR 流程上，而 DR 流程的流（`core:results`、`core:inbound-dr`、`dr:pending`）都在 report Redis。
- **建议迁到 report Redis**，和它同一个生命周期的**早到 DR 队列**一起迁；延时 DR、假 DR 超时作为第二步。状态 Redis 只留全局计数、限额和集群控制数据（第 3 节）。
- 理由主要不是 CPU（《Redis消息操作次数统计》：8386 不是瓶颈），而是：
  1. 数据量随 TPS 增长的消息数据，不应放在**单实例、不能分片**的状态 Redis 上；
  2. DR 流程的数据和流放在同一个故障域，Redis 切换时一致；
  3. 早到 DR 的判断和匹配数据放在同一实例上，可以用一个 Lua 原子完成，消除现在的竞争窗口；
  4. 集群控制数据（模块状态、bind 位置、MQM fence、配置）和消息数据隔离，消息数据积压时不影响控制面。

## 2. 现状：状态 Redis 里有什么

单实例（`Redis_host` / `Redis_port`，测试机 8386），不分片；MQ 是 `Redis_submit_masters`、`Redis_report_masters`，可以多主。

### 2.1 DR 流程数据（每条消息都有，量随 TPS 增长）

| key | 写 | 读 / 删 | 每条 MT |
|---|---|---|---|
| `{outMsgId}`（原消息序列化，TTL 按消息有效期，`getRedisExpireTime`）+ `DR{秒}` hash（索引） | Core 处理 submit 结果：SETEX + HSET（`MessageStoreManager`） | Core 处理通道 DR：GET（`InternalCoreEngineSession`），最终 DR 后 DEL + HDEL；MQM `RetrieveRedisDRThread` 每秒扫 `DR{秒}`，超过 `Redis_RetrieveRedisInterval`（默认 30 分钟）的搬进 WDQ（进度 key `DRRETRIEVETIMEKEY`） | 5 次 |
| `zset:dr:early`、`zset:dr:early:processing`、`dr:early:*` | 通道 DR 先于 submit 结果到达，匹配不到时（`EarlyDrQueue`） | 早到 DR 分发 | 早到 DR 才有 |
| `zset:delay:outdr:{shard}`、`…:processing:`、`delay:outdr:payload:*` | 延时 DR（`DelayedInDRDispatcher`） | 到期分发 | 延时 DR 才有 |
| `zset:fakedr:timeout:{shard}`、`indr:once:*` | 假 DR 超时（`FakeDrTimeoutScheduler`） | 到期发假 DR | 配了假 DR 才有 |
| `IQDR_{inMsgId}` | 查询 DR 的客户（InClientPull=2）（`MessageStoreManager`） | DR 查询 | 这类客户才有 |
| `CainiaoMsg_{requestId}` | 菜鸟提交（`AliCainiaoJsonHttpServlet`） | 推 DR 时读、删（`AliCainiaoDeliveryReportHttpHandler`） | 菜鸟客户才有 |
| `CDR` 列表、`StMsgCount` hash | `CDRManager`（`CDR` 只有 `SmsOptionWrMonitorCDR` 的客户） | 外部采集 / 统计 | 异步 |

### 2.2 全局计数和限额（每条消息，量小，必须全局一份）

`stock:{ssid}[:cc]`（计费余额）、`Duplicate:{recipient}`（24 小时同号码次数）、`blackhole_{ossid}`、`throttle:in:{ssid}:{秒}`（收侧限流）、`StreamAttempt.countOnce` 的计数标记。

### 2.3 集群和控制

模块状态（`module:status:*`、`system:nodes:*`）、客户 bind 位置（`smpp:server:session:*`、`smpp:server:active:*`，Core 发 DR 时读）、MQM 主备和 fence、消息 ID 位置、分片分配、DB 状态、配置发布（路由、内容模板）、控制命令的发布订阅、优先级队列 `Priority_{模块}_{ssid}_*`。

### 2.4 已经在 MQ Redis 的

各个流和门铃、处理进度标记（`core:att:*`）、Client 发送标记（submit MQ）、outbox（submit MQ，`CoreOutboundOutboxManager`）。

## 3. 设计

### 3.1 迁什么，留什么

| 数据 | 去向 | 说明 |
|---|---|---|
| DR 匹配数据 `{outMsgId}` + 索引 | **report Redis**（第一步） | 本设计的主体 |
| 早到 DR 队列 | **report Redis**，和匹配数据同一实例（第一步） | 见 3.4 |
| 延时 DR、假 DR 超时 | report Redis，按现有 `{shard}` 分到各主（第二步） | 都是 DR 流程的调度数据，已经按分片建 key |
| `IQDR_*`、`CainiaoMsg_*` | 可以随第二步迁，也可以不迁 | 只有部分客户，量小 |
| 2.2、2.3、`CDR`、`StMsgCount` | **留在状态 Redis** | 需要全局一份，或者属于控制面 |

### 3.2 key 和实例

- key：`drl:{outMsgId}`。**实现**：设计时想用 `drl:{rssid}:{outMsgId}` 避免两个通道的消息 ID 相同时互相覆盖，但 DR 上的 rssid 不总是可靠：`CommonHttpDRServlet` 取 `Protocol` 为该接口名的第一个 ssid 作为通道，几个通道共用一个接口时 rssid 可能不是发 MT 的那个；带上 rssid 反而会匹配不到。所以 key 和现在一样只用 outMsgId（ID 相同的问题和现在一样，不比现在差）。
- 实例：`ShardHash.of(outMsgId, reportMasters)` 选 report 的一个主（`DrLookup.master`）。
  - 不和 `core:inbound-dr` 的分片绑定：那个分片按通道 DR 的发送方、接收方号码算，而 DR 的号码可能互换（`DrSwapAddr`）或者为空。
  - 只有一个 report 主时，就是那一个。
- 值和 TTL 不变（序列化的原消息，TTL 按消息有效期）。

### 3.3 索引和搬进 WDQ

现在的做法：每秒一个 `DR{秒}` hash，MQM 按秒顺序扫，进度记在 `DRRETRIEVETIMEKEY`，只适合单实例。

改为每个 report 主上一个 ZSET `drl:due`：
- 写入时 `ZADD drl:due {写入时间} {key}`；最终 DR 后和匹配数据一起删（`DEL` + `ZREM`）。
- MQM 对每个 report 主执行一个"认领"Lua：`ZRANGEBYSCORE drl:due -inf {now - 30 分钟} LIMIT 0 {批}`，同时取出这批 key 的值并移出 ZSET（和延时 DR 的批量认领同样的写法），然后写进 WDQ。

**2026-11-26 改为按“该搬的时间”排：** 新写入的放在 `drl:due:at`，分数是 `min(dateIn + Redis_RetrieveRedisInterval, 过期时间)`（毫秒，真实时间），MQM 认领 `ZRANGEBYSCORE drl:due:at -inf {now}`；匹配数据的 TTL 至少比这个时间晚 10 分钟。原因：只按 dateIn 排时，过期时间早于 `Redis_RetrieveRedisInterval` 的 MT（测试服务器配 1000 分钟，短有效期的都是）在 Redis 里先过期被删，MQM 认领时读到空值就跳过，没进 WDQ，通道不回 DR 时客户永远收不到过期 DR。升级前写入的仍在 `drl:due`（分数 dateIn），照旧按 `now - Redis_RetrieveRedisInterval` 认领直到清空，删除时两个 ZSET 都 `ZREM`，所以升级时不会把 Redis 里已有的一次搬进 WDQ。性能：写入仍是一次 Lua，删除的 Lua 多一个 `ZREM`，MQM 每个主每轮多一次（空的）认领；只有短有效期的 MT 会比原来早进 WDQ（它们原来根本进不了）。DrLookupIT DL-07～10。
- 只有 MQM 主节点做这件事（沿用 fence）。写 WDQ 失败的，按原分数放回 ZSET。

### 3.4 早到 DR

现在匹配数据在 8386，早到 DR 也在 8386，但"查匹配数据"和"放进早到队列"是两步，期间 submit 结果写进来的话，这个 DR 要等早到队列下一轮才能匹配上（PF007 的早到 DR 问题与此有关）。迁过去之后，两件事放在同一实例：

- **通道 DR 到达**：一个 Lua 完成"有匹配数据就返回原消息，没有就把 DR 放进早到队列"，不存在中间状态。
- **submit 结果到达**：一个 Lua 完成"写匹配数据和索引，同时检查有没有为它停着的早到 DR，有就取出来"，Core 立即处理这个 DR，不用等早到队列的轮询。

为此，早到 DR 的 key 也按 outMsgId 落在同一个主上，加上每个主一个 ZSET 做超时兜底。

**实现**：早到 DR 队列（`dr:early:*`、`zset:dr:early`）的 key 不变，放在 outMsgId 的那个 report 主。做了第一点：放进队列的 Lua 先看匹配数据在不在，在就返回 `FOUND`，Core 当场再匹配一次（只重试一次）。第二点（写匹配数据时顺便取出停着的早到 DR）**不做**（2026-11-24 决定）：每条消息的 submit 结果都要多做这个检查，而商用中 DR 早到的概率很低，不值得；写匹配数据仍是一次 Lua（SETEX + ZADD），早到 DR 仍由 MQM 在 t0+T/2、t0+T 匹配。第一点只在早到 DR 这条少见的路径上，是放进队列那次 Lua 里多一个 EXISTS，不增加往返。MQM 处理每个 report 主上的队列。

### 3.5 操作次数

| | 现在 | 迁移后 |
|---|---|---|
| 状态 Redis（每条 MT） | 5（SETEX、HSET、GET、DEL、HDEL） | 0 |
| report Redis（每条 MT） | 0 | 3 次往返：写（Lua：SETEX + ZADD + 早到检查）、读（GET，或者早到 DR 那个 Lua）、删（DEL + ZREM，pipeline） |

report 的往返多了 3 次，但它可以按主扩展；8386 每条 MT 少了 5 次同步往返，Core 处理 DR 的串行路径上也少了一次跨实例。

**实测**（2026-11-24，测试机，PF003B 1000 TPS × 60 秒，各 6 万条，DR 全部回到客户，延时、积压两种模式相同；每条消息的命令数含 Lua 内部的命令和轮询）：

| 实例 | state 模式（`perf_step_PF003B_1124_1004`） | report 模式（`…_1000`） | 变化 |
|---|---|---|---|
| 8386 state | 10.3 | 5.4 | −4.9 |
| 8388 + 8398 report | 51.8 | 59.8 | +8.0（往返 +3：写 Lua 里 SETEX + ZADD，GET，删 Lua 里 DEL + ZREM） |
| 8387 + 8397 submit | 37.4 | 37.3 | — |

测完后删 Lua 去掉了多余的 `ZREM drl:due:processing`（MQM 认领期间 DR 到达，MQM 仍按已读到的值搬，RESOLVE 会删成员），report 再少 1 条，约 +7。

### 3.6 迁移（不需要）

原设计有 `DrLookupStore`（state / report）和 `DrLookupLegacyRead`（切换期间回查旧位置）两个开关，用于从旧版本滚动升级。商用都是全新部署，没有状态 Redis 上的旧数据，所以去掉了：DR 匹配数据和 DR 调度数据固定在 report Redis。

### 3.7 容量和风险

- 内存：条数约等于 TPS × 平均等 DR 的时间，上限是 30 分钟（超过的搬进 WDQ）。
  - 例：4000 TPS、平均 60 秒，约 24 万条；每条约 1 KB（实施时实测），约 240 MB，分到各个 report 主。
  - 通道长时间不回 DR 时最多是 4000 × 1800 = 720 万条，约 7 GB。这正是不应该放在单实例状态 Redis 上的原因；report 主的 `maxmemory` 要按这个算，策略保持 `noeviction`。
- report Redis 故障切换时，DR 匹配数据和 DR 流一起切换，一致。现在两者分属两个实例，可能出现流还在、匹配数据丢了（或者反过来）。
- report Redis 的持久化（AOF）要和这份数据的重要性一致，实施前核对 report 和 state 两边的配置。

### 3.8 第二步：延时 DR、假 DR 超时（已实现）

- 位置（`DrLookup.placeOf(shard)`）：该 Core 分片对应的 report 主（`shard % report 主数`，和分片流一样）。key 不变：
  - 延时 DR：`zset:delay:outdr:{shard}`、`zset:delay:outdr:processing:{shard}`、`delay:outdr:payload:{shard}:*`；
  - 假 DR 超时（way 4）：`zset:fakedr:timeout:{shard}`、`zset:fakedr:timeout:processing:{shard}`、`indr:once:{outMsgId}`。
- 延时 DR（`DelayedInDRDispatcher`）：写入由两次往返（SETEX payload、ZADD）合成一个 Lua；认领、分发、重试、失败都在认领它的那个实例上做（原来的重试、失败固定写状态 Redis）。
- 假 DR 超时（`FakeDrTimeoutScheduler`）：
  - 开始计时、取消、到期认领都在 `placeOf(shard)`。
  - 到期时要用匹配数据的 TTL 和原消息：原来用 Lua 读状态 Redis 上 `{outMsgId}` 的 TTL、再 `GET {outMsgId}`；匹配数据在 outMsgId 的 report 主上，不一定和分片的主相同，所以先用 `DrLookup.ttl` 取 TTL，作为参数传给 Lua，原消息用 `DrLookup.get` 取。
- 回归：`FakeDrIT`、`CoreDrBizIT`、`EarlyDrIT`、`DrLookupIT` 等按 report Redis 的 key（`drl:{outMsgId}`）检查；`suites.txt` 第 4 列可以给测试集加 JVM 参数（`-Dit.props=键=值,…` 在启动后写进配置，以后同一个测试换配置再跑时用）。

## 4. 验证

- 回归：
  - DR 匹配（单段、长短信每段一个 ID、通道 ID 重复的两个通道）；
  - 早到 DR（DR 先到、submit 结果后到，立即匹配）；
  - MQM 搬进 WDQ（把 `Redis_RetrieveRedisInterval` 调小）；
  - 多个 report 主；
  - 迁移：在 `state` 下写，切到 `report`，DR 到达后仍能匹配。
- routetest：PF 系列对比每条消息在各实例上的命令数、Core 处理 DR 的延时、早到 DR 数；HT002 这类 HTTP 通道的长短信。
