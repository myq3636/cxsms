# submit / report Redis 多主分片设计

> 日期：2026-11-18。起因：Redis 成为性能瓶颈（《Core处理MT慢的原因分析》：5000 TPS 时 8387 主线程 93%，Server 写进 `mt:pending` 每秒 5939 条，Core 只处理 2220 条）。目标：submit MQ（现 8387）和 report MQ（现 8388）各自支持**多个主库**，消息按哈希分到不同主库，吞吐随主库数增加。state Redis（8386）不变。本文是设计，没有改代码。

## 1. 结论

1. **按消息哈希选 Redis：用的就是现在决定分片的那个哈希**。现在每个 stream 名里都已经有分片号（`stream:mt:pending:{ossid}:{分片}` 等，分片由消息的 ssid、发送方、接收方等算出）。规则是 **实例 = 分片 → 实例的映射（默认 `分片 % 主库数`）**：一个 stream 和它的门铃、索引、StreamAttempt 标记、Client 发送标记、outbox 全在同一个实例上，现在的所有 Lua 原子操作都不用拆。
2. 不另外为每条消息算一个独立的哈希去选实例：那样同一个 stream 的条目会分到不同实例，消费者按分片的归属、长短信各段同一个 stream 按顺序处理、门铃和标记的原子性都保证不了。
3. 消费者（Core、Client、Server、MQM）按现在的分片归属读 stream，只是要**从所有实例取门铃**；生产者按 stream 名路由到实例。
4. 唯一需要调整分片算法的地方：Server 写回 DR 结果并确认 DR 条目（`RESULT_AND_ACK_LUA`）的两个 stream 要在同一个实例上，`dr:pending` 和 DR 结果 stream 改用同一个分片输入（MsgID）；暂存 stream（`stream:core:hold:{rssid}`）和死信 stream 现在没有分片号，改为每个实例一个。
5. 同时把 `TotalShards` 从 16 提到 64：每个实例分到更多分片，大客户的 MT 能被更多 Core 线程同时处理（也解决《Core处理MT慢的原因分析》3.1 的并发度问题）。
6. 工作量估计 2～3 周（含回归、多实例的测试环境、故障切换测试）。

## 2. 现状

| 项 | 现在 |
|---|---|
| 实例 | submit MQ 一个主库（8387，从库 8377）、report MQ 一个主库（8388，从库 8378） |
| 客户端 | `RedisClient` 里各一个 `RedisConnection`（`getSubmitMqRedis()`、`getReportMqRedis()`、`xxxSubmitMq(...)`、`xxxReportMq(...)`），11 个文件、59 处调用 |
| 分片 | `TotalShards`（16）个逻辑分片，体现在 stream 名里；Core、Client 按 `CoreShard.*` / `ClientShard.*` 负责一部分分片；所有分片在同一个实例上 |
| 主从切换 | 应用层（`RedisMonitor`、主从各一个连接），没有 Sentinel |

## 3. 设计

### 3.1 分片 → 实例

- 每个 MQ（submit、report）配置一组主库（和对应的从库），编号 0..N-1；
- 实例 = `shardMap[分片]`，没配映射时默认 `分片 % N`；
- 从 stream 名取分片号（各类 stream 的分片在名字里的位置不同，`StreamQueueManager` 已经有解析逻辑：`isResponsibleForStream` 就是按它判断归属）；
- 没有分片号的 key（升级前的旧 stream、`stream:outbound:http:tasks`）固定在实例 0；
- N = 1 时和现在完全一样（兼容）。

### 3.2 各类 key 放在哪

| key | 现在 | 多主以后 |
|---|---|---|
| `stream:mt:pending:{ossid}:{分片}` 等所有分片 stream | submit / report | 按分片的实例 |
| 门铃 `zset:*:active` | 每类一个 | **每个实例一个**（同名，各在自己的实例上）；生产者写 stream 所在实例的那个 |
| `idx:streams` | 每个 MQ 一个 | 每个实例一个 |
| StreamAttempt 标记 `core:mt:{msgId}`、`core:att:{stream}:{id}` | stream 所在 MQ | 读这个条目的 stream 所在实例（现在的规则就是"标记放在条目来源 stream 的实例上"） |
| Client 发送标记 | submit MQ | `mt:routed` stream 所在实例 |
| outbox（消息体、meta、`:done`、超时 zset） | submit MQ | `mt:routed` stream 所在实例；超时 zset 每个实例一个，扫描线程每个实例扫一个 |
| `stream:core:hold:{rssid}` | 每个通道一个 | **`stream:core:hold:{rssid}:{实例}`**：消息按它要去的 `mt:routed` 分片所在实例进对应的 hold；放行时 hold 和目标 routed 在同一个实例（`RELEASE_LUA` 不用拆） |
| 死信 `stream:dlq:submit` / `stream:dlq:report` | 各一个 | 每个实例一个（`DEAD_LETTER_LUA` 的来源 stream 和死信在同一实例） |
| `stream:core:results:{ssid}:{分片}` 和 `stream:dr:pending:{ossid}:{模块}:{分片}` | report MQ | 按分片的实例；见 3.3 |

### 3.3 需要同址的跨 stream 操作

| Lua | 涉及的 stream | 处理 |
|---|---|---|
| `RESULT_AND_ACK_LUA`（Server 写 DR 结果 + 确认 DR 条目） | `stream:core:results:{ossid}:{分片}`（分片 = hash(MsgID)）、`stream:dr:pending:...:{分片}`（分片 = hash(ossid_MsgID)） | 把 `calculateDrShardId` 的输入改成和结果 stream 一样（MsgID），两者分片相同、实例相同。改了分片算法要先清空积压再升级（同 `TotalShards` 的改动） |
| `PRODUCE_ONCE_LUA`（Core 写 `mt:routed`，带 StreamAttempt 标记） | `mt:routed` 所在实例 + 标记（在 `mt:pending` 所在实例） | 两个实例不同时，标记不能在脚本里写。现有代码走 `producedElsewhere`：本地记下、到 FLUSH 时才写，重投时靠已写的标记判断"已写出"。多主以后这会是常态，而"写出 routed 到写标记"之间 Core 崩溃，重投会**再发一次给通道**，所以改为：跨实例写出后**马上**对标记 `HSET p:{段}`（每条 MT 多一条普通命令），窗口缩到亚毫秒；只有这一瞬间崩溃才可能重复发送 |
| `RELEASE_LUA`（暂存放行） | hold → `mt:routed` | hold 按实例拆（3.2），同址 |
| `DEAD_LETTER_LUA` | 来源 stream → 死信 | 死信按实例拆，同址 |
| 取门铃、未读检查 | 门铃 + 同实例的 stream | 门铃每个实例一个，同址 |

`CoreOutboundOutboxManager.claimResult`（结果到达时认领 outbox）：结果里有 rssid、发送方、接收方，按外发分片的算法算出 `mt:routed` 的分片 → 实例，在那个实例上认领。

### 3.4 消费端

- 各消费者的调度循环（`popResponsibleDoorbell`）**轮流从每个实例的门铃 zset 取**，返回的 stream 名本身就确定了实例，后续读、认领、确认都按 stream 名路由；
- 分片归属（`isResponsibleForStream`）不变：一个 Core 负责的分片可能分布在几个实例上，它对每个实例都要读；
- 自动认领、积压监控、跨节点认领、重按门铃、DR 转移、`a2pctl`、`DrainController`、`BufferMonitorManager`：对每个实例分别做；
- 每个实例一个连接池（`Redis_pool_maxActive` 按实例算）。

### 3.5 配置

```properties
# 不配时用原来的 Redis_submit_master_host / port（等于 1 个实例）
Redis_submit_masters=172.19.4.38:8387,172.19.4.38:8397,172.19.4.38:8407
Redis_submit_slaves=172.19.4.38:8377,172.19.4.38:8357,172.19.4.38:8367
Redis_report_masters=172.19.4.38:8388,172.19.4.38:8398,172.19.4.38:8408
Redis_report_slaves=172.19.4.38:8378,172.19.4.38:8358,172.19.4.38:8368
# 可选：分片 -> 实例（默认 分片 % 实例数）
RedisSubmitMqShardMap=
RedisReportMqShardMap=
TotalShards=64
```

- 所有模块必须一致（同 `TotalShards`）；实例数和映射只在启动时读，改动按第 5 节的步骤；
- 按 CLAUDE.md 写进《配置项说明》；这些项需要重启，不加入热加载白名单。

### 3.6 主从切换

每个实例一对主从，各自独立判断、切换（现在 `RedisMonitor` 的逻辑按实例做一份）。一个实例的主库挂掉，只影响分到这个实例的分片。

### 3.7 代码改动

| 位置 | 改动 |
|---|---|
| `RedisClient` | submit、report 各从一个连接改成一组（`List<RedisConnection>`）；`submitMq(String streamKey)` / `reportMq(String streamKey)` 按 stream 名选实例，`submitMqByShard(int)` / `submitMqAt(int instance)`；原来不带 key 的 `xxxSubmitMq(key, ...)` 改为按 key 路由；新增"每个实例"的遍历 |
| `StreamQueueManager` | 生产、消费、门铃、索引、列 stream、积压监控、自动认领、跨节点认领都按实例；`calculateDrShardId` 的输入（3.3） |
| `StreamAttempt` | 标记跟着条目来源 stream 的实例（`claim`、`Batch` 已经带 `RedisConnection`，调用方传对的实例） |
| `ClientSendMarker`、`CoreOutboundOutboxManager` | 按 `mt:routed` stream 的实例；outbox 扫描每个实例一个 |
| `ChannelReleaseDispatcher` | hold stream 按实例拆 |
| 4 个 Core 消费者、Client、Server、MQM、HTTP 的消费者 | 取门铃轮流取各实例；其余按 stream 名路由，基本不用改 |
| `DrTransfer`、`A2pCtl`、`DrainController`、`BufferMonitorManager`、`RedisMonitor` | 每个实例 |
| 回归环境（`regression/run.sh`） | 起多个 Redis（例如 submit 2 个、report 2 个），全量回归在 1 个实例和多个实例两种配置下都跑 |

## 4. 收益估算

| 项 | 估算 |
|---|---|
| Redis | submit、report 的负载按分片均分到 N 个主库，每个主库的 CPU ≈ 现在的 1/N |
| 这台机器（16 核） | a2p 在 5000 TPS 时约用 9～10 个核，再加 MySQL；**同一台机器上最多再放约 2～4 个主库**。submit、report 各 2 个主库时，Redis 的上限约是现在的 2 倍 |
| Core 并发 | `TotalShards` 64 以后，大客户的 MT 能同时被约 64 个 Core 线程处理（现在 16 个） |
| 5000 TPS | 主库放到单独的机器（或同机只放主库、从库放别的机器）、submit 和 report 各 3 个主库时，Redis 不再是瓶颈的可能性较大；之后的上限是 Core、Server 的 CPU 和 MySQL，需要实测 |

## 5. 部署和改实例数

- **第一次部署**：代码支持多实例后先按 1 个实例上线（和现在完全一样）；再按第 3.5 节配置多个实例。改实例数（或映射、`TotalShards`、分片算法）时，分片和实例的对应变了，**要先清空积压**：
  1. Server 停止接收（或停掉 Server 模块），等所有 stream 清空（`a2pctl` / 积压监控确认为 0）；
  2. 停所有模块，改配置（所有模块一致）；
  3. 启动新增的 Redis 实例，启动所有模块；
- 以后可以做一个迁移工具，把旧映射下的积压搬到新实例，就不用等清空。

## 6. 测试

| 测试 | 内容 |
|---|---|
| 全量回归 | 1 个实例（兼容）和多个实例（submit 2、report 2）两种配置 |
| 新增 | 分片 → 实例的映射；门铃从多个实例取；一个 Core 负责的分片在几个实例上都能读到；`RESULT_AND_ACK`、`RELEASE`、死信在多实例下同址；outbox 认领在正确的实例；`PRODUCE_ONCE` 的标记在别的实例时走 `producedElsewhere`；一个实例的主库挂掉、切到从库时只影响它的分片 |
| 性能 | 1000 TPS 每条 MT 每个实例的主线程 CPU（应约为单实例的 1/N）；5000 TPS 看 `mt:pending` 是否还积压、Core 每秒处理多少；PF002 一小时（抓包对账、内存） |

## 7. 实施顺序

| 阶段 | 内容 | 估计 |
|---|---|---|
| 1 | `RedisClient` 多实例和按 stream 名路由；N=1 时行为不变；回归 | 4～5 天 |
| 2 | 门铃、索引、列 stream、监控、自动认领按实例；hold、死信按实例；`calculateDrShardId`；多实例的回归环境 | 4～5 天 |
| 3 | 测试服务器上增加 submit、report 各 1～2 个主库（加从库），`TotalShards` 64；全量回归、性能测试 | 2～3 天 |
| 4 | 故障切换测试、文档、操作手册 | 2 天 |

## 8. 改动影响评估（2026-11-18，按代码核对）

**结论：中等，集中在 Redis / stream 这一层，不动业务代码；默认 1 主时所有 key 都落到同一个实例，行为和现在完全一样。** 可以开始改。

### 8.1 改动面

| 位置 | 现在 | 要改的 |
|---|---|---|
| `RedisClient` | submit、report 各一个连接；`xxxSubmitMq(key, …)` 约 40 个方法 | 各改成一组连接。这些方法的第一个参数就是 stream 名，**在 `RedisClient` 里按 key 选实例**，调用的地方基本不用改；新增 `submitMq(key)` / `reportMq(key)`、`submitMqs()` / `reportMqs()`（遍历） |
| `StreamQueueManager`（45 处） | 直接用 `getSubmitMqRedis()` 等 | 按 key 路由的约 25 处不用改；要改的约 20 处：取门铃（ZPOPMIN / ZRANGE，各实例轮流）、索引和列 stream、积压监控、重按门铃、自动认领（各实例）、`RESULT_AND_ACK`、`PRODUCE_ONCE`、hold、死信 |
| `StreamAttempt`、3 个 Core 消费者 | 标记在 `getSubmitMqRedis()` / `getReportMqRedis()` | 按读的 stream 取连接（每个消费者 2 处） |
| `ClientSendMarker`、`CoreOutboundOutboxManager` | submit MQ | 按 `mt:routed` stream 的实例；outbox 超时扫描每个实例一个 |
| `DrTransfer`、`A2pCtl`、`BufferMonitorManager`、`DrainController` | 一个 report / submit 连接 | 每个实例 |
| 主从切换（`RedisClient.setRedisHaFlag`） | 一个全局开关，三个连接一起切到从库 | 不变：所有主库一起切到各自的从库（不做 3.6 的按实例单独切换，以后需要再做） |
| 配置、文档 | — | `Redis_submit_masters` 等 4 项（只在启动时读）、《配置项说明》 |

估计改动约 10 个文件、800～1000 行；不涉及业务代码。

### 8.2 和第 3 节相比的简化

| 项 | 第 3 节 | 简化后 | 理由 |
|---|---|---|---|
| hold | 每个实例一个 `stream:core:hold:{rssid}:{实例}` | **hold 留在实例 0**；放行时目标 `mt:routed` 在别的实例就分两步：先写 `mt:routed`（带 outbox），再确认 hold | 放行按通道限速，hold 拆成多个要改限速逻辑；两步之间崩溃最多在同一个 `mt:routed` stream 里多一条，Client 的发送标记（同一个 msgId，同一个实例）会挡住重复发送 |
| `RESULT_AND_ACK` | 改 `calculateDrShardId` | 只在多主时，`dr:pending` 的分片改成和结果 stream 一样（按 MsgID）；1 主时不变 | Server 读自己模块的所有 DR 分片，分片算法改了不影响谁读；改实例数本来就要先清空积压 |
| outbox 认领 | 按结果算出 `mt:routed` 的实例 | 先按结果算（rssid、发送方、接收方），没找到再试其他实例 | 发送方地址万一在发送前被改过，也不会漏 |
| 主从切换 | 每个实例单独判断 | 和现在一样全局切 | 改动小；按实例切换以后再做 |

### 8.3 风险和验证

- 主要风险是漏改：某类 key 被写到一个实例、却去另一个实例读，消息就会停在那里。办法：所有路由集中在 `RedisClient` 一个函数里；新增多主测试，从 Server 收 MT 到客户收到 DR 整条流程跑一遍，结束时检查每个实例都没有积压、没有残留标记；
- 1 主时所有路径都落在同一个连接上：全量回归在 1 主配置下跑，结果应和现在一样；
- 现有回归的检查直接读 16379 上的 key，所以它们只在 1 主下跑；多主由新增的测试覆盖（回归环境多起 2 个 Redis）。

## 9. 实现（2026-11-18）

按第 8 节实现，默认 1 主，配了 `Redis_submit_masters` / `Redis_report_masters` 才是多主。

| 位置 | 改动 |
|---|---|
| `RedisClient` | submit、report 各一组连接（`[0]` 就是原来的 `getSubmitMqRedis()` / `getReportMqRedis()`）；`instanceOf(key, 主库数)`：带分片号的 stream 在 `分片 % 主库数`，其余在 0；`submitMq(key)`、`reportMq(key)`、`mqs()`；`xxxSubmitMq(key, …)` 按 key 路由，门铃的写和删按成员（stream 名）路由；门铃读取（ZPOPMIN 各主库轮流、ZRANGE 合并）、SCAN、跨主库的 XREADGROUP 在所有主库上做；主从切换所有主库一起切 |
| `StreamQueueManager` | 取门铃各主库轮流，弹出的 Lua 在门铃所在的主库上；stream 索引每个主库一个，全量扫描按主库分别计时；积压监控按主库分组走 pipeline；`PRODUCE_ONCE` 在目标 stream 的主库上，标记在别的主库时写出后立即记下（`StreamAttempt.flushNow`）；`RESULT_AND_ACK` 两个 stream 不在同一个主库时分两步；多主时 `dr:pending` 按 MsgID 分片；hold 放行目标在别的主库时分两步（`RELEASE_TO_LUA` 后再确认 hold） |
| `GmmsMessage` | 新增不序列化的 `redisStreamKey`：读出来的 stream，Client 发送标记据此找主库 |
| `StreamAttempt`、3 个 Core 消费者、`ClientSendMarker`、`CoreOutboundOutboxManager`、`DrTransfer`、`BufferMonitorManager` | 按 stream 取主库；outbox 认领先找目标 stream 的主库，没有再试其他主库；outbox 超时扫描每个主库一次 |
| 回归 | `run.sh` 多起 16382、16383；`MultiMasterIT`（submit 16379 + 16382，report 16379 + 16383，真实的 Server、Core、Client 进程）：40 条 MT 各发一次、40 个 DR 各回一次，两个分片的 stream 各在自己的主库，最后每个主库都没有积压、没有残留标记和 outbox |

## 10. 测试服务器上的 5000 TPS 结果（2026-11-18）

部署：submit 8387 + 8397（从库 8377、8367），report 8388 + 8398（从库 8378、8368），新实例的数据在 `/mnt/redisdata/{端口}`；`TotalShards` 仍是 16。用例 `cases/PF002_cmd`（混合流量 5000 TPS × 60 秒，预热 10 秒，排空 10 秒），测试前清空 Redis 和 MySQL、重启 a2p。和改动前同样方法的测量（《Core处理MT慢的原因分析》）对比：

| 项 | 1 主（改动前） | 2 主 |
|---|---|---|
| submit 主库主线程 | 8387 93% | 8387 77%，8397 77% |
| report 主库主线程 | 8388 92% | 8388 65%，8398 66% |
| state 8386 | 53% | 49% |
| 通道收到的 submit_sm（80 秒内，共发 36 万段） | 15.3 万 | 24.4 万（第二次 26.5 万），**多约 60%** |
| 客户收到的 DR（测试结束时） | 13.9 万 | 17.2 万（第二次 19.5 万） |
| 客户提交 → 通道 p50 | 14.9 秒 | 23.5～26.1 秒 |
| 通道 DR → 客户 p50 | 0.12 秒 | 9.2～15.5 秒 |
| 积压 | `mt:pending` 一直涨 | 发送停止后约 20 秒 MT 积压清零；结果 stream 一度积压约 9.4 万，约 2 分钟内读完；没有卡住的消息 |

结论：

1. **Redis 不再是瓶颈**：负载在两个主库上平均分开，主库主线程 65%～77%；
2. **现在的瓶颈是这台机器的 CPU**：负载期间整机空闲只有 0～7%（16 核）。Redis 合计约 4.5 个核（4 个主库、4 个从库、state），a2p 的 9 个模块约 10 个核（Server1 1.9、3 个 Core 各 1.5～1.8、2 个 Client 各 1.0），另外还有压测工具。MT 多处理了约 60%，DR 和结果的量也跟着增加，在 CPU 不够时后面的环节（结果、DR）排队，所以 DR 延时比原来长；
3. 要在这台机器上真正跑满 5000 TPS，需要减 CPU：压测工具（客户和通道模拟器）放到另一台机器；从库放到另一台机器；或者减少每条消息的处理开销（《Redis消息操作次数统计》第 4 节第 4～6 项）。Core 的并发（`TotalShards` 16，每个 Core 只有 11～13 个线程在处理 MT）在 CPU 有余量以后再提高。
