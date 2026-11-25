# DR 消费减少 Redis 往返设计

2026-11-25。只是设计，代码还没改。

## 1. 背景：4000 TPS 时 9981 的 DR 处理不过来

PF007 的流量（4000 TPS，营销 rtmk 约 73% 走通道 9981），2026-11-25 的 10 分钟压测（`perf_long_PF007S_1125_0706`），在发送期间每 30 秒抓一次，共 12 次：

| 项目 | 测到的 |
|---|---|
| 9981 的 DR 每秒进来 | 3203 条 |
| 9981 的 8 个分片，每个每秒消费 | 约 290 条，8 个合计 2314 条；未读积压每秒 +889 |
| 每个分片的待确认（pending） | 12 次里 10-12 次是 100：一直有一整批在处理 |
| Core 每条 DR 的处理耗时（`consumer.inbound_dr`） | 平均 2.8 ms，p50 3.1 ms，p95 7.3 ms |
| Core 每次 Redis 读（`redis.op.read`） | 平均 0.74 ms，p50 0.61 ms |
| Core DR 消费线程池（`coreinbounddr.pool`） | 活跃 2-5（上限 50），排队 0 |
| 报告 Redis 8388 / 8398 | CPU 80%，没满 |
| Core CPU / 整机空闲 | 低于 11-20 的两次 / 27% |

一个分片的 stream 同一时间只交给一个线程（`InboundDRStreamConsumer` 的 `processingMap`），批里的条目一条一条处理，所以一个 ssid 的 DR 最多 S 个线程（`StreamShards`，默认 8）在处理。每条约 3 ms，一个分片每秒不到 300 条，线程池再大也用不上。

3 个 Core 的 jstack（共 154 个 DR 消费线程栈），92% 在等 Redis 回包：

| 在等什么 | 占比 | Redis | 每条 DR 的往返次数 |
|---|---|---|---|
| 给客户发 DR，写 `dr:pending`（`StreamQueueManager.produceOnce`） | 22% | 报告 | 1 |
| 查 DR 匹配数据（`DrLookup.get`） | 20% | 报告（outMsgId 的主） | 1 |
| 删 DR 匹配数据（`DrLookup.delete`，确认后） | 18% | 报告（outMsgId 的主） | 1 |
| 客户在哪个 Server 有会话（`SmppServerSessionRegistry.hasActiveSessionInModule` → `activeDetail`） | 18% | 状态 | 1 次 ZRANGE + 每个 bind 1 次 GET（PF007 6 个 bind，共 7 次） |
| 重投去重标记（`StreamAttempt.flush`） | 11% | 报告 | 1（pipeline） |
| 读一批、认领、确认 | 约 5% | 报告 | 每批 3 次，分到每条很少 |

状态 Redis 8386 在压测里每秒 21191 次 GET、3531 次 ZRANGE，正好是 6 个 bind 的 1 + 6；它 CPU 41%，命令本身只占 3.5%，开销都在网络上。

### 1.1 只加分片：9981 配 `StreamShards:16`

同样的 10 分钟压测（`perf_long_PF007S_1125_0740`），只在 cm 里给 9981 配 16 个分片：

| 项目 | 8 个分片（0706） | 16 个分片（0740） |
|---|---|---|
| 9981 的 DR 每秒消费 / 进来 | 2314 / 3203 | 2842 / 3212 |
| 未读积压 | 每秒 +889 | 每秒 +370 |
| 每个分片每秒消费 | 约 290 | 约 177 |
| 每条 DR 处理耗时（平均 / p50 / p95） | 2.8 / 3.1 / 7.3 ms | 4.4 / 3.8 / 9.7 ms |
| Core 每次 Redis 读（平均） | 0.74 ms | 1.0 ms |
| DR 消费线程池活跃（每个 Core） | 2-5 | 5-10 |
| 报告 Redis 8388 / 8398 CPU | 92% / 91% | 96% / 96% |
| 报告 Redis 命令数（每个） | 97.5k/s | 106k/s |
| 状态 Redis 8386 CPU / 命令数 | 38% / 21.4k/s | 42% / 25.4k/s |

（CPU 和命令数是发送开始后第 3 到第 9 分钟的平均，Core 指标是同一段时间的 30 秒窗口。前面命令统计那次的 80% 是从预热后第 1 分钟开始算的。）

分片翻倍，消费只多了 23%：并发上去以后，报告 Redis 到了 96%，每次访问变慢，每条 DR 从 2.8 ms 变成 4.4 ms。jstack 的分布和 8 个分片时一样（等会话明细、查匹配数据、写 `dr:pending`、删匹配数据、写标记，各占约 12-20%）。所以只加分片不够，还要减少每条 DR 对报告 Redis 的访问；3.1、3.2 少的两次都在报告 Redis 上，3.3 在状态 Redis 上（还有余量），只减少 Core 的等待。

## 2. 目标

在不改变每条 DR 处理结果和顺序的前提下，减少每条 DR 依次访问 Redis 的次数：

- 查匹配数据：每条 1 次 → 每批每个报告主 1 次（MGET）；
- 删匹配数据：每条 1 次 → 每批每个报告主 1 次（一个 Lua）；
- 查客户会话：每条 1 + N 次 → 每条 1 次（一个 Lua），同一批里同一个客户、同一个 Server 只查一次。

按 jstack 的占比，每条 DR 等 Redis 的时间能少一半左右，一个分片每秒从约 290 条到约 600 条。8 个分片约 4800 条，超过 4000 TPS 时 9981 的 3200 条。但报告 Redis 已在 92-96%（1.1），实际能到多少取决于报告 Redis 少了多少请求：3.1、3.2 每条 DR 少 2 次请求（两个主每秒各约 1600 条 DR，各少约 3200 次请求），要压测才知道；不够时再考虑报告 Redis 加到 4 个主（`ShardHash` 支持 4 个）。

## 3. 设计

### 3.1 一批的 DR 匹配数据一次取（MGET）

**现在**：`CoreMessageProcessor.handleInboundDR` 每条调一次 `DrLookup.get(outMsgId)`（GET `drl:{outMsgId}`）。

**改为**：
1. `InboundDRStreamConsumer.FetchAndProcessTask` 读到一批、认领之后，取出这批每条 DR 的 outMsgId，调 `DrLookup.prefetch(outMsgIds)`：按 outMsgId 的报告主分组（`DrLookup.master`），每个主一次 MGET（复用 `existing` 的分组方式）。结果放在线程上的一个 Map 里（`ThreadLocal`，和 `StreamAttempt.current` 一样），这批处理完清掉。
2. `DrLookup.get(outMsgId)`：线程上有预取结果，且这个 outMsgId 取到了值，就直接用（用一次删掉，下一次还是走 GET）；没取到值（null）或没有预取，照原来发 GET。
3. MGET 失败（Redis 异常）：不预取，这批每条照原来一条一条 GET，和现在一样。

**为什么没取到值时还要再 GET 一次**：早到的 DR（通道的 DR 比 submit 应答先到）在 MGET 时还没有匹配数据，到处理它时可能已经写进来了。再 GET 一次就和现在完全一样。这种情况很少，不影响正常消息。

**一致性**：匹配数据只在 submit 应答处理时写一次（`DrLookup.put`），之后只会被删除。预取到删除之间的变化只有一种：同一批里前面一条最终 DR 删掉了它。但现在的删除也是等这批确认后才做（`StreamAttempt.afterDone` → `Batch.afterAck`），同一批后面那条现在也能查到。所以预取的结果和现在每条查到的一样。

### 3.2 一批的 DR 匹配数据一次删

**现在**：最终 DR 处理成功后，`deleteDrLookup` 放进 `afterDone`。按批处理时，这批确认（`Batch.ackAll`）之后一条一条执行 `DELETE_LUA`（DEL + ZREM `drl:due`）。

**改为**：`ackAll` 之后，把这批要删的 outMsgId 按报告主分组，每个主执行一次 `DELETE_BATCH_LUA`（对每个 key：DEL + ZREM）。顺序不变，还是在确认之后。删除失败和现在一样只记日志：匹配数据按 TTL 过期，或者由 MQM 搬进 WDQ。

实现上，`afterDone` 里删匹配数据的动作改成登记 outMsgId（不是 Runnable），由 Batch 在 `ackAll` 后统一执行；不按批（`CoreStreamAttemptBatch=false`）或没有 attempt 时照原来立即删。

**崩溃窗口**：确认之后、删除之前 Core 崩溃，匹配数据留下来，现在也有这个窗口（确认和删除是两次调用），按批只是这个窗口长了一个 Lua 的时间。

### 3.3 客户会话一次查完

**现在**：`SmppServerDrRouteResolver.resolveTargetModule` 每条 DR 调 `hasActiveSessionInModule(ossid, module)`：先 ZRANGE `active:{ossid}:{module}` 拿到这个客户在这个 Server 上的所有会话，再对每个会话 GET 一次明细，看有没有过期、是不是这个 Server。6 个 bind 就是 7 次往返。

**改为**：两步，先做 a。

a. **一个 Lua 查完（结果不变）**：ZRANGE 和各会话明细的 GET 放进一个 Lua，返回 {会话 key, 明细} 列表。Java 里照原来解析：明细没了的照原来 ZREM 清理（很少），Server 节点在不在线照原来用 `isNodeOnline`（已有缓存）。每条 DR 7 次往返变 1 次，判断逻辑不变，结果和现在完全一样。

b. **同一批里只查一次**：同一批里同一个 (ossid, Server) 的结果记在线程上，这批处理完清掉。一批 100 条最多约 300 ms，相当于这批里后面的 DR 用的是这批开始时查到的会话情况。客户刚好在这段时间里断开最后一个会话，这几条 DR 还是发到原来那个 Server 的 `dr:pending`，和 DR 已经写进 `dr:pending` 之后客户才断开的情况一样，由 Server 那边处理（等客户重新 bind，或 Server 自己的回退）。只在客户断线这个异常情况下影响几百毫秒内的 DR，符合"只在异常时受影响"的要求。b 单独一个开关，默认打开，可以关掉只用 a。

### 3.4 暂不做的

- **写 `dr:pending` 和写标记合并（`produceOnce` + `StreamAttempt.flush`，33%）**：写标记要在写 stream 之前，现在是两次往返；合并要改产出 Lua 和标记的格式，影响 MT 和结果的消费，单独设计。
- **同一个分片多线程并发**：会打乱同一个分片里的顺序（同一条 MT 的中间 DR 和最终 DR），不做。并发用 `StreamShards` 调。

## 4. 会不会影响多个客户之间的发送

一批是一个分片 stream 里的 100 条，同一个通道（9981）的 DR，回给很多不同的客户。现在这 100 条就是一个线程按顺序处理的，一个客户的 DR 慢，同一批后面其他客户的也要等。一批获取不改变这个分组和顺序，只是把每条各自的查询提前合成一次：

| 情况 | 现在 | 一批获取后 |
|---|---|---|
| 正常 | 每条 1 次 GET，按顺序处理 | 每批每个主 1 次 MGET，按顺序处理，每条少等一次 Redis，同一批里所有客户的 DR 都更快 |
| 某个客户的 DR 量很大 | 和别的客户在同一批里排队 | 一样，只是每条更快 |
| MGET 失败（Redis 异常） | — | 退回每条 GET，和现在一样 |
| 早到的 DR（预取时没有匹配数据） | 走早到 DR 队列 | 处理时再 GET 一次，结果一样 |
| 客户在一批的几百毫秒内断开最后一个会话（只有 3.3 b） | 下一条 DR 就知道 | 这批剩下的 DR 还发给原来的 Server，由 Server 处理 |

MGET 本身：100 个 key 分到 2 个主，每个主约 50 个 key，一次在 0.1 ms 以内，不会让报告 Redis 卡住别的请求。

结论：除 3.3 b 在客户断线时的几百毫秒外，不影响任何客户的 DR，也不影响客户之间的先后。

## 5. 配置项（已写进《配置项说明》，都可热加载，默认 true）

| 配置项 | 默认 | 所在文件 | 生效 | 作用 |
|---|---|---|---|---|
| `CoreInboundDrBatchLookup` | `true` | GmmsConfig | 热加载 | 3.1 和 3.2：一批的 DR 匹配数据一次取、一次删；false 回到每条一次 |
| `SmppServerSessionRegistryLuaCheck` | `true` | GmmsConfig | 热加载 | 3.3 a：一个客户在一个 Server 上的会话一次 Lua 查完 |
| `SmppServerSessionRegistryBatchMemo` | `true` | GmmsConfig | 热加载 | 3.3 b：同一批里同一个客户、同一个 Server 只查一次 |

## 6. 测试

- **回归**：
  - 一批里有同一个 outMsgId 的中间 DR 和最终 DR；
  - 一批里有早到 DR（预取时没有、处理时有）；
  - MGET 失败退回；
  - 确认后删除失败；
  - 客户在一批中间断开最后一个会话（开关 b 开和关）；
  - 三个开关关掉后和现在逐条比较。
- **压测**：PF007 10 分钟（`PF007_4000tps_10m`），不配 `StreamShards`，对比：
  - 9981 各分片每秒消费数、`consumer.inbound_dr`；
  - 状态 Redis 的 GET 次数、报告 Redis 的命令数和 CPU；
  - jstack 里 DR 消费线程的分布。

  再跑 4000 TPS 1 小时（`PF007_4000tps_1h`），和 PF007H（`perf_long_PF007H_1124_2258`）比较。

## 7. 结果：4000 TPS 1 小时（2026-11-25）

`perf_long_PF007H_1125_0918`（3.1、3.2、3.3 a/b 都开），和改动前的 `perf_long_PF007H_1124_2258` 比较。两次用例相同（`PF007_4000tps_1h`，9981 默认 8 分片，不抓包，停 Redis 从库），只差这次的代码。平均值取预热后 55 分钟。

| 项 | 改动前 | 改动后 |
|---|---|---|
| 结论 | 积压增长 | 稳定 |
| 稳定期积压变化 | +1182/s | +0/s |
| `core:inbound-dr:9981` 最多 | 4,287,031 | 2,855（结束时 440） |
| 通道 DR → 客户 avg / p50 / p95 / p99 / max（ms） | 629012 / 60000 / 60000 / 60000 / 1351095 | 506 / 77 / 241 / 17540 / 52776 |
| 提交 → 客户 DR avg / p50 / p99（ms） | 679324 / 60000 / 60000 | 680 / 628 / 939 |
| 提交 → 通道 p50 / p95 / p99（ms） | 50 / 88 / 116 | 51 / 86 / 99 |
| 整机 CPU 空闲 中位 / 最低 | 26% / 9% | 29% / 12% |
| Core 三个 CPU（%） | 151 / 144 / 143 | 142 / 137 / 138 |
| Server1 / Server2 CPU（%） | 139 / 55 | 166 / 52 |
| 状态 Redis 8386 CPU / ops | 36% / 19.8k/s | 12% / 11.5k/s |
| 报告 Redis 8388、8398 CPU / ops | 89% / 94k/s | 91% / 110k/s |
| 报告 Redis 最高内存 | 2652 MB | 21 MB |

- 改动前的 p50 60000 是延迟统计的上限值，实际多数 DR 在流量结束后的回收阶段才发出。
- 改动后 9981 的 DR 跟得上（每秒约 3200 条），积压只在秒级波动。
- DR → 客户 p99 17.5 s 来自预热刚结束时的短时积压，稳定期 p95 是 241 ms。
- Server1 的 CPU 升高，是因为 DR 现在实时发给客户，不再堆在 Core 前面。
- 报告 Redis 的 CPU 没降，命令数反而多了 17%，因为它现在处理的是全部 DR 而不是 70%。粗算每条 DR 的报告 Redis 命令少了约 1/5（命令数里也有 MT 结果流的部分，只是估计）。CPU 在 91%，余量不多，再往上加 TPS 首先会卡在报告 Redis，需要再加主库或者继续合并 3.4 里的写入。
