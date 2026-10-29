# Server 第 1 批：测试用例与执行报告（共享收包池、DR 窗口满不重复、accept 与连接限制，含性能测试与推荐值）

> 设计见《Server第1批优化设计》。同时对 Core 前 7 批、通道限速和 Client 第 1、2 批做了全量回归。

## 1. 测试环境

- 本地 Redis 7.2.5，JDK 17，没有数据库。机器为 8 核、6 GB 内存的 WSL2。
- 在测试进程里启动真实的 `MultiSmppServer`（模块名 `MultiSmppServer1`），包括真实的 bind 鉴权、收包、submit 处理和 DR 消费。
- **测试客户端**：测试代码里写了一个最小的 SMPP 3.4 客户端（`SmppTestClient`），直接走 TCP，支持 bind_transceiver、异步 submit_sm（自带窗口）、enquire_link、应答 deliver_sm（可以设置应答延迟，用来模拟慢客户）。
- **测试客户**：cm.cfg 中配置了 SMPPCUST20 到 SMPPCUST39，`ConnectionType: MC`，`IncomingConnection` 允许 200 个会话，`WindowSize: 20`。
- **全局开关**：Redis 的 `thcon` 键必须存在，否则所有 submit 都会被回 `ESME_RTHROTTLED`。测试中设置了它。

## 2. 功能用例（`ServerBatch1IT`）

为了让线程池真正被打满，测试中把 `SmppServerInboundWorkerMax` 设为 2、`SmppServerInboundWorkerQueue` 设为 1。

| ID | 场景 | 预期 | 级别 |
|---|---|---|---|
| S1-01 | 不配置 `SmppServerInboundSharedPoolEnable` | 默认走共享池：submit_sm 正常回 ESME_ROK；没有每连接的收包线程（`CustReceiver_*`），只有共享 worker | P0 |
| S1-02 | worker 池配置 | 核心数 = 最大数 = 配置值；池满时拒绝，不使用 CallerRuns | P0 |
| S1-03 | 2 个 worker，5 个客户各发 200 条 | 全部 1000 条回 ESME_ROK，0 错误；池满时任务被放回队列重试（统计到上千次）；抽样 500 多次线程栈，分发线程从未自己处理 PDU | P0 |
| S1-04 | 某客户每次只允许处理 1 条，Redis 暂停 2 秒（用 2 秒的 Lua 脚本阻塞 Redis），此时这个客户有 29 条 submit 在排队，再发一个 enquire_link | enquire_link 在几毫秒内得到响应（走优先通道），submit 积压稍后处理完。暂停期间正在处理的那 1 到 2 条会因为 Redis 读超时回 ESME_RSYSERR，这是预期的 | P0 |
| S1-05 | 只建立 TCP 连接，不 bind（`SmppServerBindTimeoutMs=2000`） | 约 2 到 3 秒后连接被关闭（检查间隔为 1 秒） | P0 |
| S1-06 | `SmppServerMaxConnectionsPerIp=3` | 同一个 IP 的第 4 个连接被立即关闭；关闭一个连接后，新连接可以正常 bind | P0 |
| S1-07 | `SmppServerMaxConnections=1`，且已有连接 | 新连接被立即关闭 | P1 |
| S1-08 | 慢客户（每个 deliver_sm 30 毫秒后才应答，窗口 20），给它发 120 条 DR | 120 条全部送达，**每条恰好一次**；没有消息进入死信队列 | P0 |

## 3. 性能测试（`ServerPerf`）

### 3.1 方法

- 每种配置单独启动一个 JVM，客户端和 Server 在同一个进程里。
- 每次预热 5 秒，测量 15 秒。
- 日志级别调到 WARN，避免日志本身影响测量。
- 客户端每个连接用一个线程，保持指定数量的 submit_sm 在途（窗口），记录每条 submit_sm_resp 的时延。

**环境的限制：**
- 本机 Redis 执行这个 submit 用的 Lua 脚本（XADD + ZADD），`redis-benchmark` 测出来只有约 **6100 次/秒**（50 个并发，p50 约 7 毫秒）；正常的服务器通常要高一个数量级。
- 每条 submit 只访问 Redis 一次（已用 Redis 命令统计确认：每条 1 次 EVAL）。所以本机的吞吐被 Redis 往返时间卡在约 1000 到 1400 TPS。

**因此，下面的绝对数值不代表线上能力，只用来比较不同配置之间的差异。**

### 3.2 结果

**A/B：共享池 vs 每连接线程池**

| 负载 | 模式 | TPS | p50 | p95 | p99 | Server 线程数 |
|---|---|---|---|---|---|---|
| 20 连接 × 窗口 10 | 共享池（300 worker） | 1195 | 61 ms | 615 ms | 1043 ms | 362 |
| 20 连接 × 窗口 10 | 每连接线程池 | 1437 | 111 ms | 311 ms | 486 ms | 160 |
| 100 连接 × 窗口 20 | 共享池（300 worker） | 1164 | 1546 ms | 2806 ms | 3681 ms | 602 |
| 100 连接 × 窗口 20 | 每连接线程池 | 1223 | 1417 ms | 3574 ms | 5193 ms | 800 |

**C/D：worker 数与 Redis 连接池（100 连接 × 窗口 20）**

| Redis 连接池 | WorkerMax | TPS | p50 | p95 | p99 | 线程数 |
|---|---|---|---|---|---|---|
| 50 | 20 | 1245 | 1608 ms | 1941 ms | 2115 ms | 322 |
| 50 | **50** | **1294** | 1511 ms | **1850 ms** | **1926 ms** | 352 |
| 50 | 100 | 1032 | 1925 ms | 2359 ms | 2576 ms | 402 |
| 50 | 300 | 1164 | 1546 ms | 2806 ms | 3681 ms | 602 |
| 100 | 100 | 1156 | 1720 ms | 2010 ms | 2165 ms | 402 |
| 100 | 300 | 1039 | 1715 ms | 2933 ms | 3571 ms | 602 |

**E：公平性。** 1 个大客户 10 个连接、窗口 200 持续压满；同时 19 个小客户各 1 个连接、每秒 20 条。

| 模式 | WorkerMax | MaxInFlightPerSsid | 大客户 TPS | 小客户 p50 | 小客户 p99 | 小客户最大时延 |
|---|---|---|---|---|---|---|
| 共享池 | 300 | 100（原默认） | 911 | 73 ms | 837 ms | 1683 ms |
| 共享池 | 300 | 50 | 928 | 59 ms | 224 ms | 381 ms |
| 共享池 | 300 | 20 | 853 | 30 ms | 97 ms | 142 ms |
| **共享池** | **50** | **20** | **963** | **27 ms** | **75 ms** | **129 ms** |
| 每连接线程池 | — | — | 1024 | 56 ms | 273 ms | 493 ms |

**以推荐配置（WorkerMax=50、MaxInFlightPerSsid=20）复测：**

| 负载 | TPS | p50 | p95 | p99 | 线程数 |
|---|---|---|---|---|---|
| 20 连接 × 窗口 10 | 1368 | 139 ms | 200 ms | 271 ms | 112 |
| 100 连接 × 窗口 20 | 1284 | 1548 ms | 1877 ms | 2006 ms | 352 |

**F：1000 个已 bind 的空闲连接**

| 模式 | Server 线程数 | 每连接线程数 | 堆 |
|---|---|---|---|
| 共享池 | 3000 | 3.0 | 58 MB |
| 每连接线程池 | 4000 | 4.0 | 65 MB |

测试客户的 `MinReceiverNumber` 是 1，所以每连接线程池模式空闲时是 4 个线程/连接。有负载时，每个连接的线程池会扩到 `MaxReceiverNumber`（测试中是 5），也就是最多 8 个线程/连接；共享池模式始终是 3 个线程/连接加上固定的 worker。

### 3.3 结论与推荐值

1. **吞吐**：两种模式差不多，都被 Redis 往返时间限制。共享池的价值在于线程数与连接数脱钩（F），以及客户之间的隔离（E）。
2. **worker 数不是越多越好。** 每个 worker 处理 submit 时要占用一个 Redis 连接。worker 多于 Redis 连接池（`Redis_pool_maxActive`，默认 50）时，多出来的 worker 只能排队等连接：TPS 不升反降，p99 从 1.9 秒恶化到 3.7 秒（C）。**WorkerMax 等于 Redis 连接池大小时最好**；连接池调到 100 也没有带来提升（D），说明本机的瓶颈在 Redis 本身。
3. **MaxInFlightPerSsid 越小，小客户越不受大客户影响。** 从 100 降到 20，小客户的 p99 从 837 毫秒降到 75 到 97 毫秒，大客户的吞吐基本不变（E）。原因是大客户积压时，它能同时占用的 worker 被限制住，小客户的消息很快就能轮到 worker。
4. 推荐配置（WorkerMax=50、MaxInFlightPerSsid=20）在吞吐、尾部时延、公平性上**都是最好或接近最好**，比每连接线程池模式的小客户 p99（273 毫秒）也好得多。

**推荐值（已经改为代码默认值）：**

| 配置 | 推荐值 | 说明 |
|---|---|---|
| `SmppServerInboundSharedPoolEnable` | `true` | — |
| `SmppServerInboundWorkerMax` | **等于 `Redis_pool_maxActive`**，默认 50 | 要提高单节点吞吐，就把两者一起调大，比如都调到 100；只调大 worker 数没有用 |
| `SmppServerInboundMaxInFlightPerSsid` | **20** | 线上 Redis 往返时间约 1 毫秒时，单个客户在一个节点上仍可达到约 2 万 TPS（20 ÷ 1 毫秒），足够使用 |
| `SmppServerInboundPerSsidQueueSize` | 10000 | 单个客户最多积压 1 万条，超过就回限流 |
| `SmppServerInboundGlobalQueueLimit` | 200000 | — |
| `SmppServerInboundWorkerQueue` | 1000 | 不建议调大，积压应该留在按客户的队列里 |
| `SmppServerBindTimeoutMs` | 30000 | — |
| `SmppServerMaxConnections` / `SmppServerMaxConnectionsPerIp` | 按机器容量配置 | 例如单机按"线程上限 ÷ 3"估算总连接数；单 IP 上限按客户实际的最大连接数加一些余量 |

## 4. 不在本地范围的项

| ID | 项 | 建议做法 |
|---|---|---|
| X-S11 | 线上 Redis 下的真实吞吐 | 在测试环境用同样的方法压测（`ServerPerf` 的 tps 和 fair 场景），分别试 `WorkerMax = Redis_pool_maxActive = 50` 和 `100`，再选定 |
| X-S12 | 2 万个连接 | 测试环境建立 2 万个连接，观察线程数（约 6 万）、内存，以及 `SmppServerMaxConnections` 是否按预期拒绝 |
| X-S13 | accept 线程遇到 OOM | 用 `ulimit -u` 压低线程上限后不断建立连接，确认端口一直可以连接，日志里有 `failed to create a session` |
| X-S14 | 真实客户的 DR 窗口满 | 观察 `stream.server.dr.window_full` 指标，确认客户不再收到重复 DR，Core 的 IN_DELI_REP CDR 与发出的 DR 数量一致 |

## 5. 回归结果

执行时间 2026-10-29 19:12（AEDT）；Redis 7.2.5、OpenJDK 17.0.21；`run-all.sh` 从头全量编译后执行 17 个套件。

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
| ConfigReloadIT | 58 | 58 | 0 |
| Batch4IT | 25 | 25 | 0 |
| Batch5IT | 19 | 19 | 0 |
| Batch6IT | 22 | 22 | 0 |
| Batch7IT | 39 | 39 | 0 |
| ClientBatch1IT | 19 | 19 | 0 |
| ClientBatch2IT | 12 | 12 | 0 |
| **ServerBatch1IT（新增）** | 16 | 16 | 0 |
| **合计** | **363** | **363** | **0** |

## 6. 执行过程中的发现和调整

| 问题 | 原因 | 处理 |
|---|---|---|
| 性能测试中，每条积压的消息都打一条 WARN"等待太久"，日志锁竞争反过来拖慢了处理 | 原代码对这条日志没有限频；线上有积压时同样会发生 | **代码修正**：同一客户每 10 秒最多一条，并带上这段时间的条数 |
| 第一版 S1-04b 断言积压全部回 ESME_ROK，但失败了 | 测试用 2 秒的 Lua 阻塞 Redis，正在处理的 submit 超过 Jedis 的 2 秒读超时，回了 ESME_RSYSERR | 改为检查 30 条都得到了响应 |
| 最初的性能结果只有约 1000 TPS，时延达到秒级 | 通过线程栈确认：worker 几乎都在等 Redis 响应；`redis-benchmark` 证明本机 Redis 本身就慢 | 性能结论改为只做相对比较，并据此得出"WorkerMax 等于 Redis 连接池大小"的推荐 |
| 第一次试跑时，submit 全部回 0x58（ESME_RTHROTTLED） | Redis 里缺少全局开关 `thcon` | 测试中设置 `thcon` |
