# Server 第 1 批优化设计：共享收包池默认开启、DR 窗口满不重复发送、accept 加固与连接限制

问题清单见《SMPP-Server架构分析与改进》。按决定，计费问题（B1、B2）不改。

## 1. 问题

| # | 现象 | 原因 |
|---|---|---|
| T1 | 每个连接 4 到 8 个线程，2 万个连接就是 8 万到 16 万个线程 | 共享收包池默认关闭（`SmppServerInboundSharedPoolEnable=false`），每个连接都有自己的收包线程池（1 到 5 个线程） |
| Q1 | 共享池满时，分发线程自己处理 PDU，这段时间所有客户的分发都停了 | worker 池使用 CallerRuns 策略 |
| Q1' | 配置的 300 个 worker 平时只用到 100 个 | Java 线程池只有在队列满（1000）之后才会创建超过核心数（100）的线程 |
| Q1'' | 客户的 submit 积压时，它的 deliver_sm_resp、enquire_link 要排在几万条 submit 后面 | 所有 PDU 共用一个按客户分的队列 |
| Q2/Q3 | 3 个客户就能占满 300 个 worker；全局积压上限 200 万条，内存风险大 | 默认值偏大 |
| 新 | 有积压时，每条消息都会打一条"等待太久"的 WARN 日志，日志反过来拖慢处理（测试中发现） | 没有限频 |
| 新 | DR 窗口满后重复给客户发 DR，Core 也会多写一条 IN_DELI_REP CDR | 窗口满后的重试用 `XAUTOCLAIM minIdle=100ms` 扫描整个 stream，会把新任务正在处理的 DR 一起领走 |
| D1/D2 | 实际只有 20 个 DR worker；大客户长期占着 worker；池满时分发线程自己执行任务 | 同 Q1' 的线程池问题；任务读到 stream 空才退出；CallerRuns |
| T2 | accept 线程遇到 OOM（如 `unable to create native thread`）就退出，端口不再接受连接 | 只捕获了 `IOException` |
| T3 | 没有连接数上限；未 bind 的连接要等 600 秒才关闭 | 没有相应配置 |

## 2. 方案

### 2.1 共享收包池

| 改动 | 说明 |
|---|---|
| 默认开启 | `SmppServerInboundSharedPoolEnable` 默认值从 false 改为 true。是否使用共享池在会话建立时决定，修改后需要重启 Server |
| worker 池 | 核心线程数 = 最大线程数 = `SmppServerInboundWorkerMax`，空闲 60 秒回收；`SmppServerInboundWorkerCore` 不再生效。默认值从 300 改为 **50**（与 `Redis_pool_maxActive` 默认值相同，依据见测试报告的性能测试） |
| 池满 | 拒绝（AbortPolicy）。分发线程把 PDU 放回这个客户队列的**队首**，同一客户的顺序不变；休眠 1 毫秒后继续分发，自己从不处理 PDU |
| 优先通道 | 响应类 PDU（deliver_sm_resp 等）、enquire_link、unbind 进入每个客户的优先通道：先于 submit 分发，不计入队列长度和同时处理数，也不会因为队列满被拒绝 |
| 日志限频 | "任务等待太久"的 WARN，同一客户每 10 秒最多一条，并带上这段时间内的条数 |
| 默认值 | `SmppServerInboundMaxInFlightPerSsid` 100 → **20**；`SmppServerInboundPerSsidQueueSize` 50000 → 10000；`SmppServerInboundGlobalQueueLimit` 2000000 → 200000；`SmppServerInboundWorkerQueue` 1000 不变。20 和 50 是性能测试后的推荐值（原计划是 50 和 300） |

客户队列满或全局积压达到上限时，与原来一样回 `SmppServerInboundQueueFullCommandStatus`（默认 `ESME_RTHROTTLED`），由客户稍后重发。

### 2.2 DR 窗口满不再重复发送

复用 Client 第 1 批的 `StreamDeferrals`：

- 窗口满时，只把本任务**还没发出**的 DR 放进延后列表。过 `ServerDrWindowFullRetryMs`（100 毫秒）后，先用 `XCLAIM ... JUSTID` 保留这些 DR（不增加投递次数），再继续发送。
- 延后期间，这个 stream 不启动其他任务。门铃只做记录，等延后的任务结束后再重新挂上。
- 不再扫描整个 stream，也就不会把别的任务正在处理的 DR 领走。
- `ServerDrWindowFullRetryBatchSize` 不再使用。

### 2.3 DR 消费者线程池

- 核心线程数 = 最大线程数（`RedisStreamConsumer.ServerDrToCustomer.WorkerMaxPoolSize`，默认 50），配置的最大值能真正用上。
- 线程池满时，把 stream 放回门铃，不再由分发线程执行。
- 任务处理 `ServerStreamTaskMaxBatches`（10）批或 `ServerStreamTaskMaxMs`（2000 毫秒）后让出 stream，避免大客户长期占着 worker。

### 2.4 accept 加固与连接限制（SMPP 和 SMPP over TLS 两个端口）

| 改动 | 配置 | 默认值 |
|---|---|---|
| accept 循环捕获 `Throwable`。会话创建失败时关闭这个连接、记录错误日志、等待 100 毫秒后继续 accept | — | — |
| 全局最大连接数，超过则 accept 后立即关闭 | `SmppServerMaxConnections` | 0（不限） |
| 单个 IP 的最大连接数 | `SmppServerMaxConnectionsPerIp` | 0（不限） |
| 从 accept 到 bind 的最长时间，超时关闭（原来只看 `MaxSilentTime`，默认 600 秒） | `SmppServerBindTimeoutMs` | 30000（0 = 不单独限制） |

连接按 socket 计数，会话关闭了 socket 之后就不再计入。

## 3. 不在本批范围

- 计费（B1、B2）：按决定不改。
- 线程模型（每个连接的 Receiver、Sender、ServerSessionThread 共 3 个线程）、有界发送队列、DR pending 的内存模式、客户短暂断线时 DR 的宽限期：留到 Server 第 2 批。
