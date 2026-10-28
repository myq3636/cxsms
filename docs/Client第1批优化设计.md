# Client 第 1 批优化设计：外发不重复、限流不阻塞、低速通道不影响其他通道

## 1. 问题

问题清单见《Client模块架构分析与改进》的 3.1 和 3.2 节。本批解决 D1、D2、D3、T2，以及线程池的使用问题。

| # | 现象 | 原因 |
|---|---|---|
| D1 | 窗口满后，同一个 stream 中正在被别的任务处理的消息被领走再发一次 | 窗口满后的重试用 `XAUTOCLAIM minIdle=100ms` 扫描整个 stream，同时门铃又为这个 stream 启动了新任务 |
| D2 | 慢通道的消息被自动重投领走再发一次 | 一次读 100 条，再逐条阻塞等令牌。读出的消息排队超过 60 秒（`RedisStreamAutoClaimIdleMs`），就被自动重投领走；原来的 worker 轮到它时也会发。只要平均到每个分片每秒不到 1.7 条就会触发，例如 16 个分片都有积压、TPS 低于约 27 的通道 |
| D3 | 发出后确认消费失败或 Client 重启，消息被再发一次 | 发出和确认消费是两步，中间没有去重 |
| T2 | 低速通道占住 worker，其他通道发不出去 | 限流用阻塞的 `acquire()`；worker 处理一个 stream 直到读空才退出 |
| P1 | 配置的 200 个 worker 实际只用到 50 个 | Java 线程池只有在队列满（2000）后才会创建超过核心数的线程 |
| P2 | 极端情况下所有通道一起停 | 线程池满时 CallerRuns，分发线程自己执行任务，也睡在限流器上 |

## 2. 方案

### 2.1 外发去重标记（解决 D1、D2、D3）

新增 `ClientSendMarker`。标记 key 为 `client:sent:{msgId}:{段号}:{重试次数}`，与 Core outbox 一样，每次外发一个标记。标记放在 submitMq 上，与路由 stream 在同一个实例；TTL 为 1 天。

**发送前**，用一段 Lua 判断标记（处理者 = consumer + JVM 标识 + 线程，重启后的 JVM 不会把别人的标记当成自己的）：

| 标记 | 处理 |
|---|---|
| 不存在 | 写入"发送中"（处理者和时间），然后发送 |
| 已发出（`D`） | 上次已经发出，只是确认消费失败：只确认消费 |
| 发送中，是自己的 | 重新占用，然后发送 |
| 发送中，是别人的，未超过 `ClientSendLeaseMs`（60 秒） | 别人正在发：不发、不确认消费，留在待确认列表 |
| 发送中，是别人的，已超过占用期 | 处理者在发送时停止，不知道供应商是否收到。**不重发**，只确认消费；结果由 Core outbox 兜底（超过 `CoreOutboundRespTimeoutMs` 没有结果就按 2700 处理） |

**发送成功后**，用一段 Lua 完成：标记改为已发出 → XACK → 确认成功则 XDEL 并删除标记。整段 Lua 失败时，标记仍是"发送中"，占用期过后按"不重发"处理。

**没有发出时**：
- 窗口满：释放标记。
- 被拒绝或会话断开：回失败结果，然后确认消费并删除标记；如果失败结果没写成功，就释放标记并留在待确认列表。

**其他规则**：
- **会话断开时**，也要先判断标记。被重投、其实已经发出的消息，不能再给 Core 回一个失败结果。
- **发送成功后再出异常**（例如确认消费时 Redis 出错），不再回失败结果，由重投时的标记处理。
- **标记读不到**（Redis 出错）时不发送，留给自动重投。
- **HTTP 外发**（`stream:http:submit:*`）使用同样的标记。推送给 HTTP 客户的 DR（`stream:http:dr:*`）不在本批范围。
- **开关** `ClientSendIdempotency`：默认 true，false 时恢复原来的行为。每条外发多 1 次 Redis 访问（判断标记）；确认消费改为 Lua，访问次数不变。

### 2.2 限流不阻塞，读出的消息延后处理（解决 D1、D2、T2）

`OutgoingThrottlingNum`（cm.cfg 中的 `SMSOptionOutgoingThrottlingNum`）保持原来的含义：**每个 Client 节点**的 TPS，用本地 Guava 限流器，节点之间不分摊。

- **取令牌改为 `tryAcquire`**，拿不到就不等待。
- **延后处理**：窗口满或拿不到令牌时，本批剩下的消息按 stream 放进延后列表，worker 立即释放。
  - 延后时间：拿不到令牌时约为一个令牌的间隔（1000 / TPS 毫秒，限制在 10 毫秒到 1 秒之间）；窗口满时为 `ClientOutboundWindowFullRetryMs`。
  - 时间到后启动一个任务，先处理这些消息，再接着读这个 stream 的新消息。
  - 处理前用 `XCLAIM ... JUSTID` 重置这些消息的空闲时间，自动重投不会把它们领走。这种方式不增加投递次数，不会因为反复延后而进入死信队列。已经被确认消费的消息不会被领回，直接跳过。
  - 延后期间，这个 stream 不启动其他任务。门铃只做记录，等延后的任务结束后再重新挂上。
  - **不再扫描整个 stream**（去掉 `XAUTOCLAIM minIdle=100ms`），D1 的根因就没有了。
- **按速率读取**：限速通道每次最多读 `min(BatchSize, TPS)` 条，即大约 1 秒的量。
- **自动重投领到的消息**遇到窗口满或没有令牌时，也放进同一个延后列表。

### 2.3 线程池（解决 P1、P2）

- Client 的外发和 HTTP 消费者改为**核心线程数 = 最大线程数**，空闲 60 秒后回收。需要时就能用到 `WorkerMaxPoolSize` 个线程，不必等队列满。
- 线程池满时直接拒绝，分发线程把 stream 放回门铃（与 Core 第 4 批相同），不再自己执行任务。
- 任务处理一定量后让出 stream：`ClientStreamTaskMaxBatches`（10 批）或 `ClientStreamTaskMaxMs`（2000 毫秒），先到为准。
- HTTP 外发和 HTTP DR 推送每次读取的默认条数从 100 改为 10。HTTP 是同步调用，单条最长可能要 90 秒，少读一些可以避免同批后面的消息排队超时。如果线上配置了 `RedisStreamConsumer.Client.BatchSize`，它会覆盖这个默认值，需要另外为 HTTP 配置 `RedisStreamConsumer.HttpClientSubmit.BatchSize`。

### 2.4 配置项

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `ClientSendIdempotency` | true | 外发去重标记 |
| `ClientSendLeaseMs` | 60000 | "发送中"的占用期（毫秒）；超过后视为发送者已停止，不重发 |
| `ClientStreamTaskMaxBatches` | 10 | 一个任务处理多少批后让出 stream |
| `ClientStreamTaskMaxMs` | 2000 | 一个任务处理多久后让出 stream |
| `RedisStreamConsumer.HttpClientSubmit.BatchSize` / `RedisStreamConsumer.HttpClientDR.BatchSize` | 10（原 100） | HTTP 外发和 HTTP DR 推送每次读取的条数（只改了默认值） |

以上配置都在 GmmsConfig 中，支持热加载。`RedisStreamConsumer.Client*.WorkerCorePoolSize` 对 Client 的外发和 HTTP 消费者不再生效，只看 `WorkerMaxPoolSize`。

## 3. 不在本批范围

- 多个 Client 节点共享限流额度：按约定，`OutgoingThrottlingNum` 就是单个节点的 TPS。
- 会话断开时先等待一段时间再判失败（S1）、HTTP 连接池和超时（S2）、HTTP 限流（T3）。
- 协议层面的重复：submit_resp 超时或会话断开时，供应商其实已经收到。是否重发由通道的 `SMSOptionSubmitRetryCode` 决定，建议不要把超时类和断连类的错误码配进去。
