# Server 发 DR 慢的原因分析

> 日期：2026-11-17。起因：5000 TPS 一小时测试（《5000TPS一小时测试瓶颈分析》）里，Server 给客户发 DR 每秒只有约 660 条，是全链路最慢的一段。本文只做分析，**没有改代码**。

## 1. 结论

**DR 本身发得并不慢，是客户回的 deliver_sm_resp 被堵在了 Server 没读的 submit_sm 后面。**

1. Server 发 DR 受 DR 窗口限制：每个客户会话最多 `WindowSize`（这次 1000）条 DR 在等 deliver_sm_resp，满了就延后 100 ms（`ServerDrWindowFullRetryMs`）。所以 DR 速率 ≈ 窗口 ÷ deliver_sm_resp 的往返时间。
2. 客户的 submit_sm 和 deliver_sm_resp 走**同一条 TCP 连接**（TR 方式 bind），Server 也是按顺序处理同一个会话收到的 PDU：
   - 每个会话一个 socket 读线程，把解析出的 PDU 放进这个会话的接收线程池（`CustReceiver_{ssid}`，核心 `MinReceiverNumber` 个线程，最多 `MaxReceiverNumber` 个，队列 `ThreadPool.MaxQueueSize` 8000，满了以后读线程自己执行 `A2PCallerRunsPolicy`，于是不再读 socket）；
   - **submit_sm 和 deliver_sm_resp 进同一个先进先出的队列**。
3. Server 收 submit 的速度被 Redis 8387 限住：每个 submit_sm 要同步地把 MT 写进 `stream:mt:pending`（`xaddWithDoorbell` 的 Lua），8387 在 95% 以上，每次调用都要等。
4. 测试工具发 submit **不受窗口限制**（不等 submit_sm_resp，一直写），Server 读不过来，数据就堆在 TCP 里。deliver_sm_resp 排在几万个 submit_sm 后面，十几秒以后才被 Server 读到，DR 窗口几乎一直是满的。

## 2. 证据（2026-11-17 18:43～18:45，5000 TPS 3 分钟，每 20 秒采样）

**socket 队列**（`ss -tnmi`，客户 rtcs1 的两个连接）：

| 时间 | 工具一侧未发出（Send-Q） | Server 一侧未读（Recv-Q） |
|---|---|---|
| 18:43:20 | 3,643,850 / 3,614,960 字节 | 922,265 字节 |
| 18:43:41 | 3,594,496 / 3,640,106 | 954,174 |
| 18:44:02 | 3,641,726 / 3,607,119 | 926,121 |
| 18:44:22 | 3,631,460 / 3,638,997 | 942,630 |

即每个连接上 Server 前面约有 4.5 MB（按每个 submit_sm 约 150 字节，约 3 万个）没处理，工具此刻回的 deliver_sm_resp 排在它们后面。

**Server1 的线程**（`jcmd Thread.print`）：客户 rtcs1 的接收线程 `CustReceiver_9995` 共 12 个，**每次采样都全部停在 `RedisConnection.evalCached ← xaddWithDoorbell ← RedisClient.xaddWithDoorbellSubmitMq`**，即在等 8387 写 `mt:pending`。

**测试工具的线程**：发送线程（`main`）停在 socket 写（TCP 发送缓冲满）；rtcs1 的读线程要回 deliver_sm_resp，停在 `Customer.send` 等同一个连接的锁。即便没有这把锁，resp 也要在 TCP 里排在那 3.6 MB 后面。

**DR 发送和窗口**（Server1 日志，每 5 分钟取一分钟）：每分钟发出 DR（`[DR-OUT]`）约 1.9 万～2.7 万条，即 320～440 条/秒；每分钟 DR 窗口满约 1300～2600 次。

**结果**：这 3 分钟里"通道回 DR → 客户收到"的中位数 21.5 秒，"客户提交 → 客户收到 DR"中位数 17 秒。

## 3. 和真实客户的差别

真实的 SMPP 客户一般有发送窗口（在途的 submit_sm 不超过某个数，等到 submit_sm_resp 才继续发），Server 读得慢时客户也跟着慢下来，TCP 里最多积压一个窗口的 submit。这样 deliver_sm_resp 前面最多排一个窗口（例如 1000 个 submit），等待时间是秒级以下，DR 速率会比这次高很多。

这次的测试工具不限窗口，**放大了这个问题**。但真实客户也可能窗口很大，问题的方向是一样的：只要 Server 收 submit 慢，同一连接上的 deliver_sm_resp 就会被拖慢。

## 4. 处理办法（建议，都没有做）

| # | 办法 | 改什么 | 作用 |
|---|---|---|---|
| 1 | **打开共享收包池** `SmppServerInboundSharedPoolEnable=true`（代码默认就是 true，这台服务器的 GmmsConfig 设成了 false） | 配置，新连接生效 | 收到的响应（deliver_sm_resp）、enquire_link、unbind 进单独的优先队列，先处理，不受 submit 积压影响；某个客户的队列满了回 submit_sm `ESME_RTHROTTLED`（`SmppServerInboundQueueFullCommandStatus`），socket 继续读，不会堵在 TCP 里。**直接解决这次的问题** |
| 2 | 测试工具发 submit 按窗口（例如 `WindowSize` 条在途），收到 submit_sm_resp 再发 | 测试工具 | 更接近真实客户，压测结果更可信 |
| 3 | 加大客户的 `WindowSize`（DR 窗口） | cm.cfg | 只能缓解：等待 20 秒时窗口要 2 万才能到 1000 条/秒 |
| 4 | 降低 Server 收 submit 的 Redis 开销：一批 submit 合成一次写入（pipeline 或一次 Lua 写多条），限流计数（`incrWithExpire`）和写 stream 合并 | 代码 | 提高收 submit 的速度（和《Redis CPU瓶颈分析》的方向一致） |

建议先做 1（只改配置），用同样的方法（3 分钟 5000 TPS + 采样）验证：预期 Server 一侧的 Recv-Q 不再积压，DR 窗口满明显减少，DR 速率接近 Core 产生 DR 的速度；超出处理能力的 submit 会收到 `ESME_RTHROTTLED`。

## 5. 修改和验证（2026-11-17 19:00）

修改：

- 测试服务器 `Gmms/GmmsConfig.properties`：`SmppServerInboundSharedPoolEnable` false → **true**（改前备份 `GmmsConfig.properties.bak.20261117_sharedpool`），其他共享收包池的参数不变（`SmppServerInboundWorkerMax` 300、`SmppServerInboundMaxInFlightPerSsid` 100、`SmppServerInboundPerSsidQueueSize` 50000、队列满回 `ESME_RTHROTTLED`）；
- PF002 用例（`cases/PF002_5000tps_1h/cm.extra.cfg`）三个测试客户 RTCS1～3 的 `WindowSize` 1000 → **5000**（通道 RTVS 仍是 1000）。

同样的 3 分钟 5000 TPS 测量对比：

| | 修改前（18:43） | 修改后（19:02） |
|---|---|---|
| Server 一侧 socket 未读（Recv-Q） | 约 92 万字节，一直积压 | **0** |
| 工具发出 | 82.3 万条（计划 90 万，被 TCP 顶住） | **90.0 万条（100%）**，submit_sm_resp 全部成功，没有被限流 |
| Server1 每分钟发出 DR | 约 2.0 万～2.7 万（320～440/秒） | **约 6.7 万（约 1110/秒）** |
| Server1 每分钟 DR 窗口满 | 1300～2600 次 | **0** |
| 通道回 DR → 客户收到，中位数 / p95 | 21.5 秒 / 60 秒以上 | **0.26 秒 / 14 秒** |
| 客户提交 → 通道收到，中位数 | 60 秒以上 | 0.57 秒（p95 仍 60 秒以上） |
| 结束时没收到 DR 的 MT | 未统计 | 56 万（共 90 万） |

**结论**：DR 被 submit 堵住的问题解决了。Server 收 submit 也更快（共享收包池最多 300 个 worker 并行写 Redis），客户的 6000 段/秒全部收进 `mt:pending`。**瓶颈移到了 Core 处理 MT**（受 Redis 8387 限制），MT 在 `mt:pending` 里积压；DR 能发多少，取决于 Core 处理完多少 MT。
