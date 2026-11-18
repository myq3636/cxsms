# Redis 消息操作次数统计和可以省掉的操作

> 日期：2026-11-18。代码是 `5ddefd8`。目的：不拆 Redis，先减少每条消息的 Redis 操作，找出哪些不是必须的、可以在内存里解决或者合并。第 2、3 项已改（4.1）。

## 1. 测量方法

- 测试服务器，Redis 只留 `thcon`、MySQL 清空，a2p 重启后空闲 90 秒；
- 先抓 20 秒空闲时的 `MONITOR`（只有轮询），再在 100 TPS × 30 秒的负载下抓三个 Redis 的 `MONITOR`（用例 `cases/PF_OPS`，只有最常见的一种流量：rtcs1 → rtvs1 的单段普通 MT + DR）；
- 共 2995 条 MT，全部收到 submit_sm_resp 和 DR；
- 按（实例、模块、命令或 Lua 脚本、key 的模式）分组，算出平均每条 MT 几次；同一组空闲时每秒几次，用来区分"每条消息的操作"和"轮询"。结果目录 `/usr/local/a2p/log/redisops_*`，分析脚本在 scratchpad 的 `opsan.py`。

注意：

- 100 TPS 时每次读 stream 平均只读到 1～2 条，所以"每批一次"的操作（读 stream、门铃、未读检查、按批 CLAIM / ACK）在这里约等于每条一次；5000 TPS 时每批约 4 条以上，这部分会少很多（下表标 **批**）；
- 只统计了单段普通 MT。长短信、改道、重试、假 DR 会多一些操作，没有计入；
- 一次 Lua 调用（EVALSHA）算 1 次。5000 TPS 时 8387 上 EVALSHA 平均每次 25.8 µs，普通命令 2～4 µs（《Core处理MT慢的原因分析》2.2），所以**减少 Lua 调用的次数比减少普通命令更有用**。

## 2. 总数

| 实例 | 每条 MT 的命令 | 其中 Lua 调用 | 空闲时每秒（轮询） |
|---|---|---|---|
| 8386 state | 17.8 | 9.9（其中 7.9 是轮询） | 354 |
| 8387 submit | 20.0 | 9.4 | 212 |
| 8388 report | 31.2 | 13.9 | 222 |
| **合计** | **69** | **33** | 788 |

## 3. 逐项列表

"类别"：**必须** = 消息流本身需要，不能省；**批** = 每批一次，TPS 高时会少；**轮询** = 和消息数无关，空闲时也有。

### 3.1 8386 state

| 每条 MT | 谁 | 操作 | 用途（代码） | 类别 |
|---|---|---|---|---|
| 3.97 | Core | Lua：`zset:delay:outdr` 到期扫描 | 延时 DR（轮询，每个 Core 约 55 次 / 秒） | 轮询 |
| 3.96 | Core | Lua：`zset:fakedr:timeout` 到期扫描 | 假 DR 超时（轮询，同上） | 轮询 |
| 2.01 | Core | GET `smpp:server:session:...` | 发 DR 前判断客户的 bind 在哪个 Server 模块：每个 bind 读一次详情（`SmppServerDrRouteResolver` → `SmppServerSessionRegistry.hasActiveSessionInModule`） | **可以放内存** |
| 1.00 | Core | ZRANGE `smpp:server:active:{ossid}:{模块}` | 同上，先列出这个模块的 bind | **可以放内存** |
| 1.00 | Server | Lua：INCR + TTL + EXPIRE `throttle:in:{ssid}:{秒}` | 客户收侧限流，所有 Server 节点共用一个每秒计数（`ThrottlingControl.isAllowedToReceive`） | **可以放内存** |
| 1.00 + 1.00 | Core | SETEX `{outMsgId}` + HSET `DR{秒}` | submit 结果到达后，存"通道消息 ID → 原消息"，等通道 DR 来匹配（`MessageStoreManager`，`setPipeline`） | 必须 |
| 1.00 + 1.00 + 1.00 | Core | GET `{outMsgId}`，DEL + HDEL | 通道 DR 到达时找原消息，最终 DR 处理完删掉（`handleInboundDRInternal`、`deleteDrLookup`） | 必须（DR 可能到任何一个 Core，Core 重启后也要能找到） |

### 3.2 8387 submit

| 每条 MT | 谁 | 操作 | 用途 | 类别 |
|---|---|---|---|---|
| 1.00 | Server | Lua：XADD + SADD + ZADD（`xaddWithDoorbell`） | 收到的 MT 写进 `mt:pending`，加索引、按门铃 | 必须（但可以**按批写**，见 4.2） |
| 2.37 + 1.47 + 0.75 | Core | ZRANGE 门铃（`mt:pending`、`mqm:submit`、`core:hold`） | 调度线程找有消息的 stream | 轮询 |
| 1.75 | Core | XREADGROUP `mt:pending` | 读 MT | 批 |
| 0.88 | Core | Lua：XLEN + ZREM（取门铃） | stream 读空时摘掉门铃 | 批 |
| 0.87 | Core | Lua：XLEN + XPENDING（未读检查） | 一批处理完，判断要不要重按门铃 | 批（可以省，见 4.5） |
| 0.88 + 0.88 | Core | Lua：按批 CLAIM / ACK（`StreamAttempt.Batch`） | 崩溃后重投时不重复处理 | 批 |
| 0.99 | Core | Lua：`PRODUCE_ONCE`（HSET 标记、XADD `mt:routed`、SADD、ZADD 门铃、SETEX outbox 消息体和 meta、ZADD outbox 超时） | 写给 Client，同时记 outbox | 必须 |
| 1.00 | Core | Lua：outbox 认领（EXISTS、DEL、ZREM、SETEX `:done`） | submit 结果到了，清 outbox | 必须（outbox 在设计上要共享） |
| 1.86 | Client | ZRANGE 门铃 `mt:routed` | 轮询 | 轮询 |
| 1.76 + 0.88 | Client | XREADGROUP `mt:routed`，Lua 取门铃 | 读要发给通道的 MT | 批 |
| 1.00 | Client | Lua：发送标记 CLAIM（GET、SET） | 发之前记"我在发"，防止崩溃后重复发给通道（`ClientSendMarker`） | **可以按批** |
| 1.00 | Client | Lua：发送标记 ACK（EXISTS、SET、XACK、XDEL、DEL） | 收到 submit_sm_resp 后确认 | **可以按批**；脚本里先 SET 再 DEL 是多余的 |

### 3.3 8388 report

| 每条 MT | 谁 | 操作 | 用途 | 类别 |
|---|---|---|---|---|
| 1.00 | Client | Lua：XADD + SADD + ZADD `core:results` | 通道的 submit 结果写给 Core | 必须（可以**按批写**） |
| 1.00 | Client | Lua：XADD + SADD + ZADD `core:inbound-dr` | 通道 DR 写给 Core | 必须（可以**按批写**） |
| 3.10 + 3.24 | Core | ZPOPMIN 门铃 + XREADGROUP `core:results` | 读结果（每条 MT 两条结果：submit 结果、客户的 DR 结果） | 批；**每条结果读约 1.6 次，有空读** |
| 1.62 × 3 | Core | Lua：未读检查、按批 CLAIM、按批 ACK（results） | 同 8387 | 批 |
| **2.00** | Core | HSET `core:att:...results...` | 按批处理时处理完一条就写进度标记（`_done` 等），几毫秒后 ACK 时整个标记被 DEL | **可以放内存** |
| 2.35 + 1.47 | Core | ZRANGE 门铃（`core:inbound-dr`、`mqm:report`） | 轮询 | 轮询 |
| 1.76 + 0.88 × 4 | Core | 读、取门铃、未读检查、按批 CLAIM / ACK（`core:inbound-dr`） | 处理通道 DR | 批 |
| 1.00 | Core | Lua：`PRODUCE_ONCE` 写 `dr:pending` | 给客户的 DR 交给 Server | 必须 |
| **1.00** | Core | HSET `core:att:...inbound-dr...` | 同上面的进度标记 | **可以放内存** |
| 1.45 + 1.22 + 0.73 | Server | 读 `dr:pending`、ZRANGE 门铃、Lua 取门铃 | 读要发给客户的 DR | 批 / 轮询 |
| 0.99 | Server | Lua：XADD 结果 + XACK + XDEL（`RESULT_AND_ACK`） | 客户回了 deliver_sm_resp，写结果并确认 | 必须（可以**按批**） |

## 4. 可以省掉或合并的

按"正常消息不受影响，只有系统或 Redis 异常时才有影响"的标准。

| # | 改法 | 省掉的操作（每条 MT） | 只在什么异常时有影响 | 改动 |
|---|---|---|---|---|
| 1 | **Core 缓存客户 bind 在哪个 Server 模块**（`hasActiveSessionInModule` 的结果在内存里保留 1～2 秒，bind / unbind 时本节点立即失效） | 8386：GET 2 + ZRANGE 1，**都在 Core 处理 DR 的串行路径上** | 客户刚换了 bind 的 1～2 秒里，DR 可能先送到旧模块（要确认 Server 对没有 bind 的 DR 会转交或重试，现有 `DrTransfer`） | 小 |
| 2 | **收侧限流改为本地计数**：每个 Server 节点按"限额 × 本节点 bind 数 / 总 bind 数"在内存里限流；或者本地计数、每 100 ms 用一次 INCRBY 同步 | 8386：Lua 1 | Server 节点数或 bind 数变化的一瞬间，限流可能短时间偏松或偏紧 | 小 |
| 3 | **Core 按批处理时进度标记不写 Redis**：处理完一条只记在内存里，afterDone 的动作挪到 ACK 之后；ACK 时 DEL 标记（本来就会删） | 8388：HSET 3（results 2、inbound-dr 1） | 一条处理完到 ACK 之间（几毫秒）Core 崩溃：重投后再处理一次，**客户可能多收一次 DR** | 小 |
| 4 | **Client 发送标记按批**：CLAIM 按读到的一批一次 Lua（同 `StreamAttempt.Batch.claim`）；ACK 在内存里攒几毫秒或几十条，一次 Lua 确认；ACK 脚本改为先 XACK，确认成功直接 XDEL + DEL，不再先 SET | 8387：Lua 2 → 约 0.2～0.5 | Client 崩溃：攒着没确认的那几毫秒的 MT，标记是"在发"，重投时按现有规则不再发（同现在的崩溃处理），只是数量多几条 | 中 |
| 5 | **Server、Client 写 stream 按批**：同一个 stream 的写入在内存里攒 1～2 ms（或攒满 N 条），一次 Lua 写多条；Server 写成功以后才回 submit_sm_resp，Server 收到 deliver_sm_resp 后的 `RESULT_AND_ACK` 同样按批 | 8387：Lua 1 → 约 0.1；8388：Lua 3 → 约 0.3 | 正常消息只多 1～2 ms 延时；进程崩溃时还没写进去的那几毫秒的消息没有回 resp，客户会重发（和现在崩溃时一样） | 中 |
| 6 | **未读检查按读到的条数判断**：XREADGROUP 读满 COUNT 就直接重按门铃，没读满才检查（要先确认和生产者按门铃之间的竞态） | 每批 Lua 1（8387、8388 各种 stream） | 判断错了只会多一次空读或晚一点被自动认领扫到 | 中 |
| 7 | 结果 stream 的空读：每条结果读约 1.6 次，查清楚为什么 ZPOPMIN 弹出门铃后读空 | 8388：每条 MT 约 1～2 次 XREADGROUP / ZPOPMIN | — | 先查原因 |

### 4.1 已做（2026-11-18）

| # | 结果 |
|---|---|
| 1 | **不做**：访问的是 8386，8386 不是瓶颈 |
| 2 | **已改**：`ThrottleInSyncIntervalMs`（默认 500）。每个 Server 节点在内存里计数，每 500 ms 把本节点新增的数 INCRBY 到原来的 key `throttle:in:{ssid}:{秒}`，从返回的总数得到其他节点的数；每秒第一次同步之前，其他节点按它们上一秒的数等比例估算。收消息时不再访问 Redis。多个节点同时收同一个客户时，最多可能比限额多出其他节点在一个同步间隔里收的数；只有一个节点时和原来一样准。0 回到每条一次 INCR。代码：`ClusterSecondCounter`、`ThrottlingControl.isAllowedToReceive`；测试 `ThrottleSyncIT` |
| 2 | **已改**：去掉 Redis 全局开关 `thcon`。原来 SMPP 每 50 秒、3 个 HTTP 入口每个请求读一次，key 不存在时拒收所有消息；现在不再读，没有它也照常收（`ServerFuncIT` T-02 改为验证这一点） |
| 3 | **已改**：`CoreStreamAttemptBatchFlagsInMemory`（默认 true），见下 |

第 3 项会不会造成 submit 重复提交：**不会**。处理完写进 Redis 的标记里，和"重复写出消息"有关的有三类，这三类照常立即写：

- `p:{段}`：消息写进了**别的 Redis** 上的 stream。例如 submit 结果要重试、改道时，Core 把 MT 再写进 8387 的 `mt:routed`，而标记在 8388。同一个 Redis 上的写出（例如给客户的 DR 写进 `dr:pending`）在写出的 Lua 里就记下了，不靠这次 HSET；
- `delivery#`：投递到客户会话；
- `x:`：改道、重发，用 `markDoneNow` 立即写的。

另外 submit 结果处理前先认领 outbox，同一个结果重投时认领返回 STALE 直接丢掉，这是第二道保护。

其余标记（CDR、统计、DB 写入、给客户的 DR 等）和 `_done` 只放内存，几毫秒后这批确认时连同标记一起删掉；`afterDone`（如删除 DR 匹配数据）挪到确认之后执行。另外，这批从认领起超过 `CoreMtLeaseMs` 的一半（默认 30 秒）时也照常写，防止处理太慢被别的 Core 自动认领后重复处理。影响只在 Core 崩溃时：处理完到确认之间崩溃，重投后会再做一遍还没记下的副作用（客户可能多收一次 DR、多一条统计）。回归里的崩溃用例（AttemptBatchIT AB-04）实测 CDR 也没有重复，因为 CDR 的标记在给客户的 DR 写出时已经一起写进了 Redis。

### 4.2 不建议放内存的



- **DR 匹配数据**（8386 的 `{outMsgId}`、`DR{秒}`）：通道 DR 可能被任何一个 Core 处理，Core 重启后也要能匹配上；
- **outbox**（8387）：submit 结果可能被别的 Core 处理，Core 崩溃后也要能发现没有结果的消息；可以以后考虑不存整条消息体的副本，只是减少内存，Lua 调用次数不变；
- **Core 的 `PRODUCE_ONCE` 和 StreamAttempt 的按批 CLAIM / ACK**：防止崩溃后给通道重复发送（重复发送要花钱、用户会收到两条），已经按批；
- **轮询**（门铃 ZRANGE、延时 DR、假 DR 扫描）：空闲时三个 Redis 合计约 800 次 / 秒，和消息数无关，5000 TPS 时可以忽略。

## 5. 效果估计

按 5000 TPS 时 Lua 每次约 25 µs 粗算（每批的操作按每批 4 条算）：

| 实例 | 现在每条 MT 的 Lua | 做了 3～6 以后 | 说明 |
|---|---|---|---|
| 8387 | 约 6 | 约 3 | Server 按批写和 Client 标记按批省得最多；8387 每条 MT 的 Lua 开销大约减半 |
| 8388 | 约 7 | 约 3.5 | Client 写结果、DR，Server 写结果按批；外加少 3 次 HSET |
| 8386 | 2（另有轮询） | 约 1 | 第 1、2 项；8386 不是瓶颈，但这 4 次是 Core 处理 DR、Server 收 MT 路径上的同步往返，省掉以后处理更快 |

建议顺序：先做 1、2、3（改动小，风险小，各省几次往返），再做 4、5（收益最大，8387、8388 的 Lua 次数大约减半），6、7 先查清楚再定。做完 4、5 以后再跑一次 5000 TPS，看 Redis 主线程和 Core 的处理速度。
