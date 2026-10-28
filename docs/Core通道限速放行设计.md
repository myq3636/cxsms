# Core 通道限速放行设计

> 目的：客户提交速率远大于通道（供应商）速率时（例如客户每秒 5000 条、通道每秒 1 条），由 Core 按通道配置的速率放行，积压留在 Core 的 Redis 暂存队列里，`stream:mt:routed` 和 Client 侧不再积压。
> 状态：设计已确认，未实现。计划排在第 3 批（MT 幂等与 outbox 改造）之后。

## 1. 现状问题

| 环节 | 问题 |
|---|---|
| Core 路由 | 选出 rssid 后直接写 `stream:mt:routed`，不看通道能力 |
| Redis | `XADD MAXLEN ~ 1000000` 会删除最老的消息，不管有没有被消费过，造成静默丢失 |
| Client | 一次读 100 条后逐条阻塞等限流；低速通道上消息在待确认列表里放超过 60 秒，被自动重投领走后重复下发 |
| Client | 每个节点一个本地限流器，N 个节点合起来是 N 倍速率 |
| 过期 | 发送前不检查过期，已经失效的验证码照样发出去 |

## 2. 方案

```
stream:mt:pending:{ossid}:{shard}
   │ MTStreamConsumer（正常速度，不等待）
   ▼ 路由，选出 rssid
   ├ 通道没配 CoreOutboundTps → 和现在一样，直接写 stream:mt:routed（行为不变）
   └ 配了：
       ├ 积压条数 ≥ 上限 → 不写入；写 CDR，给客户回失败 DR（通道拥塞）
       └ 否则写入 stream:core:hold:{rssid}，然后确认消费 pending
                 │
                 ▼ ChannelReleaseDispatcher（每个 Core 节点一个）
                 │ 轮询非空的 hold 队列（门铃 zset:core:hold:active）
                 │ 每个 rssid：本地 RateLimiter.tryAcquire 拿到几个令牌，就读出几条
                 │   ├ 已过期，或在 hold 里超过 CoreHoldMaxSeconds → 按过期回结果，写 CDR，给客户回失败 DR
                 │   └ 否则执行一段 Lua，原子地搬过去：XADD routed + 登记 outbox + 门铃 + XACK/XDEL hold
                 ▼
stream:mt:routed:{rssid}:{shard} → Client（几乎没有积压）
```

暂存队列必须按通道单独建，不能直接在 `stream:mt:pending` 上等令牌。pending 是按客户分的，同一个 stream 里混着发往不同通道的消息；在慢通道的消息上停下来等令牌，会把后面发往快通道的消息一起堵住。

## 3. 配置（按 rssid，写在 cm.cfg 的通道块里）

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `CoreOutboundTps` | 0（不限速） | **单个 Core 节点**对这个通道的放行速率。实际总速率 = 这个值 × 放行该通道的 Core 节点数，配置时按"供应商能力 ÷ Core 节点数"填写 |
| `CoreHoldMaxSeconds` | 21600（6 小时） | 消息在暂存队列里最多等多久；同时检查消息自身的过期时间，先到者为准。也用来计算积压上限 |

- 积压上限 = `CoreOutboundTps × CoreHoldMaxSeconds`（按 rssid）。例如 1 × 21600 = 21600 条。
- 超过上限：直接回失败 DR，不写入暂存队列。
- cm.cfg 热加载后生效：放行线程每一轮都从客户配置快照里读取最新值；本地 `RateLimiter.setRate` 随之调整。改成 0 以后，新消息直接走 routed，暂存队列里剩下的消息不再限速，尽快放完。

## 4. 实现要点

| 项 | 设计 |
|---|---|
| 令牌 | 每个 Core 节点本地，每个 rssid 一个 Guava `RateLimiter`，使用不阻塞的 `tryAcquire`；不使用 Redis，不追求跨节点精确 |
| 暂存队列 | `stream:core:hold:{rssid}`，放在 submitMq 所在的 Redis 实例上；消费组 `CoreHoldGroup`，多个 Core 节点一起放行；**不设 MAXLEN 裁剪**，靠积压上限控制长度 |
| 积压判断 | 每个 Core 本地缓存各个 hold 队列的长度（每秒刷新一次 XLEN），不用每条消息都查 Redis |
| 原子搬运 | 一段 Lua 完成"写 routed、登记 outbox、写门铃、确认并删除 hold 条目"，所以搬运不会丢也不会重复；已读出但还没搬完的消息，崩溃后由自动重投领回 |
| outbox | 从放行那一刻开始计时；routed 队列很短，"没读取超过 2 分钟 = Client 故障"这个判断成立 |
| 失败 DR | 超过上限和暂存超时这两种情况，都走 Core 现有的失败结果处理流程（写 CDR、按客户配置回 DR）；需要新增一个状态码，或者复用现有的拥塞类状态码，实现时再确定 |
| 指标 | 每个 rssid 的积压条数、放行速率、超过上限被拒条数、暂存超时条数 |
| Client | 不用改；原来的出向限流 `OutgoingThrottlingNum` 保留作为保险，配置成大于等于 Core 放行的总速率 |

## 5. 容量估算

| 通道速率 | 6 小时积压上限 | Redis 占用（每条 1 到 2KB） |
|---|---|---|
| 1 TPS | 21600 条 | 20 到 40 MB |
| 50 TPS | 108 万条 | 1 到 2 GB |
| 500 TPS | 1080 万条 | 10 到 20 GB |

高速通道建议单独把 `CoreHoldMaxSeconds` 配小一些，或者给 submitMq 所在的 Redis 预留足够内存。

## 6. 改动范围

| 文件 | 改动 |
|---|---|
| `A2PCustomerInfo` | 新增 `coreOutboundTps`、`coreHoldMaxSeconds` 两个字段，在 `assignValue` 里解析 |
| `StreamQueueManager` | 新增 hold 写入、搬运 Lua、门铃 `zset:core:hold:active` |
| `MessageProcessorThread` 外发循环 | 配了限速的通道改写 hold，写入前检查上限；HTTP 通道同样处理 |
| 新增 `ChannelReleaseDispatcher` | 放行线程：本地限流、读取、过期检查、搬运、指标 |
| `DeliveryRouter` | 启动和停止放行线程 |
| `CoreOutboundOutboxManager` | outbox 在放行时登记（与第 3 批的外发 Lua 合并） |

## 7. 验证场景（本地集成环境）

1. 配 `CoreOutboundTps: 2` 的通道，每秒写入 50 条：放行速率不超过 2；另一个不限速的通道不受影响。
2. 积压达到上限后，新消息立即得到失败 DR，hold 长度不再增加。
3. 在 hold 里超过 `CoreHoldMaxSeconds`（测试时配成几秒）的消息不会发出，客户收到失败 DR。
4. 搬运到一半时进程退出：恢复后不丢失、不重复。
5. 热加载把 TPS 从 2 改成 10：放行速率随之变化；改成 0：新消息直接走 routed。
