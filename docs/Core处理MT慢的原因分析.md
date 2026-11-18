# Core 处理 MT 慢的原因分析（8387）

> 日期：2026-11-18。代码是 `5ddefd8`（StreamAttempt 按批处理、共享收包池已开）。起因：5000 TPS 时 Server 能把客户的 MT 全部收进 `stream:mt:pending`，但 Core 处理不过来，`mt:pending` 每秒增加约 2800 条。本文只做分析，没有改代码。

## 1. 测量方法

测试服务器，测试前 Redis 只留 `thcon`、MySQL 清空，a2p 重启；5000 TPS × 60 秒（`cases/PF002_cmd`，不重启模块），在负载中段：

- 8386、8387、8388 的 `INFO cpu`、`INFO commandstats` 取 28 秒的前后差；
- 8387 `MONITOR` 2 秒（按 `ss -tnp` 把连接对应到模块；EVALSHA 只能看到 SHA，按脚本里执行的命令认出是哪个脚本）；
- 3 个 Core `jcmd Thread.print`。

结果目录：`/usr/local/a2p/log/hot8387_*`。

## 2. 数据

### 2.1 吞吐

| 段 | 每秒 |
|---|---|
| Server 写进 `stream:mt:pending` | **5939** |
| Core 写进 `stream:mt:routed` | **2220** |

### 2.2 Redis（28 秒窗口）

| 实例 | 主线程 | 命令执行 | 每秒命令 | 最大的一项 |
|---|---|---|---|---|
| 8387 submit | **93%** | 70% | 12.3 万 | EVALSHA 1.81 万/秒，平均 25.8 µs，**占 46.6% 个核** |
| 8388 report | 92% | 52% | 11.0 万 | EVALSHA 1.44 万/秒，23.3 µs，33.5% |
| 8386 state | 53% | 17% | 4.6 万 | EVALSHA 0.81 万/秒，12.6 µs，10.2% |

### 2.3 8387 上的 Lua（MONITOR 2 秒）

| 脚本（代码） | 调用方 | 每秒 | 脚本里的命令 |
|---|---|---|---|
| `xaddWithDoorbell`（写 `mt:pending`） | Server | 5939 | XADD、SADD、ZADD |
| `PRODUCE_ONCE_LUA`（写 `mt:routed` + outbox） | Core | 2220 | HGET、XADD、SADD、ZADD×2、SETEX×2、HSET×1.8 |
| `ClientSendMarker` CLAIM / ACK | Client | 约 1600 / 1600 | GET、SET / EXISTS、SET、XACK、XDEL、DEL |
| `CoreOutboundOutboxManager.CLAIM_LUA`（submit 结果认领 outbox） | Core | 约 1600 | EXISTS、DEL、ZREM、SETEX |
| StreamAttempt 按批 CLAIM / ACK | Core | 每批平均约 4 条 | — |

按脚本里的命令数粗算：Server 收 MT 约占 8387 Lua 开销的 1/3，Core 写 `mt:routed` 约 1/3，Client 的发送标记约 1/5，outbox 认领约 1/8。

### 2.4 Core 的线程

| | Core1 | Core2 | Core3 |
|---|---|---|---|
| 正在处理 MT 的线程（`MTStreamConsumer$FetchAndProcessTask`） | 9 | 9 | 11 |
| 其中停在等 Redis 的 Lua 返回（`evalCached`） | 7 | 8 | 10 |

29 个线程里：16 个在 `produceOnce`（写 `mt:routed`），4 个在按批 CLAIM，2 个在写标记，2 个在按批 ACK，其余在读 stream、未读检查、长短信合并。线程池配置是核心 50、最多 100（`RedisStreamConsumer.CoreMT.*`），**实际只用到约 10 个**。

Core 读 `mt:pending` 每次 `COUNT 100`，但读得最多的是 rtcs2、rtcs3（9996、9997）的小 stream，平均每次只读到约 4 条；rtcs1（9995，75% 的流量）的 stream 每次读满 100。

## 3. 原因

### 3.1 并发度被 stream 数限制

每个 stream 同一时刻只有一个任务在处理（`processingMap`），批内逐条串行。一个客户的 MT 按 `TotalShards`（16）分成 16 个 stream，每个 Core 负责约 5～6 个分片。所以大客户 rtcs1 在全系统里**最多 16 个线程同时处理**，线程池再大也用不上。

### 3.2 每条 MT 要依次做十几次 Redis 往返

处理一条 MT 时，读、认领、号码和路由的查找、写 `mt:routed` 和 outbox、CDR 相关、确认等，都是同步的 Redis 调用，分在 8386、8387、8388 上。Redis 在 90% 以上时每次往返都要排队。按 2220 条 / 秒 ÷（3 个 Core × 约 10 个忙线程）≈ 每个线程每秒 74 条，**每条约 13 ms**。

3.1 × 3.2：约 30 个线程 × 74 条 ≈ 2200 条 / 秒，和实测一致。

### 3.3 8387 自身已经接近上限

即使提高 Core 的并发，8387 主线程只剩约 7%。光是 Server 把 6000 段 / 秒的 MT 写进 `mt:pending`（每段一次 Lua：XADD、SADD、ZADD），就用掉 8387 约三分之一的 Lua 开销。

## 4. 可以做的（建议，没有改）

| # | 办法 | 改什么 | 作用 | 风险 |
|---|---|---|---|---|
| 1 | **门铃 ZADD 不重复写**：stream 已有门铃时不再改它的分数（`ZADD ... NX`，或先判断） | `xaddWithDoorbell`、`PRODUCE_ONCE_LUA` 等 | 绝大多数 ZADD 不再修改数据，不写 AOF、不复制给从库（SADD 已经是这样）；Server 收 MT 和 Core 写 routed 的开销都降 | 低：门铃的分数只决定先后，NX 保留最早的时间，取门铃仍按时间先后 |
| 2 | **提高 `TotalShards`**（例如 16 → 64） | 配置（要先清空积压，所有模块一致） | 一个客户的 MT 分到更多 stream，Core 能同时处理的线程多 4 倍 | 中：8387 余量很小，并发上去后 8387 先满；要配合 1、3 |
| 3 | **减少 Core 写 `mt:routed` 时的 key**：outbox 现在每条 MT 写 3 个 key（消息体副本、meta、超时 zset 成员），消息体是整条消息的副本 | 代码（outbox 设计） | `PRODUCE_ONCE` 从约 9 条命令降到 5～6 条，内存和 AOF 也少 | 中：outbox 的超时、重发逻辑要跟着改 |
| 4 | **Client 的发送标记合并**：CLAIM、ACK 各一次 Lua，可以像 StreamAttempt 一样按批 | 代码 | 每条 MT 少约 2 次 Lua | 中 |
| 5 | **Server 收 MT 按批写**：多个 submit 合成一次 pipeline 或一次 Lua 写多条 | 代码 | 8387 上最大的一块（6000 次 / 秒）降成几百次 | 中：要保证 submit_sm_resp 在写入成功之后才回 |
| 6 | 增加 Redis 主库（《Redis集群方案设计》） | 架构 | 8387 的负载分到多个实例 | 高 |

建议先做 1（改动小、风险低），再看 2 能把 Core 的处理速度提到多少；要到 5000 TPS 需要 3～5 一起做，最终还是需要 6。
