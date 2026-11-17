# Redis 集群方案设计（评估，不改代码）

> 日期：2026-11-17。问题：现在的 Redis 主从模式性能不够（5000 TPS 时 8387、8388 主线程 95% 以上），改成 Redis Cluster 会不会更好？本文对比现状、Redis Cluster 和"应用层分片到更多主从实例"三种做法，给出代码影响、收益估算、风险和建议。

## 1. 结论

1. **慢的原因不是"主从模式"本身，而是每个实例只有一个主线程**。现在一共只有 3 个主库，每个角色（state、submit、report）一个，单个主库的上限就是系统的上限。主从复制本身的开销不大：主库写复制流、从库占机器的 CPU，去掉从库估计也只能多出 5%～10%。
2. **Redis Cluster 能提高吞吐，前提是主库数量变多、而且 key 能均匀分散**。主库数相同时，集群并不会更快。它的好处是：按 hash slot 自动分片、自带故障切换（不用现在应用层的 `RedisMonitor` 切换），扩容时可以迁移 slot。
3. **代价是代码改动大**：现在的 Lua 脚本大多一次操作多个 key（stream + 门铃 zset + 标记 + 索引……），集群要求这些 key 在同一个 slot。门铃、`idx:streams`、outbox 超时 zset 这类全局 key 会成为单个 slot 的热点，必须按分片拆开。客户端要从 `JedisPool` 换成 `JedisCluster`，pipeline、SCAN、自动认领都要按节点或 slot 重写。
4. **建议**：先做不需要更多实例的优化（打开共享收包池、`io-threads`、减少每条消息的 Lua 次数），看单实例能到多少。确实需要 5000 TPS 以上时再上集群，并且要配合更多机器：16 核的机器上还要跑 a2p 和 MySQL，能给 Redis 主库的核不多，从库也应该放到别的机器。

## 2. 现状

| 项 | 现在 |
|---|---|
| 实例 | 3 个独立主库：8386 state（会话登记、DR 查找、长短信、假 DR）、8387 submit（MT 的 stream、outbox）、8388 report（结果、DR 的 stream、MQM）；各有一个从库（8376～8378），都在同一台机器上 |
| 客户端 | Jedis 5.2 `JedisPool`，每个模块对每个实例一个池（`Redis_pool_maxActive` 50） |
| 读写 | 只读写主库；从库用于主库故障时切换，由应用里的 `RedisMonitor` 判断，没有 Sentinel |
| 分片 | 应用层已经有 16 个逻辑分片（`TotalShards`），体现在 stream 名里（`stream:mt:pending:{ossid}:{分片}` 等），各 Core 按 `CoreShard.*` 负责一部分分片；但**所有分片都在同一个 Redis 实例上** |
| 实测上限 | 8388 每条 MT 约 636 µs CPU，单实例约 1400～2000 条 MT/秒（《Redis CPU瓶颈分析》第 8 节）；5000 TPS 时 8387、8388 都在 95% 以上 |

## 3. Redis Cluster 怎么工作，对我们意味着什么

- 16384 个 hash slot 分到 N 个主库，key 按 CRC16 落到某个 slot；`{...}` 里的部分（hash tag）决定 slot，同一个 tag 的 key 在同一个 slot。
- **多 key 的命令、Lua 脚本、MULTI 都要求所有 key 在同一个 slot**，否则报 `CROSSSLOT`。
- 每个主库仍然是单线程，**集群吞吐 ≈ 主库数 × 单库吞吐，前提是负载在 slot 之间均匀**。一个热点 key 只能在一个主库上，它所在的主库就是上限。
- 自带故障切换（从库自动提升），客户端通过 `MOVED`/`ASK` 重定向找到新主库。
- SCAN 只扫一个节点，要对每个主库分别扫；pub/sub 普通频道在集群里广播，可以继续用。

## 4. 现在的代码要改什么

### 4.1 多 key 的 Lua 脚本

| 脚本 | 涉及的 key | 集群下怎么办 |
|---|---|---|
| 写 stream + 门铃（`xaddWithDoorbell`，`produce`） | stream、该类型的门铃 zset（全局一个，例如 `zset:mt:pending:active`）、`idx:streams`（全局） | 门铃和索引按分片拆：`zset:{mt:pending:5}:active`、`idx:{mt:pending:5}`，stream 用同一个 tag |
| 只写一次（`PRODUCE_ONCE_LUA`） | stream、门铃、消息标记、outbox 消息体 / meta、outbox 超时 zset（全局） | 标记和 outbox key 带上 stream 的 tag；超时 zset 按分片拆，扫描时逐个分片扫 |
| 结果写回 + 确认（`RESULT_AND_ACK_LUA`） | `core:results` 的 stream 和门铃、**DR 来源 stream**（`dr:pending`） | 两个 stream 的分片算法不同，不在同一个 slot：要么拆成两次调用（失去原子性，要重新设计防重），要么让结果 stream 的分片跟随 DR stream |
| 暂存放行（`RELEASE_LUA`） | hold stream、hold 门铃、目标 stream、目标门铃、outbox | hold 和目标 stream 用同一个分片 tag |
| 死信（`DEAD_LETTER_LUA`） | 来源 stream、死信 stream | 死信 stream 按来源的 tag 拆开 |
| StreamAttempt 的 CLAIM / FLUSH / ACK、Client 发送标记 | 每个条目的标记 + stream | 标记 key 带 stream 的 tag |
| 取门铃（`POP_DOORBELL_LUA`） | 门铃 zset + 多个候选 stream | 门铃已按分片拆，候选 stream 和它同一个 tag |
| outbox 认领（`CoreOutboundOutboxManager.CLAIM_LUA`） | outbox、超时 zset、meta、`:done` | 同上，带分片 tag |
| 长短信（`RedisCsmReassembler`）、假 DR 定时（`FakeDrTimeoutScheduler`）、模块登记、配置发布 | state 上的多个 key，部分是全局 zset | 全局 zset 按 Core 分片拆；配置、登记类低频，可以放一个 tag |

### 4.2 其他

| 项 | 改动 |
|---|---|
| 客户端 | `RedisConnection` 现在基于 `JedisPool` 和 `Jedis`；换成 `JedisCluster`（或每个节点一个池 + 自己算 slot）。`runPipeline` 在集群里只能按节点或 slot 批量（Jedis 5 有 `ClusterPipeline`） |
| 门铃分发 | 消费者原来从一个门铃 zset 取，改成从自己负责的分片的门铃 zset 取。这和现有的分片归属（`MyShards`）一致，反而更直接 |
| 列 stream、自动认领、积压监控 | 按分片的索引集合读；全量 SCAN 对每个主库分别做 |
| 3 个角色 | 可以合并成一个集群（靠 tag 分开），也可以保留 3 个集群；合并更省事，但 state 的低频全局 key 要单独考虑 |
| 运维 | 集群创建、扩缩容（slot 迁移）、监控；`CONFIG`、`FLUSHDB` 的使用方式变化；现在应用层的主从切换（`RedisMonitor`）不再需要 |
| 测试 | 回归环境要有一个集群（至少 3 主）；所有 Lua 相关的套件都要在集群上跑；故障切换要单独测（主库挂掉时脚本、pending、门铃的行为） |

### 4.3 热点

即使拆了 tag，有些负载天然集中：

- **一个大客户、一个大通道**：stream 按 `{ssid}:{分片}` 分，一个客户的 MT 能分到 16 个分片，可以分散；
- **按类型的全局计数和限流**：例如 Server 收 MT 时的计数（`incrWithExpire`），每个客户一个 key，大客户就是热点，可以接受，或按分片计数再汇总；
- **16 个分片 → N 个主库**：分片数要远大于主库数，负载才均匀；可能要把 `TotalShards` 调大（例如 64）。

## 5. 另一种做法：应用层分片到更多主从实例

不用 Redis Cluster，保留独立的主从实例，但每个角色从 1 个主库变成 M 个，按应用的分片号把 key 路由到第 `分片 % M` 个实例：

| | Redis Cluster | 应用层分片 |
|---|---|---|
| key 的同址要求 | 同一个 slot（hash tag） | 同一个实例（同一个分片号），设计和 4.1 基本相同 |
| 客户端 | 换 `JedisCluster`，所有调用都要适配 | 保留 `JedisPool`，加一层"分片 → 实例"的路由 |
| 故障切换 | 自动 | 仍是应用层切换，每个实例一对主从 |
| 扩容 | 迁移 slot，在线 | 改分片映射，要停机或做数据迁移 |
| 改动量 | 大 | 中（key 设计相同，客户端改得少） |

两种做法的核心工作是一样的：**把一次操作涉及的 key 放在同一个分片，并拆掉全局 key**。区别主要在客户端和运维。

## 6. 收益估算

- 机器：16 核。负载时 a2p 用掉约 9～10 个核（Core 3×1.1、Server 1.2、Client 1.4、MQM、MySQL），**同一台机器上能给 Redis 主库的大约 4～6 个核**，从库应该放到别的机器。
- 如果 submit、report 各 3 个主库、key 均匀：这两类的上限约 ×3，系统的上限会转到 Core、Server 的 Java 处理和 MySQL；按现在的每条 MT 开销粗估，单机能到 3500～4500 TPS。要稳定 5000 TPS 以上，需要 Redis 放到单独的机器（或多台），同时继续做《Redis CPU瓶颈分析》第 5 节的 5、6（减少每条消息的 Lua 调用）。
- 这些是估算，需要先做原型验证：把一个流程（例如 MT 的 `mt:pending` → `mt:routed`）按 tag 改造，在 3 主的集群上测吞吐。

## 7. 建议的顺序

1. **配置**：打开共享收包池（`SmppServerInboundSharedPoolEnable=true`，《Server发DR慢的原因分析》），试 Redis `io-threads`，调大 AOF 重写阈值，Redis 数据目录移到 `/mnt`；测试工具按窗口发 submit。
2. **减少每条消息的 Redis 开销**：按批处理 StreamAttempt、合并 FLUSH（《Redis CPU瓶颈分析》第 5 节第 5、6 项）。
3. 1 和 2 之后用同样的方法重测，看单实例能到多少 TPS。
4. 还不够、而且要 5000 TPS 以上时，**按本文第 4 节做 key 的同址设计**（两种扩展方式都需要），先做一个流程的原型，在 3 主集群和"3 个主从实例 + 应用层分片"上对比，再决定用哪一种。

| 步骤 | 改动 | 估计工作量 | 风险 |
|---|---|---|---|
| 1 | 配置、测试工具 | 1～2 天（含验证） | 低 |
| 2 | StreamAttempt 批量化 | 1～2 周 | 中：可靠性机制的核心 |
| 4 | key 同址 + 集群或应用层分片 | 4～8 周（含回归、故障切换测试） | 高：所有 Redis 访问都要改 |
