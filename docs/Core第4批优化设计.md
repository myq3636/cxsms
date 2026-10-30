# Core 第 4 批优化设计

> 范围：
> - A 重投次数上限与死信队列
> - B 用 SCAN 代替 KEYS
> - C 消费线程公平性与分发线程不被卡住
> - D 分片配置校验
> - E 停机时把 CDR 写完
>
> 已确认的决定：**任何 stream 的消息进入死信队列后，只停放并告警，不回失败 DR，也不产生额外的结果。**

## A. 重投次数上限与死信队列

**现状**：`RedisStreamAutoClaimRunner` 每 `RedisStreamAutoClaimIntervalMs`（默认 5 分钟）用 XAUTOCLAIM 领回空闲超过 `RedisStreamAutoClaimIdleMs`（默认 60 秒）的待确认消息，重新处理，没有次数上限。一条处理时一定会失败的消息会被永远重投，待确认列表只增不减，也没有告警。

**设计**：

1. 领取改为两步：
   - 先用 `XPENDING key group IDLE <idleMs> - + <count>` 取出空闲的待确认消息，以及它们各自被投递过几次。
   - 投递次数**达到** `RedisStreamMaxDeliveries` 的，进死信队列；其余用 `XCLAIM` 按 id 领回，和现在一样交给业务处理（XCLAIM 会让投递次数加 1）。
2. 进死信队列用一段 Lua，在原 stream 所在的 Redis 实例上执行：
   - 读出原消息，写入 `stream:dlq:submit`（submit MQ 上的 stream）或 `stream:dlq:report`（report MQ 上的 stream）。
   - 字段：`reason=max_deliveries`、`sourceStream`、`sourceRedisId`、`sourceGroup`、`deliveries`、`createdAt`，以及原消息的 `body` 等字段。
   - 在原 stream 上执行 XACK 和 XDEL。
3. 打 ERROR 日志 `[STREAM-DLQ]`，带上 msgId、来源 stream 和投递次数，并计入指标，供告警使用。
4. 不回 DR，不产生结果。
   - `stream:mt:routed` 的消息进死信队列后，Core 的 outbox 仍按原规则处理：消息已不在 stream 里，会被当成"已发出、没有结果"，在 `CoreOutboundRespTimeoutMs` 之后回 2700，再由重试码决定后续。
5. 影响范围：`RedisStreamAutoClaimRunner` 是 Core、Server、Client 三个模块共用的，所以三个模块的自动重投都会生效。
   - 默认关闭的 PEL 监控（`RedisStreamGlobalPELMonitorEnable`）也会增加投递次数，打开它会让消息更早达到上限。

**配置**：`RedisStreamMaxDeliveries`，默认 10，0 表示不限次数（即原来的行为），支持热加载。按默认的重投间隔，一条消息大约 50 分钟后进入死信队列。

## B. 用 SCAN 代替 KEYS

**现状**：`StreamQueueManager` 有 4 处用 `KEYS pattern` 列出 stream，执行期间阻塞整个 Redis：
- `listResponsibleStreams`：自动重投时调用
- `reRingExistingStreams`：启动时调用
- `scanBacklogPattern`：积压监控
- `scanAndClaimStreams`：PEL 监控

**设计**：在 `RedisConnection` 里新增 `scanAll(pattern)`，用游标反复执行 `SCAN cursor MATCH pattern COUNT 1000`，结果放进 Set 去重；4 处都改成调用它，行为不变。

## C. 消费线程公平性与分发线程不被卡住（Core 的 4 个消费者：MT、入向 DR、结果、MQM）

**现状**：
1. `FetchAndProcessTask` 一直读到 stream 为空才退出，积压大的 stream 会长时间占住一个线程。
2. 线程池满了以后用 CallerRunsPolicy，任务改由分发线程自己执行，执行期间所有 stream 都停止分发。

**设计**：
1. **每个任务限定处理量**：处理满 `CoreStreamTaskMaxBatches` 批，或者用时超过 `CoreStreamTaskMaxMs`，先处理完当前这一批，然后退出。退出时，任务原有的收尾逻辑看到 stream 里还有消息，会重新挂门铃，这个 stream 就排到队尾。
2. **线程池拒绝时，分发线程不自己执行**：放弃这次分发，从"处理中"标记里移除这个 stream，重新挂门铃，分发线程休眠一个空闲间隔后继续。
3. Server DR 和 Client 外发的消费者写法相同，放到它们各自模块的批次里改。

**配置**：`CoreStreamTaskMaxBatches`（默认 10），`CoreStreamTaskMaxMs`（默认 2000），支持热加载。

## D. 分片配置校验

**现状**：`TotalShards`、`MyShards`、`CoreShard.*`、`ClientShard.*` 分散在各模块的配置里，没有校验。

**设计**（新增 `ShardAssignmentChecker`，由 `ModuleStatusReporter` 驱动，覆盖 server、core、client 三个角色）：
1. 启动时，以及之后每次心跳（每 10 秒），写入 `system:shards:{role}:{module}:{node}` = `TotalShards|负责的分片`，过期时间为 3 次心跳。
2. 每 60 秒做一次比对：
   - **所有登记节点的 TotalShards 不一致**：ERROR。
   - **core 节点之间、client 节点之间负责的分片有重叠**：WARN。
   - **core 或 client 节点合起来没有覆盖 0 到 TotalShards-1**：ERROR，并列出缺少的分片号。
   - server 默认消费本模块的所有 DR 分片，只参与 TotalShards 一致性比对。
3. 结论不变时，每 10 分钟最多重复打一次；结论变化时立即打日志。只打日志，不改变任何行为。

**后续修正（2026-10-30）**：
- **按消费的 stream 分组比对，不再按模块角色。** SMPP Client（`MultiSmppClient*`，消费 `stream:mt:routed:*`）和 HTTP Client（`CommonHttpClient*`，消费 `stream:http:submit:*`、`stream:http:dr:*`）的模块角色都是 `client`，但消费的是两套 stream。
  - 原来混在一起比对，两者配置相同分片时会误报重叠 WARN。
  - 更严重的是，缺分片的检查也是合起来算的：只要两类 Client 合起来覆盖了 0 到 TotalShards-1 就不报错。比如 SMPP Client 只配了 8 到 15、HTTP Client 配了 0 到 7，SMPP 的 0 到 7 号分片没有人消费，却不会报错。
  - 现在 HTTP Client 的分片登记为 `system:shards:httpclient:{模块}:{节点}`（模块心跳的角色仍是 `client`），重叠和缺失都在 core、client、httpclient 各组内单独比对。
- **新增汇总日志。** 第一次比对时，以及之后任何节点、分片配置发生变化时，打一条 INFO：`[SHARD-CHECK] shard owners: client: ...=0-7, ...=8-15; core: ...; httpclient: ...; server (all DR shards of their module): [...]; TotalShards=[16]`，按组列出每个节点负责的分片。

**配置**：无。

## E. 停机时把 CDR 写完

**现状**：CDR 先进 `CDRAsynWriter` 的内存队列（最多 1000 条），由后台线程写文件。收到停机事件后只是不再接收新记录（新记录改走同步写入），队列里已有的记录来不及写完就会丢失，而这些记录对应的消息已经确认消费了。

**设计**：收到停机事件（`TYPE_SHUTDOWN`，由 stop 命令触发）后，在当前线程把队列里剩下的记录全部写入文件并刷盘。写文件和后台线程用同一把锁，不会交错写。`kill -9` 属于强制结束，这个问题无法从代码层面解决。

**配置**：无。
