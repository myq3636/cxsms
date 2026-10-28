# Core 第 5 批优化设计：出向结果 stream 分片

## 1. 现状与问题

所有出向结果都由 `StreamQueueManager.produceResult()` 写入**同一个** `stream:core:results`（在 report MQ 所在的 Redis 上）。写入方包括：

- Client：SMPP/HTTP 外发的 submit_resp、超时结果
- Server：客户回的 deliver_sm_resp、DR 无法下发的失败结果
- Core：outbox 超时结果

`ResultStreamConsumer` 用 `popDoorbell` 弹出这个 stream 唯一的门铃，由某个 Core 的一个工作线程处理，所以**整个集群大多只有 1 个线程在处理所有结果**：

- 吞吐低：每条约 3 到 5ms，集群每秒只能处理 200 到 300 条。
- 结果积压导致 DR 延迟：DR 反查缓存写得晚，供应商 DR 先到时查不到原消息，只能存进 SDQ 以后再匹配。
- 不公平：某个供应商的结果暴增，所有结果都要跟着排队。

## 2. 方案（已确认）

### 2.1 stream 命名

```
stream:core:results:{ssid}:{shard}
  ssid  = DR 类结果（DELIVERY_REPORT_RESP / DELIVERY_REPORT / DELIVERY_REPORT_QUERY_RESP）取 ossid，
          其他（SUBMIT_RESP 等）取 rssid；为空时取另一个
  shard = (msgId（为空时用 inMsgID）的 hashCode & Integer.MAX_VALUE) % TotalShards
```

- 按 ssid 拆分：各供应商、各客户的结果互不影响，积压在哪个 ssid 上一目了然。
- 按 msgId 定分片：拆分短信的各段 msgId 相同，落在同一个 stream；大供应商的结果分散到 `TotalShards` 个 stream 上并行处理。
- 只改 `produceResult` 一个方法，所有写入方自动生效。

### 2.2 消费

- **所有 Core 都可以处理任意结果 stream，不按分片指定归属**。理由：
  - 处理结果不依赖本机状态（outbox、DR 反查缓存、重试队列都在 Redis 或数据库里）。
  - 某个 Core 挂掉，也不会有结果分片一直没人处理。
  - 同一次外发的两个结果（真实结果和超时结果）即使被不同 Core 同时处理，第 3 批的 outbox 判定也只认第一个。
- 仍用 `popDoorbell` 弹出门铃。stream 一多，多个 Core、多个线程就能并行处理；第 4 批的"任务限定处理量、处理完让出 stream"同样生效。

### 2.3 兼容

- Core 同时消费新的分片 stream 和旧的 `stream:core:results`。门铃、自动重投、积压监控、日志和指标的匹配条件，都由"等于 `stream:core:results`"改为"以它开头"。
- 开关 `CoreResultStreamSharded`（GmmsConfig，默认 `true`，支持热加载）：设为 false 时写回旧的单个 stream，用于回退。
- 发布顺序：全新部署时没有要求。如果逐个模块滚动升级，必须先升级 Core。如果做不到，就先在 Server 和 Client 上把开关设为 false，等 Core 升级完成后再打开。

### 2.4 连带调整

| 位置 | 改动 |
|---|---|
| `getDoorbellZSet` | 新旧结果 stream 用同一个门铃 zset |
| `ResultStreamConsumer.start()` | 启动时用 SCAN 找出所有 `stream:core:results*` 中还有消息的 stream，逐个重新挂门铃 |
| 自动重投 | 匹配 `stream:core:results*` |
| 积压监控、日志和指标的流向与消息类型 | 判断改为"以 `stream:core:results` 开头" |

### 2.5 不改的部分

结果的处理逻辑本身（outbox 判定、CDR、DR 反查缓存、重试策略），以及 outbox 的 key 和超时规则。
