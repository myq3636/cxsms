# Server 第 3 批优化设计：DR 结果写入与确认消费合并、enquire_link 按 EnquireLinkTime 发送

## 1. DR 的结果写入与确认消费合并成一段 Lua

### 问题

在 Server 第 2 批"收到响应后才确认消费"之后，默认的内存 pending 模式下，每条 DR 收到客户的 deliver_sm_resp 时，要访问两次 Redis：

1. 把结果写到 `stream:core:results:*`（XADD + 门铃 ZADD，一段 Lua）；
2. 确认消费这条 DR 自己的 stream 记录（XACK + XDEL，一段 Lua）。

两个 stream 都在 reportMq 上，可以放进同一段 Lua。

### 方案

新增 `StreamQueueManager.produceResultAndAck`，一段 Lua 依次执行：写结果（XADD + ZADD 门铃）→ XACK 这条 DR → 确认成功则 XDEL。

- **一次往返，而且原子**：结果没写成，DR 就不会被确认消费，这条 DR 留在待确认列表里重试；不会出现确认了、结果却没写的情况。
- **只合并同一条 DR 的两步，不做跨消息攒批**：每条 DR 收到响应就立即处理。客户之间没有新增任何共享的东西，DR 少的时候也不需要等待。
- **所有写结果的路径都走合并**：
  - 收到 deliver_sm_resp；
  - pending 超时；
  - 写结果失败后，pending 管理器的重试；
  - 会话关闭时写出结果。

  pending 管理器的重试和关闭原来直接调用 `produceResult`，现在通过新的接口方法 `PendingDrTimeoutHandler.produceDrResult` 交给会话处理，由会话一起完成确认消费。这也修复了第 2 批遗留的一个缺口：写结果失败后重试成功时，DR 原来不会被确认消费，过一段时间就会被自动重投重发一次。
- **客户响应比 DR worker 记录序列号还快时**（极少数）：先单独写结果，DR worker 记录时再单独确认消费，逻辑与第 2 批相同。实测 200 条中有 1 条走这条路径。
- **其他情况保持两步**：
  - redis/hybrid pending 模式（删除 pending 记录的那一步在 state Redis 上，不在同一个实例）；
  - `ServerDrAckOnResp=false`；
  - 写结果被 stream 背压拦住时。
- **日志不变**：仍然是 `[STREAM-PUT]`（结果）和 `[STREAM-ACK]`（DR）两行。`[STREAM-ACK]` 后面加上 `with_result=<结果的 id>`，表示两者由同一段 Lua 写入。

## 2. enquire_link 按 EnquireLinkTime 发送

### 问题

开启 `ServerEnquireLink` 时，刚发出一个 enquire_link，计数就已经是 1，保活检查把它当成"有一个没回"，下一次检查要等 `FailEnquireLinkTime`（默认 10 秒）。所以实际约每 10 秒才发一次，而不是按 `EnquireLinkTime`。

### 方案（`ServerSessionKeeper`）

- **只看之前发出的 enquire_link**：只有之前发出、到这次检查时仍未回的 enquire_link 才算"未回"，这时按 `FailEnquireLinkTime` 的间隔检查。刚发出的那一个不算。
- **刚发出时**：`EnquireLinkTime` 后再检查。
- **这次没发**（有业务流量，或者刚收到响应）：在"链路空闲满 `EnquireLinkTime`"的那一刻再检查，而不是从这次检查起再等一整个 `EnquireLinkTime`。否则间隔会被拉长到将近两倍。
- **断开条件不变**：累计 `EnquireLinkFailureNum` 个没回就关闭连接；超过 `ConnectionSilentTime` 没有任何活动也关闭。

`SmppServerSessionKeeperShared=false` 时使用旧的每连接线程，仍是原来的行为。

## 3. 测试中发现并修复：DR pending 管理器重复创建（原有问题）

**现象**：本地全量回归中，某条 DR 的 deliver_sm_resp 回来时，日志是 `Can't find the original message from bufferMonitor, sequence=1`，这条 DR 没有结果，也没有确认消费。

**原因**：会话的 DR pending 管理器是在第一次发 DR 时才创建的（`if (pendingDrManager == null) pendingDrManager = new ...`），没有加锁。同一个客户有两个分片 stream，由两个 DR worker 同时处理。如果两个 worker 同时给一个新会话发出它的第一批 DR，就会各自创建一个管理器，后创建的那个覆盖前一个。前一个管理器里登记的 DR 再也查不到。

**后果**（在线上同样会发生）：
- 客户已经收到并回了 resp，但 Server 匹配不上这个 resp。
- 约 `BufferTimeout`（60 秒）后，被覆盖的那个管理器按超时给 Core 回 FAIL_SENDOUT_DELIVERYREPORT。
- Core 按 DR 重试策略重发，客户收到重复的 DR；CDR 里多出一条失败记录。
- 被覆盖的管理器占用的窗口名额也不会再释放给新管理器。

触发条件是：同一个客户在新连接 bind 后，第一批 DR 同时来自多个分片。

**修复**：DR 和 submit 的 pending 管理器都改为加锁、只创建一次（字段改为 volatile，双重检查）。Client 侧创建 submit 的 pending 管理器也是同样的写法，一并修复。

## 4. 配置项

本批没有新增配置项。两项改动分别受已有的开关控制：
- `ServerDrAckOnResp`：false 时，DR 在发出后就确认消费，收到响应时只写结果，不走合并。
- `SmppServerSessionKeeperShared`：false 时使用旧的每连接保活线程，enquire_link 仍是原来的间隔。
