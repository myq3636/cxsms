# Server 第 2 批优化设计：日志、共享保活调度、有界发送队列、DR 收到响应后确认消费

问题清单见《SMPP-Server架构分析与改进》。按决定，D5（客户断线时 DR 的宽限期）不做。

## 1. 日志：每个关键点一条 INFO

每条 INFO 都带上 msgId（日志前缀）、ssid、会话名、序列号；与 stream 相关的再带上 redisId。用 msgId 一条 grep 就能串起一条消息在 Server 上的完整轨迹。

| 关键点 | INFO 日志 | 原来的日志 |
|---|---|---|
| 收到 submit_sm（PDU） | `[RECV] ... cmd=submit_sm`（pduLogger） | 不变 |
| 解码完成 | `[SUBMIT-IN] ssid session seq inMsgId sender recipient`（deliver_sm 为 `[DELIVER-IN]`） | `Smpp receive message :{整个消息}` → 改为 DEBUG |
| 写入 Core 的 stream | `[STREAM-PUT] stream redisId`（stream 日志） | 不变 |
| 已入 Core、回 resp | `[SUBMIT-TO-CORE] ssid session seq inMsgId redisId -> submit_sm_resp ESME_ROK` | `SUBMIT_SM accepted asynchronously` → 改写 |
| 从 Core 读到 DR | `[STREAM-GET] ... msg={简短形式}` | 原来打印整个消息 → 改为简短形式，整个消息改为 DEBUG（Core、Client 的 STREAM-GET 也一样） |
| DR 发给客户 | `[SEND] cmd=deliver_sm`（pduLogger）+ `[DR-OUT] ssid session seq status redisId` | `send dr, inTransID` → 改写；`resolved session` → 改为 DEBUG |
| 收到 deliver_sm_resp | `[RECV] cmd=deliver_sm_resp`（pduLogger）+ `[DR-RESP] ssid seq pduStatus status` | `receives DELIVER_SM_RESP, sequence`（没有 msgId）→ 改为 DEBUG；`DeliverSM receivies response` → 改写 |
| DR 结果写回 Core | `[STREAM-PUT] stream:core:results...` + `[DR-RESULT-TO-CORE] ssid session seq status redisId` | `Produced DR_RESP to stream:core:results` → 改写 |
| DR 的 stream 确认消费 | `[STREAM-ACK]` | 位置从"发出后"移到"收到响应后"（见第 4 节） |
| 拒绝、失败、超时 | 原有的 WARN/INFO | 不变 |

pduLogger 新增开关 `SmppPduLogEnable`，**默认打开**，支持热加载。

## 2. 共享保活调度替代每连接的 ServerSessionThread

新增 `ServerSessionKeeper`，用一个共享的定时调度器（`SmppServerSessionKeeperThreads` 个线程，默认 4）替代每个连接一个的 `ServerSessionThread`。检查逻辑完全相同：

- **bind 之前**：每秒检查一次。断开就停止；超过 `SmppServerBindTimeoutMs` 没有 bind 就关闭；超过 `MaxSilentTime` 没有活动就关闭。
- **bind 之后，开启了 `ServerEnquireLink`**：
  - 每 `EnquireLinkTime` 检查一次，按需发送 enquire_link。
  - 有未回的 enquire_link 时，按 `FailEnquireLinkTime` 的间隔检查（刚发出 enquire_link 时也算一次未回，所以默认是每 10 秒左右发一次。这是原来的行为，没有改）。
  - 达到 `EnquireLinkFailureNum` 次没有回，或超过 `ConnectionSilentTime` 没有活动，就关闭。
- **bind 之后，没有开启 `ServerEnquireLink`**：只做静默检查。
- **多节点客户的 RECOVER 状态**：节点恢复时间到了以后改回 CONNECT。原来用的是每个会话一个 Timer 线程，现在也放到共享调度器上。

开关 `SmppServerSessionKeeperShared`（默认 true，新连接生效）；false 时恢复每连接一个线程。只用于 SMPP 服务端会话，PeeringTcp 不变。

效果：每个连接从 3 个线程（读、写、保活）减少到 2 个。

## 3. 有界发送队列和反压

- **发送队列上限**：客户连接的发送队列最多 `SmppServerSendQueueMax`（默认 20000）个 PDU。超过上限，说明客户已经不读 socket 了，直接关闭连接（客户会重连），不再无限占用堆内存。
- **DR 反压**：发送队列中等待的 PDU 达到 `SmppServerSendQueueHighWater`（默认 2000）时，`hasServerDrWindow()` 返回"窗口满"。DR 按第 1 批的延后机制暂缓发送，客户读走之后再继续。
- **写线程不再空闲轮询**：写线程空闲时每 200 毫秒醒一次（2 万个连接就是每秒 10 万次）。`send()` 和 `close()` 本来就会唤醒它，所以等待时间改为 5 秒，只作为保底。
- **共享收包池的分发线程**：空闲时每 1 毫秒轮询一次，改为阻塞等待有客户被激活（最长 100 毫秒）。

### 测试中发现并修复的问题：关闭连接时可能永久卡住

客户不读 socket 时，写线程会阻塞在 `write()` 里，同时持有写锁。原来的 `close()` 要先拿到这把锁才关闭 socket，结果永远拿不到。这样一来，调用 `stop()` 的线程（worker、保活检查）会被永久卡住，这个问题原来就存在。

现在 `close()` 先关闭 socket，让阻塞的 `write()` 抛出异常、释放写锁，再去拿锁清理输入输出流。

## 4. DR 收到 deliver_sm_resp 后才确认消费

- **原来**：deliver_sm 放进发送队列就确认消费。内存模式的 pending（`SMPPDRPendingMode=memory`，默认）下，Server 崩溃时，已经发出但客户还没回 resp 的 DR 就丢了。
- **现在**：DR 发出后，会话按序列号记下它对应的 stream 记录。下面任一情况写好结果后，才确认消费：
  - 收到 deliver_sm_resp；
  - pending 超时（`BufferTimeout`）；
  - 会话关闭。

  崩溃时这些记录还在待确认列表里，由自动重投在空闲 60 秒后重新发送（至少一次，不会丢）。
- **两边的先后顺序不固定**：客户的 resp 可能比 DR worker 记下序列号还快。两边无论谁先到，后到的那一方负责确认消费。
- **Redis 访问次数不变**：确认消费只是从 DR worker 挪到了处理 resp 的地方。
- **范围**：只对内存模式生效。redis/hybrid 模式本身已经持久化了 pending，仍然在发出后确认消费。
- **开关**：`ServerDrAckOnResp`（默认 true，支持热加载）；false 时恢复发出后确认消费。

## 5. 配置项

| 配置项 | 默认值 | 生效方式 |
|---|---|---|
| `SmppPduLogEnable` | true | 热加载 |
| `SmppServerSessionKeeperShared` | true | 热加载（新连接生效） |
| `SmppServerSessionKeeperThreads` | 4 | 重启 |
| `SmppServerSendQueueMax` | 20000 | 热加载（新连接生效） |
| `SmppServerSendQueueHighWater` | 2000 | 热加载 |
| `ServerDrAckOnResp` | true | 热加载 |

## 6. 不在本批范围

- D5（客户断线时 DR 的宽限期）：按决定不做。
- 去掉每个连接的写线程：需要非阻塞 IO，留给阶段 C（Netty）。
- 计费：按决定不改。
