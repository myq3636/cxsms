# HTTP 外发结果写入失败导致重复发送：修复设计

> 日期：2026-11-06。状态：**已实现**（第 10 节是实现和测试结果）。第 2、3 节来自读当前 `main`（`7c08125`）的代码；第 2.3 节的“60 秒后重发”是按代码推断的，回归里只碰到过前半段（`ClientHttpIT` HT-13e）。

## 1. 结论

- **HTTP 外发有这个问题：** 供应商已经接受了 MT，但 Client 连续两次往 Redis 写结果都失败时，Client 会**删掉发送标记**，约 60 秒后自动重投把这条交给某个消费者，**再发一次 HTTP**。供应商收到两次（客户手机收到两条，费用两份），第一次的 outMsgId 丢失，它的 DR 匹配不上。只要 Redis 恰好在这一刻抖一下就会发生，和 cm.cfg 的重试码配置无关。
- **SMPP 外发没有同样的问题：** SMPP 在 submit_sm 写进 socket 后就把 stream 记录标记为“已发”并确认，结果在收到 submit_sm_resp 后才写；写失败时在进程内每隔几秒重试，最多 50 次，不会重发。只有 Redis 连续不可用超过 `CoreOutboundRespTimeoutMs`（180 秒）时，Core 报 2700，是否重发由供应商的 `SMSOptionSubmitRetryCode` 决定。这是已知、已写进配置说明的“2700 可能重复”，不是这个问题。
- **修复思路：** HTTP 改成和 SMPP 一样——**请求一旦发出，就不能再被重发**；结果写不进去时在进程内重试写结果，而不是让整条消息重来。另外修两处同源的小问题（2.4）。

`docs/Client模块功能与测试用例清单.md` 第 14 节当时的推断有两处不对，修复时一起改正（第 8 节）。

## 2. HTTP 外发现状

### 2.1 正常流程

`HttpClientStreamConsumer.processMessage`（`src/com/king/gmms/messagequeue/HttpClientStreamConsumer.java:330`）：

| 步 | 代码 | 动作 |
|---|---|---|
| 1 | `ClientSendMarker.claim` | 写发送标记 `client:sent:{msgId}:{段号}:{重试次数}` = `S\|owner\|时间`（发送中），抢到 `SEND` 才往下走 |
| 2 | `session.submit(msg)` | 每条消息新建一个 `CommonHttpSession`（或 `CommonRESTHttpSession`），里面依次：发 HTTP 请求；`dealResp` 解析响应，消息改为 `SUBMIT_RESP`、填 outMsgId；`putGmmsMessage2RouterQueue` 把结果写进 `stream:core:results:*` |
| 3 | 返回值 | `submit` 返回的是**结果写没写进去**（`CommonHttpSession.java:317` 的 `getLastRouterQueuePutResult()`），不是请求发没发出去 |
| 4 | `ClientSendMarker.ackSent` | 返回 true：一个 Lua 里把标记改为 `D`、`XACK`、`XDEL`、删标记 |

### 2.2 返回 false 之后

`failToCoreAndAck`（`HttpClientStreamConsumer.java:443`）：

1. 打 WARN `failed before HTTP response result`——这时供应商其实已经接受了，**日志是错的**。
2. 再写一次结果。消息已经是 `SUBMIT_RESP`，状态码不是负数就不改，所以这次写成功就是正确的成功结果，**没有问题**。
3. 第二次也失败（`resultProduced=false`）：
   - `ClientSendMarker.release(msg)`：**删掉发送标记**（第 485 行）。这个方法的注释是 “Not sent, entry stays pending for a retry: remove the marker so the retry can send”；
   - stream 记录不确认，留在这个消费者的待确认列表里。

### 2.3 之后会发生什么

```text
t=0     HTTP 请求成功，供应商回 code=0、outMsgId1
t≈0     写结果失败两次（Redis 刚切换或重启，连接池里是断开的连接）
        → 删标记，记录留在待确认
t≈60s   自动重投（RedisStreamAutoClaimIdleMs）把它交给某个消费者
        → claim：标记不存在 → SEND
        → 再发一次 HTTP，供应商回 outMsgId2，结果写成功
        → Core 记的是 outMsgId2
之后    供应商对 outMsgId1 的 DR：Core 找不到，匹配不上
```

Core 的 `CoreOutboundRespTimeoutMs` 是 180 秒，比 60 秒晚，所以通常不会再叠加一次 Core 层面的 2700 重试。

**特殊情况：** 删标记这一步本身也失败时，标记停在“发送中”。同一个消费者（同一个 owner）再抢会得到 `SEND`，照样重发；别的消费者在租约（`ClientSendLeaseMs`，60 秒）过后得到 `ACK_ONLY`，只确认不发，180 秒后 Core 报 2700。

**影响面：** Redis 抖动的那一刻，所有“请求刚成功、正在写结果”的消息都可能碰上；高峰时可能是几十条，不只一条。

### 2.4 两处同源的问题

根子都是 `submit` 的一个 boolean 分不清“没发出去”和“发出去了、只是后面出错”：

| 情况 | 现在 | 问题 |
|---|---|---|
| **A. 发出去了，结果没写进去**（2.2） | 删标记，60 秒后重发 | 重复发送（本设计的主问题） |
| **B. 请求发出后 IO 异常**（读响应超时、连接被对端断开） | `handleHttpStatusCode` 设状态，`dealHttpErrorResp` 写失败结果；写失败时同 A | 供应商可能收到了，删标记后同样会重发 |
| **C. 发出去后，解析响应时抛运行时异常** | `CommonHttpSession.submit` 的 `catch (Exception e) { return false; }`（第 323 行）把它当失败；消息类型如果还是 `SUBMIT`，`failToCoreAndAck` 写 `COMMUNICATION_ERROR`（2500） | 2500 的含义是“确定没发出去，重发安全”，配置说明建议把它放进重试码；**这里实际已经发出**。解析器类的异常在 `HttpInterface.invokeHandler` 里已被捕获，只有内置解析分支会走到这里，概率很低 |
| D. 请求前就失败（没有会话、拼参数出错） | 删标记，写 2500 | 正确，不用改 |

## 3. SMPP 外发对照

`OutboundStreamConsumer.handleMessageSending`（`src/com/king/gmms/messagequeue/OutboundStreamConsumer.java:329`）：

| 步 | 动作 |
|---|---|
| 1 | 抢发送标记，同 HTTP |
| 2 | `submitClientOutbound`：submit_sm 写进 socket，消息放进发送窗口 / pending（`SmppPendingSubmitManager`） |
| 3 | 返回 `SENT` 就 `ackSent`：标记 `D`，`XACK` + `XDEL`。**这时还没有结果** |
| 4 | 收到 submit_sm_resp（`MultiSmppSession.java:1601`）：写结果；失败时 `markResultPending`，由 `SmppPendingSubmitManager` 每隔 `min(BufferTimeout/3, 10 秒)` 重试，最多 `SMPPSubmitPendingResultMaxRetry`（50）次；`SMPPDRPendingMode=redis` 时还持久化在 Redis 里 |
| 5 | 50 次都失败：放弃，ERROR `result production exhausted`，释放窗口 |

所以：

- **短暂抖动：** 重试几次就写进去了，不重发，Core 拿到的是正确的 outMsgId。
- **Redis 连续不可用超过 180 秒：** Core outbox 先报 2700（`SUBMIT_RESP_ERROR`），之后 Client 补写的真实结果被 outbox 丢弃（同一次外发只认第一个结果）。是否重发看供应商的 `SMSOptionSubmitRetryCode` 有没有 2700。这是已知的“2700 可能重复”（配置说明 4.8、`Core模块功能与测试用例清单.md` 第 123 行），属于设计取舍，不在本次修复范围。
- **Client 进程在重试期间被停（内存模式）：** 结果丢失，同样由 Core 2700 兜底。

**结论：SMPP 没有“写结果失败就重发”的问题。** 本设计让 HTTP 达到和 SMPP 一样的保证。

## 4. 方案

### 4.1 原则

**请求一旦可能到达供应商，这次外发就不能再被重发。** 之后出的任何错都只影响“结果什么时候、以什么状态写给 Core”，不影响“发不发”。

### 4.2 session 报告“请求是否已发出”

在 `HttpSession` 加一个字段 `requestAttempted`（每条消息新建 session，没有多线程共用的问题）：

- `submit` 开始时置 false；
- **紧挨着网络调用之前**（`doGet`、`doPost`、`post`）置 true。之后不论是成功、IO 异常、解析异常，都算“可能已发出”；
- 提供 `isRequestAttempted()`。

`CommonHttpSession` 和 `CommonRESTHttpSession` 都要改。`submit` 的返回值含义不变（结果写没写进去），调用方再看这个字段。

### 4.3 消费者的处理

`HttpClientStreamConsumer.processMessage` 里，`submit` 返回 false 时：

| `isRequestAttempted()` | 处理 |
|---|---|
| false（情况 D） | 和现在一样：`failToCoreAndAck`，写 2500，写不进去就删标记、留待确认 |
| true（情况 A、B、C） | 1. **先把标记改为 `D`**（`SET marker D KEEPTTL`，幂等）<br>2. 消息交给结果重试（4.4），**不删标记**<br>3. 确认 stream 记录（`XACK` + `XDEL`）；确认失败也没关系，重投时抢到 `ACK_ONLY` 只会确认 |

情况 C 的消息类型如果还是 `SUBMIT`，改为 `SUBMIT_RESP` + **2700**（`SUBMIT_RESP_ERROR`，可能已发出），**不再写 2500**。

第 1 步要在最前面：它决定之后的重投还会不会发。第 1 步本身失败（Redis 还没恢复）时，交给结果重试一起重试（4.4），而且本进程登记这个标记“由我负责”，同一个消费者再抢到这条时不发（4.5）。

### 4.4 结果重试 `HttpResultRetry`

新类，每个 HTTP Client 进程一个，和 SMPP 的 pending 结果重试相同的做法：

```text
任务 = { 消息（已是 SUBMIT_RESP，带 outMsgId 和状态）, 标记 key, stream key, redisId, 已完成的步骤 }
每 HttpResultRetryIntervalMs（默认 5000）一轮，每个任务按顺序做没做完的步骤：
    1. 标记改为 D
    2. produceResult(消息)
    3. XACK + XDEL（失败不重试：重投时 ACK_ONLY 会做）
    全部完成 → 移除任务
重试 HttpResultRetryMax 次（默认 50）仍未写进结果 → 放弃，ERROR，Core outbox 180 秒后报 2700 兜底
```

- **进程停止：** `GracefulStop` 加一个任务，停机时把重试队列每个任务再试一次；剩下的打 ERROR 并列出 msgId，由 Core 2700 兜底。和 SMPP 内存模式一致。
- **为什么不持久化到 Redis：** 写不进 Redis 正是问题本身；放在进程内，和 SMPP 内存模式相同的取舍。
- **默认 5 秒 × 50 次约 4 分钟：** 超过 Core 的 180 秒。180 秒后才写进去的真实结果会被 outbox 丢弃，Core 按 2700 处理——和 SMPP 相同。

### 4.5 重投和抢标记

重试期间，这条 stream 记录可能还在待确认（第 3 步没做成），60 秒后会被自动重投：

| 重投时标记的状态 | 抢到 | 结果 |
|---|---|---|
| `D`（第 1 步已成功） | `ACK_ONLY` | 只确认，不发；结果由重试任务写 |
| `S`，别的消费者，租约内 | `BUSY` | 留在待确认，下次再看 |
| `S`，别的消费者，租约已过 | `STALE` → `ACK_ONLY` | 只确认，不发 |
| `S`，**同一个 owner** | 现在是 `SEND`（会重发） | **改：** 抢标记前先查本进程的重试队列，标记 key 在里面就当作 `BUSY`，不发 |

### 4.6 日志

| 现在 | 改为 |
|---|---|
| WARN `failed before HTTP response result`（所有失败都打这句） | 情况 D 保留原句；A、B、C 打 WARN `[HTTP-OUT] sent, result not written yet: retried, not sent again. outMsgId=..., reason=...` |
| — | 重试成功 INFO `[HTTP-RESULT-RETRY] written after N tries` |
| — | 放弃 ERROR `[HTTP-RESULT-RETRY] given up after N tries, Core reports 2700` |

### 4.7 不改的

- **HTTP DR 推送**（`submitMq=false`，推给 HTTP 客户的 DR）：没有发送标记，失败重推是有意的（DR 最多重发一次），不改。
- **SMPP 外发：** 不改（第 3 节）。
- **Core 的 2700 重试策略：** 不改，仍由 `SMSOptionSubmitRetryCode` 决定。

## 5. 配置项

| 配置项 | 默认值 | 生效方式 | 作用 |
|---|---|---|---|
| `HttpResultRetryIntervalMs` | `5000` | 热加载 | HTTP 外发请求已发出、结果没写进 Redis 时，补写结果的间隔 |
| `HttpResultRetryMax` | `50` | 热加载 | 补写的最多次数，之后放弃，由 Core 的 `CoreOutboundRespTimeoutMs` 报 2700 |

实现时按惯例：写进 `docs/配置项说明.md`（作用、默认值、所在文件、生效方式），加入 `GmmsUtility` 的热加载白名单。

## 6. 改动范围

| 文件 | 改动 |
|---|---|
| `HttpSession` | `requestAttempted` 字段和 `isRequestAttempted()` |
| `CommonHttpSession`、`CommonRESTHttpSession` | 网络调用前置位；`catch (Exception)` 分支保留返回 false |
| `HttpClientStreamConsumer` | 4.3、4.5 的分支；`failToCoreAndAck` 的 2700；日志 |
| `ClientSendMarker` | 新增 `markSent(msg)`（只把标记改为 `D`，保留过期时间） |
| 新增 `HttpResultRetry` | 4.4 |
| `GracefulStop` | 停机时清一次重试队列 |
| `GmmsUtility` | 两个配置项进白名单 |

## 7. 回归用例

**注入方法：** 结果流 key 写成字符串（`SET stream:core:results:{ssid}:{分片} x`），`produceResult` 的 `XADD` 就会报 `WRONGTYPE` 失败；`DEL` 之后恢复。比 HT-13 那样切断 Redis 连接稳定，可以精确控制失败几次、持续多久。

| 编号 | 场景 | 预期 |
|---|---|---|
| HR-01 | 写结果失败 1 次（对照组，现状就对） | 第二次写成功，outMsgId1，供应商 1 次请求 |
| HR-02 | 失败 2 次以上，10 秒后恢复；等过 `RedisStreamAutoClaimIdleMs` | 供应商**只收到 1 次请求**；结果是成功、outMsgId1；标记 `D` 后删除，stream 记录确认 |
| HR-03 | 同上，重投落到同一个消费者 | 不发（4.5 最后一行） |
| HR-04 | 一直失败，超过 `HttpResultRetryMax` | 不重发；ERROR 放弃日志；有 Core 时 180 秒后 2700 |
| HR-05 | 供应商读超时（请求已到）+ 写结果失败 | 不重发；结果最终写进去，状态按超时处理 |
| HR-06 | 解析响应时抛运行时异常 | 结果是 2700，不是 2500；不重发 |
| HR-07 | 请求前失败（没有会话） | 和现在一样：2500，标记删除 |
| HR-08 | 重试队列里有任务时 `kill`（SIGTERM） | 停机时再试一次；写不进去的 ERROR 列出 msgId |
| HR-09 | 标记改 `D` 本身失败 | 重试任务补做；期间重投不发 |
| HR-10 | 对照：SMPP 写结果失败 2 次 | 已有行为：pending 重试写进去，不重发（补一条用例固化现状） |
| HR-11 | HTTP DR 推送写结果失败 | 和现在一样（会重推） |

`ClientHttpIT` HT-13e 现在按“结果没写进去、记录留在待确认”断言，修复后改为断言 HR-02 的结果。

## 8. 文档修正

`docs/Client模块功能与测试用例清单.md` 第 14 节：

- “第二次也失败时，这条留在待确认里，发送标记也没有清除” → **标记被删掉了**（`release`）。
- “其他消费者接手：标记租约过期后只确认不重发” → 标记已删，**任何消费者接手都会重发**；只有删标记也失败时才是原来写的情况。
- “SMPP 外发是否有同样的问题还没有查” → 已查，没有（本设计第 3 节）。

修复上线后改为“已修复，见本设计”。

## 9. 风险和没覆盖的

- **结果晚到：** 补写超过 180 秒时，Core 已按 2700 处理，补写的真实结果被丢弃，是否重发看重试码。和 SMPP 相同。
- **进程在重试期间崩溃（`kill -9`）：** 重试队列丢失，由 Core 2700 兜底。
- **HTTP 外发的吞吐：** 正常路径不变；只有失败路径多一次 `SET`。

## 10. 实现和测试结果

按第 4 节实现，和设计不同的地方：

- **`requestAttempted` 的位置：** 放在 `HttpSession` 的 `doGet`、`doPost`（两个重载）开头，`CommonHttpSession` 走静态 `post` 的分支在调用前置位。所有 HTTP session 都覆盖到，不用逐个改子类。连接被拒（确定没发出去）也算“已发出”：只是不再重发，结果仍按失败状态写给 Core，由重试码决定。
- **第一次补写在消费者里做：** `sentResultNotWritten` 先改标记、再写一次结果（相当于原来 `failToCoreAndAck` 的第二次写），都成功就直接确认；不成功才交给 `HttpResultRetry`。
- **没有结果状态时一律 2700：** 消息还不是 `SUBMIT_RESP`，或状态码为负，都改为 `SUBMIT_RESP_ERROR`。
- **`GracefulStop` 任务在第一次有补写任务时才注册**，其他模块停机时的任务数不变。

**注入方法（7 节）实测可用：** 结果流 key 改成字符串后，`produceResult` 返回 false；恢复要用一段 Lua 把 `DEL` 和 `RENAME` 一起做，否则补写恰好落在两步之间时会被 `RENAME` 覆盖。测试进程里 Core 的结果消费者要停掉，否则结果会被读走、删掉，数不到。

**`ClientHttpIT` 结果（68 项全部通过）：**

| 编号 | 结果 |
|---|---|
| HR-02 | 请求 1 次，结果没写进去：标记为 `D`，补写任务 1 个；日志是新的 `[HTTP-OUT] sent, result not written yet` |
| HR-03 | 补写期间同一个消费者、另一个消费者各拿到一份副本：都不发 |
| HR-04 | 恢复后 0.3 秒内写进去：成功、outMsgId 是那一次请求的；之后请求 1 次、结果 1 条、待确认 0、标记已删 |
| HR-05 | 一直写不进去：20 次后放弃并打 ERROR；之后重投抢到“已发”，不发 |
| HR-06 | 供应商回 503、结果写不进去：恢复后写进 2600，请求 1 次 |
| HR-08 | 停机时再试一次，写不进去的 msgId 列在 ERROR 里 |
| HT-13e | 切断 Redis 后恢复，第一次写结果碰到断开的连接：补写进去，请求 1 次（原来是“结果 0 条、记录留在待确认”） |

**没有用例：** HR-07（解析响应时抛异常：本地的解析器都不会抛到 `submit` 这一层）、HR-09（标记改 `D` 也失败：`HttpResultRetry` 会重试这一步，没有单独注入）、HR-10（SMPP 对照，代码路径没改）、HR-11（HTTP DR 推送，代码路径没改）。
