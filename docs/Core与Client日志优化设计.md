# Core 与 Client 日志优化设计

## 1. 目标

- **每个处理环节一条 INFO 业务日志**：用 msgId grep 就能看出一条消息在 Core、Client 走到了哪一步。
- **每条 CDR 一行 `[CDR]` 日志**：写出了哪种 CDR、状态是什么，不用去翻 CDR 文件。
- **其他日志降级**：每条消息都会打、但不说明问题的日志改为 DEBUG；每条消息都会重复的 WARN 改为限频。
- **WARN / ERROR 只留真正要处理的问题**：正常的兜底逻辑不再打 WARN；真正的失败（结果没写进 Redis、CDR 没写出）从 INFO 提到 ERROR。

本批没有新增配置项。`[STREAM-PUT]` / `[STREAM-GET]` / `[STREAM-ACK]` 仍由已有的 `RedisStreamTraceLogEnable`（默认 true）控制，其他业务日志不受开关影响。

## 2. 日志格式

统一为 `[标签] key=value ...`，行首是 msgId（`SystemLogger` 的 `log.info(msg, ...)`）。

```
INFO  ... IOSMSChannelRouter 8888_LC1 [MT-ROUTE] ossid=8888 rssid=8 by routingRelay
```

## 3. Core 的日志链路

### 3.1 MT

| 环节 | 日志 | 说明 |
|---|---|---|
| 从 `stream:mt:pending` 读到 | `[STREAM-GET] flow=server->core` | 已有 |
| 入库 CDR | `[CDR] type=IN_SUBMIT status=... ossid rssid split inMsgId outMsgId` | 新增，见第 5 节 |
| 选出通道 | `[MT-ROUTE] ossid=... rssid=... by <路由方式>` | 原来是 `get Rssid: x by y`；没选出时改为 DEBUG |
| 写到 `stream:mt:routed` | `[STREAM-PUT] flow=core->client` | 已有 |
| 交给 Client | `[MT-TO-CLIENT] rssid=... redisId=...`（HTTP 通道带 `protocol=HTTP`） | 新增 |
| 通道限速暂存 | `[MT-HOLD] rssid backlog tps`、放行时 `[MT-RELEASE] rssid heldMs redisId` | 放行原来没有 INFO |
| 拒绝 | `[MT-REJECT] ossid status ... rssid` | 新增 |
| 走 DB 兜底 | `[MT-FALLBACK] ossid rssid status ...` | 新增 |
| 存储模式 | `[MT-STORE] ossid store mode: kept in SMQ / scheduled` | 原来是无标签的文字 |
| 确认消费 | `[STREAM-ACK] flow=server->core result=ok` | 原来去重路径（`StreamAttempt`）确认后没有这一行，现在补上 |

### 3.2 出向结果（submit_resp、超时结果）

| 环节 | 日志 |
|---|---|
| 读到 | `[STREAM-GET] flow=result->core` |
| 处理 | `[RESULT] type=Submit Resp status=0 ossid rssid split outMsgId outbox=FIRST/UNTRACKED` |
| 迟到或重复的结果 | `[RESULT-DROPPED]`（INFO，原来是 WARN：属于正常的去重，不需要处理） |
| CDR | `[CDR] type=OUT_SUBMIT ...` |
| 确认消费 | `[STREAM-ACK] flow=result->core` |

### 3.3 供应商 DR

| 环节 | 日志 |
|---|---|
| 读到 | `[STREAM-GET] flow=client->core`（行首是供应商 DR 自己的 id） |
| 匹配原消息 | `[VDR-MATCH] outMsgId=... status=... rssid=... -> msgId=... ossid=... (redis)` 或 `(DB)` |
| 匹配不到 | `[VDR-MATCH] ... not in redis, looking up the DB`；DB 也没有时 `[VDR-STORE] ... stored for later matching` |
| CDR | `[CDR] type=OUT_DELI_REP ...` |
| 发给客户 | `[DR-TO-SERVER] ossid status stream redisId` 或 `[DR-TO-HTTP] ossid status redisId` |
| 确认消费 | `[STREAM-ACK] flow=client->core` |

`[VDR-MATCH]` 一行里同时有供应商 DR 的 id（行首）和原消息的 msgId，从任一个 id 都能把整条链串起来。

## 4. Client 的日志链路

### 4.1 SMPP 外发

| 环节 | 日志 |
|---|---|
| 发出 submit_sm | `[MT-OUT] rssid=... session=... seq=... split=...`（新增） |
| 收到 submit_sm_resp | `[SUBMIT-RESP] seq=... pduStatus=... status=... outMsgId=...`（原来是 `SubmitSM receivies response, pdu status:...`，没有 seq 和 outMsgId） |
| 结果写给 Core | `[STREAM-PUT] flow=result->core` |
| 外发 stream 确认消费 | `[STREAM-ACK]`（原来走发送标记 `ClientSendMarker` 的确认消费没有这一行，现在补上） |
| 没等到响应 | WARN `[RESP-TIMEOUT] no response from session=... seq=... in time, result type=... status=...`（原来是 INFO，无会话和序列号）。redis pending 模式同样是 `[RESP-TIMEOUT]` |

### 4.2 SMPP 供应商 DR

| 环节 | 日志 |
|---|---|
| 收到 deliver_sm | `[VDR-IN] outMsgId=... status=... seq=...`（原来是 `Received DR,outmsgid=...`） |
| 写给 Core | `[STREAM-PUT] flow=client->core` |
| 回 deliver_sm_resp | `[VDR-TO-CORE] outMsgId=... status=..., deliver_sm_resp ROK`（新增） |
| 写 Redis 失败 | WARN `[VDR-TO-CORE] ... not written to Redis, deliver_sm rejected (ESME_RX_T_APPN)` |

原来拒绝 deliver_sm 时还会打一行 INFO `send response ok.`，容易误以为成功，已删除。

### 4.3 HTTP 外发

| 环节 | 日志 |
|---|---|
| 发出并收到响应 | `[HTTP-RESP] outMsgId=... resp=...`（原来是 `received submit resp ...`） |
| 外发完成 | `[HTTP-OUT] rssid=... sent, outMsgId=...`（新增） |

## 5. CDR 日志

每次调用 `CDRManager` 写 CDR，打一行：

```
[CDR] type=OUT_SUBMIT status=0 ossid=8888 rssid=8 split=0 inMsgId=IN-8888_LC1 outMsgId=OUT-LC1
```

- 异步写和同步写都失败时，改为 ERROR（原来没有任何提示）。
- 去重跳过时一行 `[CDR] ... already written by an earlier attempt of this entry, skipped`。
- 原来在 INFO 打印整条 CDR 文本的地方改为 DEBUG（CDR 文件里已经有）。

## 6. 降级与 WARN / ERROR 调整

### 6.1 降为 DEBUG（每条消息都打、但不说明问题）

| 位置 | 原日志 |
|---|---|
| `MessageProcessorThread` | `Can not find customer session ...`、`Can not find moduleName ...`（每条 DR 都会打） |
| 各路由类 | 没选出 rssid 的那一行 |
| `RedisConnection` | stream 还没有消费组时的 NOGROUP |
| `CommonHttpSession` | `post msg ...`（整个请求体，可能含内容和账号） |
| `MultiSmppSession` | `put message into exMsgQueue successfully.` |
| `SmppPendingSubmitManager` / `SmppPendingDrManager` | `SMPP pending ... timeout result produced`（与 `[RESP-TIMEOUT]` 重复） |
| `CDRManager` | 整条 CDR 文本 |

### 6.2 限频（每 10 分钟最多一行，附带这段时间内的次数）

新增 `LogRateLimiter`（`com.king.gmms.util`）：每个 key 每个间隔最多放行一行，返回期间累计的次数。key 数量上限 10000，超过时清空，防止异常输入让 map 无限增长。

| 位置 | 日志 | key |
|---|---|---|
| `SmppServerDrRouteResolver` | DR 选 Server 模块时的各种兜底（bind 注册表没有数据、模块不在线等） | ossid + 原因 |
| `CommonHttpClientFactory.getSession` | 通道不存在、不是 CommonHttpClient、未初始化 | ssid |

`SmppServerDrRouteResolver` 里"注册表关闭""没有模块数据"属于正常兜底，改为 INFO；其他原因仍是 WARN。

### 6.3 删除

| 位置 | 原日志 | 原因 |
|---|---|---|
| `InternalCoreEngineSession` | WARN `TMP-DRLOOKUP ...`（带发送方、接收方、短信内容） | 临时排查用；每条匹配不到的 DR 都打 WARN，还打印了短信内容。由 `[VDR-MATCH]` 取代 |
| `MultiSmppSession.sendDeliverSMErrorResp` | INFO `send response ok.` | 拒绝时也打"ok"，误导 |

### 6.4 提级或补充信息

| 位置 | 调整 |
|---|---|
| `CommonHttpSession` | `Send response to core engine failed!` 从 INFO 改为 ERROR：结果没写进 Redis，Core 收不到 |
| `CDRManager` | CDR 写失败从无日志改为 ERROR |
| `MultiSmppSession` | 响应超时从 INFO 改为 WARN `[RESP-TIMEOUT]`，带会话名和序列号 |
| `ContentTemplate` | 解析内容模板异常时带上异常栈 |

## 7. 改动的文件

Core：`StreamQueueManager`（新增 `traceAck`）、`MTStreamConsumer`、`InboundDRStreamConsumer`、`ResultStreamConsumer`、`CDRManager`、`MessageStoreManager`、`InternalCoreEngineSession`、`MessageProcessorThread`、`ChannelReleaseDispatcher`、`SyncSubmitRouter`、6 个路由类、`SmppServerDrRouteResolver`、`RedisConnection`、`ContentTemplate`。

Client：`MultiSmppSession`、`MultiSmppPduProcessor`、`ClientSendMarker`、`HttpClientStreamConsumer`、`CommonHttpSession`、`CommonHttpClientFactory`、`SmppPendingSubmitManager`、`SmppPendingDrManager`。

新增：`com.king.gmms.util.LogRateLimiter`。

## 8. 对现有排查方式的影响

以下原日志文本已经改变，按旧文本 grep 的脚本或告警需要同步修改：

| 原文本 | 现在 |
|---|---|
| `get Rssid: ` | `[MT-ROUTE]` |
| `SubmitSM receivies response` | `[SUBMIT-RESP]` |
| `Received DR,outmsgid=`（SMPP Client） | `[VDR-IN]` |
| `Send Delivery Report to stream=` | `[DR-TO-SERVER]` |
| `Late or duplicate submit result dropped` | `[RESULT-DROPPED]` |
| `received submit resp` | `[HTTP-RESP]` |
| `do not receive the response after timeout` | `[RESP-TIMEOUT]` |
| `TMP-DRLOOKUP` | 已删除，见 `[VDR-MATCH]` |
