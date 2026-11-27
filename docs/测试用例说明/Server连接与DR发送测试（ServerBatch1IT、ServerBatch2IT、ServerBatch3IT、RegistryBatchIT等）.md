# Server 连接与 DR 发送测试（ServerBatch1IT、ServerBatch2IT、ServerBatch3IT、RegistryBatchIT、ServerConnectionRaceCheck、DrShardCheck、InlineDrCheck）

> 日期：2026-11-27。用例代码在 `regression/test/it/`（`*Check` 在 `regression/test/com/king/...`，unit 模式），都在 `regression/suites.txt`：
>
> | 套件 | 模式 | 项数 | 内容 |
> |---|---|---|---|
> | ServerBatch1IT | it-server | 16 | 共用的入向线程池、绑定超时和连接数上限、DR 窗口满时推迟 |
> | ServerBatch2IT | it-server | 23 | 日志链、共用的会话保活线程、有界的发送队列、客户回应后才确认 DR |
> | ServerBatch3IT | it-server | 24 | DR 的结果和确认在一个 Lua 里、enquire_link 的间隔、DR 等回应的存放方式 |
> | RegistryBatchIT | it-server | 32 | 会话登记：批量写、PDU 刷新兜底、能扛住失败的对账 |
> | ServerConnectionRaceCheck | unit | 9 | 旧会话关闭和新会话绑定同时发生 |
> | DrShardCheck | unit | 16 | Server、Core 读哪些 DR 分片 |
> | InlineDrCheck | unit | 7 | 同步发 DR 的开关在线程上的作用范围 |
>
> Server 的协议功能在《Server功能测试》，多个 Server 进程在《多Server与故障恢复测试》。

## 1. 测什么

| 部分 | 内容 |
|---|---|
| 入向处理 | 所有连接共用一个工作线程池（不再每个连接一个 `CustReceiver_` 池）；池满时任务放回重试，不在分发线程上处理；一个客户 Redis 卡住时 enquire_link 照样马上回应 |
| 连接管理 | 绑定超时、每个 IP 的连接数、总连接数；共用的会话保活线程（不再每个连接一个 `ServerSessionTrd`）；客户不读数据时发送队列有上限 |
| DR 发送 | 客户窗口满时推迟，不重复、不进死信；客户回 deliver_sm_resp 以后才确认 DR 流里的条目（`ServerDrAckOnResp`），崩溃时没回应的留给 auto-claim；结果和确认在一个 Lua 里 |
| DR 等回应的存放 | `SMPPDRPendingMode`：memory、redis、hybrid |
| 会话登记 | Core 选 DR 发往哪个 Server 时查的 `smpp:server:*`：绑定时批量写，对账定时刷新，PDU 刷新兜底；Redis 卡住、出错时对账照常 |
| 日志 | 一条提交、一条 DR 在 Server 里每一步一行 INFO |

## 2. 怎么跑、怎么判定

```
regression/run.sh ServerBatch1IT     # 其余套件同样
cat regression/.work/results/ServerBatch1IT.txt
```

- 环境：测试服务器上的回归环境（JDK 17，本地 Redis 16379）。进程内启动真实的 `MultiSmppServer`，用 SMPP 测试客户端绑定、提交、回应 DR。
- 本程序扮演 Core：往客户的 DR 流 `stream:dr:pending:{客户}:{Server模块}:*` 写 DR，读 Server 写给 Core 的 DR 结果。
- 判定 Redis 往返次数时用 Redis `MONITOR` 数命令（MGET、SETEX、ZADD、EVAL）；判定线程时看线程名和线程栈采样。
- 让 Redis 卡住：`CLIENT PAUSE`。
- unit 模式的三组不起进程，直接调用类。

## 3. 公共配置

客户用回归环境基础 cm.cfg 里的 SMPPCUST20～SMPPCUST29，例如：

```
[SMPPCUST27]
CCBSSID: 27
A2PPlayerType: operator
Protocol: SMPP
ConnectionType: MC
WindowSize: 20
IncomingConnection: (ConnName=SMPPCUST27_c1,userName=c27,password=p27,URL=127.0.0.1,SessionNumber=200,MinReceiverNumber=1,MaxReceiverNumber=5,BindMode=TRX,Version=V34)
```

用户名 `c{ssid}`、密码 `p{ssid}`。各用例按需要在测试里改客户的 `BufferTimeout`（3 秒）、`ConnectionSilentTime`（3 秒）、`ServerEnquireLink`、`EnquireLinkTime`、`EnquireLinkFailureNum`、`SMPPDRPendingMode`、窗口大小。

测试里改的 GmmsConfig：

| 配置 | 值 | 套件 |
|---|---|---|
| `SmppServerInboundWorkerMax`、`SmppServerInboundWorkerQueue` | 2、1 | ServerBatch1IT |
| `SmppServerInboundMaxInFlightPerSsid` | 1 | ServerBatch1IT |
| `SmppServerBindTimeoutMs` | 2000 | ServerBatch1IT |
| `SmppServerMaxConnectionsPerIp`、`SmppServerMaxConnections` | 3、1 | ServerBatch1IT |
| `SmppPduLogEnable` | false（热加载） | ServerBatch2IT |
| `ServerDrAckOnResp` | false | ServerBatch2IT S2-05f |
| `RedisStreamBackPressureEnable`、`RedisStreamMonitor.BlockXLEN` | true、1 | ServerBatch3IT（让结果写不进去） |
| `SmppServerSessionRegistryEnable` | 先 false，再 true（热加载） | RegistryBatchIT |
| `SmppServerSessionRegistryBatchSize` | 200、10 | RegistryBatchIT |
| `SmppServerSessionRegistryReconcileIntervalMs` | 200、1000、2000 | RegistryBatchIT |
| `SmppServerSessionRegistryRefreshIntervalMs` | 2000、4000、180000 | RegistryBatchIT |
| `SmppServerSessionRegistryLuaCheck` | false | RegistryBatchIT |

## 4. 用例

### 4.1 入向线程池和连接上限（ServerBatch1IT）

| ID | 情况 | 预期 / 2026-11-27 实测 |
|---|---|---|
| S1-01a～c | 10 条提交 | 经共用池都回 ROK；没有每个连接的 `CustReceiver_*` 池；没配 `SmppServerInboundSharedPoolEnable` 时就是打开的 |
| S1-02 | 工作线程池 | 核心数 = 最大数 = 2，池满时拒绝（不用 CallerRuns） |
| S1-03a～c | 5 个客户各 200 条，2 个工作线程 | 1000 条都接受、0 错误；池满 1071 次（任务放回重试）；255 次线程栈采样里分发线程从没自己处理 PDU |
| S1-04/04b | 一个客户的 Redis 暂停 2 秒，它有 29 条在排队（每个客户同时只处理 1 条） | enquire_link 1 毫秒就回应；之后积压也回应（28 条成功，2 条在暂停期间 Redis 超时回 `ESME_RSYSERR`） |
| S1-05 | 连上不绑定 | 2001 毫秒后关闭（`SmppServerBindTimeoutMs=2000`） |
| S1-06a/b | 同一 IP 第 4 个连接（上限 3，已有 1 个） | 马上关闭；关掉一个后新的能连上并绑定 |
| S1-07 | `SmppServerMaxConnections=1` | 新连接关闭 |
| S1-08a～c | 客户窗口一直满，120 条 DR | 都发出，不重复，推迟的不进死信 |

### 4.2 日志、线程、发送队列、DR 确认（ServerBatch2IT）

| ID | 情况 | 预期 |
|---|---|---|
| S2-01a | 一条提交的日志 | `[RECV]` → `[SUBMIT-IN]` → `[STREAM-PUT]` → `[SUBMIT-TO-CORE]` |
| S2-01b | — | INFO 不再打整条消息 |
| S2-01h | — | `[SUBMIT-TO-CORE]` 写明分片和 Core（`core=CoreEngineS2`） |
| S2-01c/d | 一条 DR 的日志 | `[STREAM-GET]` → `[DR-OUT]` → `[DR-RESP]` → `[DR-RESULT-TO-CORE]`；`[STREAM-ACK]` 在 `[DR-RESP]` 之后 |
| S2-01e/f | — | `[STREAM-GET]` 用简短格式（454 字符）；重复的 “resolved session” 不再是 INFO |
| S2-01g | `SmppPduLogEnable=false`（热加载） | 没有 PDU 行，业务行保留 |
| S2-02a | 保活线程 | 没有每个连接的 `ServerSessionTrd`；共用的 `SmppSessionKeeper` 4 个 |
| S2-02b | 再加 20 个连接 | 多 20 个接收线程和 20 个发送线程（每个连接 2 个） |
| S2-03a～c | 绑定超时；绑定后不说话（`ConnectionSilentTime` 3 秒）；Server 主动保活 | 3004 毫秒关闭；3903 毫秒关闭；发了 2 次 enquire_link，有回应就保持 |
| S2-04a | 客户不读数据 | 发了 217 个 PDU 后队列到 200，发送失败，关闭连接 |
| S2-05a | 发了 5 条 DR，客户不回 | 条目都留在待确认里（这时崩溃，auto-claim 会接手） |
| S2-05b/c | `BufferTimeout` 3 秒到 | 写失败结果，确认条目；每条 DR 一个结果 |
| S2-05d/e | 回得快的客户，300 条 DR | 都发出、都确认（待确认 0），不重复（回应在登记前、后到都能确认） |
| S2-05f | `ServerDrAckOnResp=false` | 发出就确认（以前的行为），不等回应 |

### 4.3 结果和确认一个 Lua、保活间隔、DR 等回应的存放（ServerBatch3IT）

| ID | 情况 | 预期 / 实测 |
|---|---|---|
| S3-01a | 每条 DR 的 Lua 调用 | 结果和确认合在一起 1.07 次（以前 2.03 次） |
| S3-01c | memory 模式 | 只有结果+确认那一次：1.07 |
| S3-01b | — | 每条 DR 只确认一次：192 条在结果的 Lua 里，8 条单独确认（客户在 DR 线程登记前就回了） |
| S3-02a/b | 发出后客户不回 | 留在待确认；超时后写一个失败结果并确认 |
| S3-03a～c | 客户回了，但结果写不进去 | 不确认，不写结果；等回应管理器重试时写结果并确认 |
| S3-04 | 有回应的保活 | 5.5 秒 5 次（每个 `EnquireLinkTime`；以前每个 `FailEnquireLinkTime` 最多 1 次） |
| S3-05 | 不回应的保活 | 4103 毫秒、3 次后关闭（`EnquireLinkFailureNum` 3） |
| S3-06 | 10 个新会话，同时从 2 个分片流来第一批 DR | 200 条都发出，每个回应都对上（0 个对不上），都确认 |
| PM-01/02 | memory | 50 次放入和取出，每条 DR 不访问 Redis（0 次 EVAL），没有 pending key |
| PM-03/04 | redis、hybrid | 写 pending key，回应时删掉 |
| PM-05a/b | redis 模式写的，客户改成 memory | key 还在，它的回应照样删掉 |
| PM-06 | 从 memory 改成 redis | 删旧的没有影响，下一条照常写 |
| PM-07a/b | memory 模式下一个无关的残留 key | 回应不会删它；扫描报它的超时结果，不丢 |
| PM-08a/b | redis 模式经过真实会话 | 20 条 DR 发出并确认，没有残留 |

### 4.4 会话登记（RegistryBatchIT）

| ID | 情况 | 预期 / 实测 |
|---|---|---|
| RB-00a/b | 登记关着；热加载打开 | 没有登记；下一轮对账就登记（以前根本没有对账任务） |
| RB-01a～d | 绑定 | 1 次 MGET + 一个管道（2 次 SETEX、5 次 ZADD）；明细字段和 TTL 都对；索引 module、active、conn、node、slot 都写了；Core 的查找能找到 |
| RB-02 | 断开 | 登记删掉 |
| RB-03a～d | 对账 55 个连接，每批 200 | 每轮 1 次 MGET（2 轮 2 次），没有单独的 GET；每个连接 2 次 SETEX（110）；热加载改成每批 10 后每轮 6 次 MGET；每轮打日志（成功、失败、删除数） |
| RB-04 | enquire_link 每 100 毫秒，刷新间隔 4 秒，对账每 1 秒 | PDU 路径从不写 |
| RB-05a～d | 客户 20 的登记被删掉，对账停了 | 过了刷新间隔后的下一个 PDU 恢复登记；3 秒 60 个 PDU、间隔 2 秒，最多写 2 次（实际 1 次）；日志写明对账没到 |
| RB-05e | 没有 PDU 的会话 | **已知限制**：对账停着时不会恢复 |
| RB-05f/g | 对账超过 5 个间隔没完成 | ERROR；对账恢复后，没有 PDU 的会话也恢复 |
| RB-06a～d | Redis 暂停超过 socket 超时 | 记批量失败；之后照常对账，登记还在；不断开任何客户的连接；恢复健康 |
| RB-07a/b | 一轮里出了 Error | 记 ERROR，之后照常（以前定时任务会永久取消） |
| RB-08a～c | 一个客户读不出来 | 这一轮照常，但跳过清理，什么都不删；别的客户照常刷新 |
| RB-09a | 本模块过时的登记 | 下一轮正常对账删掉 |
| RB-10a/b | 不在存活集合里、但在这一轮开始后写进来的 | 保留（以前会删掉）；对账恢复清理误删的 |
| RB-11 | 对账 955 个连接 | 30、28、29 毫秒（一个一个做每个约 8 毫秒） |

### 4.5 会话关闭和绑定同时发生（ServerConnectionRaceCheck）

对 `MultiServerConnectionImpl`、`NodeServerConnection` 两种连接：

| 情况 | 预期 |
|---|---|
| 旧会话关闭的线程删掉旧会话后检查数量 | 正确 |
| 旧会话关闭的同时新会话绑定 | 连接状态 CONNECT（以前是 DISCONNECT，但有活着的会话，DR 发不过去） |
| — | 新会话在连接里（DR 只在 CONNECT 时经连接池发出） |
| DISCONNECT 且有活着的会话时，第二个会话绑定 | 又变成 CONNECT |
| 最后一个会话单独关闭 | DISCONNECT，没有会话 |
| 之后重新绑定 | CONNECT |

### 4.6 DR 分片（DrShardCheck）

| 情况 | 预期 |
|---|---|
| 默认（`MyShards=0`） | 本模块的分片 0、1 都读；别的模块的不读；没有模块名的旧 key 读；没有分片的 key 读 |
| 打开分片过滤 | 分片 1 跳过，分片 0 读，别的模块不读 |
| MT 的分片过滤 | 不变 |
| Core 的供应商 DR 分片（`MyShards=0`） | 分片 0 读，分片 1 不读；没有分片的旧 key 每个 Core 都读 |
| — | 同一个发送方、接收号码的 DR（同一条 MT 的各段）落在同一个分片，例如 `stream:core:inbound-dr:9991:1` |
| 200 个接收号码 | 分布在全部 4 个分片 |
| `CoreInboundDRShardEnable=false` | 没有分片的 key |

### 4.7 同步发 DR 的作用范围（InlineDrCheck）

| 情况 | 预期 |
|---|---|
| 默认 | 关 |
| `withInlineDrDispatch` 里 | 开 |
| 返回后；嵌套调用到最外层返回前一直开；最外层返回后；任务抛异常后 | 关 |
| 别的线程 | 照常用线程池 |

## 5. 最近一次结果

| 时间 | 环境 | 结果 |
|---|---|---|
| 2026-11-27 | 测试服务器全量回归（提交 `32c4d33` 的代码） | 7 组共 127 项，全部通过；表里的次数、毫秒数取自这次的实测输出 |

## 6. 没有覆盖到的

| 内容 | 说明 |
|---|---|
| 生产规模的连接数（几千个客户连接） | RB-11 只到 955 个 |
| 对账停止时没有 PDU 的会话 | RB-05e 记为已知限制 |
| Server 进程崩溃后的 DR | 在《多Server与故障恢复测试》（DrTransferIT、DrLivenessIT） |
