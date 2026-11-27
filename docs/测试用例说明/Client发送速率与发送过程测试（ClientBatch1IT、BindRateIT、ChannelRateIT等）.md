# Client 发送速率与发送过程测试（ClientBatch1IT、ClientMarkerBatchIT、BindRateIT、ChannelRateIT、ThrottleSyncIT、SenderPaceCheck、ClientPendingIT、ClientLifecycleIT、LogChainClientIT）

> 日期：2026-11-27。用例代码在 `regression/test/it/`（SenderPaceCheck 在 `regression/test/com/king/...`，unit 模式），都在 `regression/suites.txt`：
>
> | 套件 | 模式 | 项数 | 内容 |
> |---|---|---|---|
> | ClientBatch1IT | it | 25 | 每条只发一次、限速不卡线程、慢通道不拖累别的通道 |
> | ClientMarkerBatchIT | it | 12 | 发送标记按批领取、确认 |
> | BindRateIT | it-mod:MultiSmppClient1 | 20 | 每个绑定的速率 `SMSOptionOutgoingThrottlingNumPerBind` |
> | ChannelRateIT | it | 40 | Core 侧的通道速率（暂存 + 放行） |
> | ThrottleSyncIT | it | 7 | 入向限速在内存计数、同步到 Redis |
> | SenderPaceCheck | unit | 3 | 发送节拍 |
> | ClientPendingIT | it-server | 14 | 等 submit_sm_resp 的存放方式 `SMPPSubmitPendingMode` |
> | ClientLifecycleIT | it-mod:MultiSmppClient1 | 36 | 启动时的积压、死信、重新加载速率和通道、结果流满、多节点通道、SMPP 3.3 |
> | LogChainClientIT | it-server | 11 | Client 的日志链 |
>
> SMPP 连接和 PDU 字段在《Client SMPP连接与PDU测试》，HTTP 通道的速率和超时在《Client HTTP通道测试》。

## 1. 测什么

| 部分 | 内容 |
|---|---|
| 每条只发一次 | Client 发之前在 Redis 写发送标记（`ClientSendIdempotency`），重投、auto-claim、发送者中途停住时都不会让供应商收到两次 |
| 限速 | 通道速率 `SMSOptionOutgoingThrottlingNum`（每秒），每个绑定的速率 `SMSOptionOutgoingThrottlingNumPerBind`；等许可时不占工作线程；不突发 |
| 隔离 | 慢通道、大积压不拖累别的通道 |
| Core 侧速率 | 通道配了 `SMSOptionCoreProcessorThrottlingNum` 时，Core 先把 MT 放进暂存流 `stream:core:hold:{通道}`，再按速率放行到出向流；暂存满、暂存太久（`CoreHoldMaxSeconds`）回失败 |
| 入向限速同步 | 多个节点共用一个入向限额：各自在内存计数，按 `ThrottleInSyncIntervalMs` 同步到 Redis |
| 等回应的存放 | `SMPPSubmitPendingMode`：memory（内存）、redis、hybrid |
| 长时间运行 | 启动时的积压、死信、窗口满的重试延迟、回应和超时的竞争、结果流满、重新加载 cm.cfg |
| 日志 | 一条 MT、一条 DR 在 Client 里每一步一行 INFO |

## 2. 怎么跑、怎么判定

```
regression/run.sh BindRateIT     # 其余套件同样
cat regression/.work/results/BindRateIT.txt
```

- 环境：测试服务器上的回归环境（JDK 17，本地 Redis 16379）。
- 供应商：
  - BindRateIT、ClientLifecycleIT 用假 SMSC（`FakeSmsc`，见《Client SMPP连接与PDU测试》）。BindRateIT 记下每个绑定收到每条 submit_sm 的时间，按秒、按 100 毫秒数条数。
  - ClientBatch1IT、ClientMarkerBatchIT 用进程内的假会话（`FakeVendor`），记下每条的发送次数，可以让它断开、或在 submit 里卡住（模拟发送者停住）。
  - ClientPendingIT、LogChainClientIT 用真实的 SMPP 链路：通道 41 的 Client 会话绑定到本进程的真实 MultiSmppServer（客户 42），Server 扮演供应商。
- 本程序扮演 Core：往出向流写 MT，读结果流。
- “每条只发一次”：数供应商收到的每个消息 ID 的次数。

## 3. 公共配置

### 3.1 回归环境基础 cm.cfg 里用到的块

```
[CUST7001]
CCBSSID: 7001
A2PPlayerType: operator
Protocol: SMPP

[VENDOR9]
CCBSSID: 9
A2PPlayerType: operator
Protocol: SMPP
SMSOptionCoreProcessorThrottlingNum: 2
CoreHoldMaxSeconds: 5

[VENDOR11]
CCBSSID: 11
A2PPlayerType: operator
Protocol: SMPP
SMSOptionOutgoingThrottlingNum: 2

[VENDOR12]
CCBSSID: 12
A2PPlayerType: operator
Protocol: SMPP

[VENDOR13]
CCBSSID: 13
A2PPlayerType: operator
Protocol: SMPP
SMSOptionOutgoingThrottlingNum: 5
```

VENDOR8（`CoreHoldMaxSeconds` 默认 21600）见《长短信测试》3.1。

### 3.2 BindRateIT 的通道（假 SMSC 端口 17804）

公共部分：

```
[BR901]
CCBSSID: 901
A2PPlayerType: operator
Protocol: SMPP
ConnectionType: MC
IsInit: true
ReconnectInterval: 1
EnquireLink: false
BufferTimeout: 30
WindowSize: 100
（额外配置）
OutgoingConnection: (ConnName=BR901_c,userName=u901,password=p,URL=127.0.0.1,Port=17804,SessionNumber=2,BindMode=TRX,Version=V34,IsInit=true)
```

| 块 | WindowSize | 额外配置 | 连接 |
|---|---|---|---|
| BR901 | 100 | `SMSOptionOutgoingThrottlingNum: 100` | 1 个连接，2 个会话 |
| BR902 | 100 | `SMSOptionOutgoingThrottlingNumPerBind: 20` | 同上 |
| BR903 | 100 | `SMSOptionOutgoingThrottlingNum: 30`<br>`SMSOptionOutgoingThrottlingNumPerBind: 20` | 同上 |
| BR904 | 100 | `SMSOptionOutgoingThrottlingNumPerBind: 20` | 同上 |
| BR905 | 100 | `SMSOptionOutgoingThrottlingNumPerBind: 10` | 2 个连接，各 1 个会话 |
| BR906 | 100 | `SMSOptionOutgoingThrottlingNumPerBind: 20`（BR-06 改成 5） | 1 个会话 |
| BR907 | 100 | `SMSOptionOutgoingThrottlingNumPerBind: 10` | 1 个会话 |
| BR908 | 5 | `SMSOptionOutgoingThrottlingNumPerBind: 50` | 2 个会话 |
| BR909 | 5 | — | 2 个会话 |

### 3.3 ClientLifecycleIT 的通道（假 SMSC 端口 17797）

公共部分同 3.2（`WindowSize: 100`、`BufferTimeout: 60`，1 个会话）。

| 块 | 额外配置 |
|---|---|
| LV801、LV805 | — |
| LV802 | `SMSOptionOutgoingThrottlingNum: 2`（TW-07 改成 50） |
| LV803 | `BufferTimeout: 1` |
| LV804 | `WindowSize: 1` |
| LV806 | `BufferTimeout: 30` |
| LV807 | `ConnectionType: MN`（多节点），`ConnectionSilentTime: 300`，`SubmitPolicy_Node: (Policy=primary,Primary=N1)`，`MaxRetryTimes: 1`，两个节点的连接 N1（LV807_a）、N2（LV807_b） |
| LV808 | `SMSOptionSMPPVersion: V33` |
| LV809 | —（CF-03 时由重新加载加进来） |

### 3.4 GmmsConfig 和测试里改的值

| 配置 | 默认 | 测试里 | 套件 |
|---|---|---|---|
| `ClientSendIdempotency` | true | C1-01i 改成 false | ClientBatch1IT |
| `ClientSendLeaseMs` | — | 800（C1-01e 用 60000） | ClientBatch1IT |
| auto-claim | — | 打开，空闲 1500 毫秒、间隔 500 毫秒 | ClientBatch1IT C1-02 |
| `ClientSendMarkerBatch` | true | MB-04b 改成 false | ClientMarkerBatchIT |
| `CoreHoldRecoverIntervalMs`、`CoreHoldRecoverIdleMs` | — | 1000、1500 | ChannelRateIT |
| `RedisStreamMaxDeliveries` | — | 3 | ClientLifecycleIT CS-08 |
| `SMPPSubmitPendingMode`（cm.cfg 通道上） | memory | 用例里切换 memory、redis、hybrid | ClientPendingIT |

## 4. 用例

### 4.1 每条只发一次（ClientBatch1IT）

| ID | 情况 | 预期 |
|---|---|---|
| C1-01a | 正常 | 发一次，确认，标记随确认删掉 |
| C1-01b | 发出后确认丢了，重投 | 不再发，确认 |
| C1-01c | 另一个消费者正在发 | 不发，留在待确认里 |
| C1-01d | 发送者发到一半停了（占用过期） | 不再发，确认，不回失败结果（由 Core 的 outbox 报告） |
| C1-01e | 第一个发送者还在 submit 里，这时重投 | BUSY，不发 |
| C1-01f | 占用过期后 | 不发（不知道供应商收到没有），确认 |
| C1-01g | 会话断了 | 给 Core 回失败结果，确认，删掉标记 |
| C1-01h | 会话断了，但重投的这条之前已经发过 | 不回失败结果，只确认 |
| C1-01i | `ClientSendIdempotency=false` | 不写标记直接发 |
| C1-06a～c | auto-claim 在工作线程确认前读走 | 供应商只收到一次；没有残留标记；打日志 |
| C1-06d | auto-claim 先做完，工作线程的旧副本后到 | 只收到一次 |
| C1-06e | 工作线程死了，条目还在 | auto-claim 发出并确认 |
| C1-06f | 副本到时会话断了 | 已经发过的不回失败结果 |

### 4.2 限速和隔离（ClientBatch1IT）

| ID | 情况 | 预期 / 2026-11-27 实测 |
|---|---|---|
| C1-03 | 4 个工作线程，两个慢通道有积压 | 快通道的 20 条 101 毫秒发完，这时慢通道才发到 4/20 |
| C1-04 | — | 没有线程在 `RateLimiter.acquire` 里等（工作线程被放开） |
| C1-02a～c | VENDOR11（2 TPS）20 条 | 都发出；虽然等待超过 auto-claim 的空闲时间（1.5 秒），也没有发两次；用了 9629 毫秒（守住速率） |
| C1-02d | VENDOR13（5 TPS）60 条 | 每条一次 |
| C1-02e～g | — | 推迟的条目不算投递次数（死信流里没有）；没有残留标记；出向流都空了 |
| C1-05 | 工作线程池 | 核心数 = 最大数 = 配置的 4；池满时拒绝（不用 CallerRuns） |

### 4.3 发送标记按批（ClientMarkerBatchIT，VENDOR12 不限速，VENDOR13 5 TPS）

| ID | 情况 | 预期 |
|---|---|---|
| MB-01a～d | 一批读 30 条 | 每条发一次，都确认并删掉，没有残留标记；领取 1 次 Lua、确认 1 次 Lua（一条一条要 60 次） |
| MB-02a/b | 一批里混着：新的两条、之前发过的、别的发送者正在发的、停住的发送者的 | 只发新的两条；只有别人正在发的那条留在待确认里（带标记），其余确认、标记删掉 |
| MB-03a～c | 20 条 5 TPS | 1.2 秒时发出 6 条，等许可的 14 条没有标记；全部发完 3917 毫秒（不比限速快）；没有残留 |
| MB-04a～c | 打开 / 关闭 `ClientSendMarkerBatch` | 都是 30 条每条一次；关闭时 Lua 调用多（91 次 vs 33 次） |

### 4.4 每个绑定的速率（BindRateIT）

| ID | 通道 | 情况 | 预期 / 实测 |
|---|---|---|---|
| BR-00/00b | 全部 | — | 都绑定上；配置读到 20，默认 0 |
| BR-01 | BR901 | 只有通道速率 100 | 都发出，两个绑定差不多（155 / 145，差不超过 30） |
| BR-10/10b | BR902 | 每绑定 20，200 条 | 每条一次，每条一个结果 |
| BR-02a | BR902 | — | 任何一秒里每个绑定不超过 20（+1） |
| BR-02b | BR902 | — | 两个绑定加起来约 40 TPS：200 条 4938 毫秒 |
| BR-08 | BR902 | — | 均匀：任何 100 毫秒里每个绑定不超过 3 条（不突发） |
| BR-03 | BR903 | 通道 30、每绑定 20 | 取小的：150 条 4967 毫秒，每个绑定每秒不超过 21 |
| BR-04a/b | BR904 | 一个绑定断开（重新绑定被拒） | 剩下的那个还是 20，不变成 40：60 条 2938 毫秒 |
| BR-04c/d | BR904 | 绑定恢复 | 又约 40：80 条 1938 毫秒 |
| BR-05 | BR905 | 两个连接 | 每个连接的每个绑定每秒不超过 10（+1）：29 + 31 |
| BR-06a～c | BR906 | 重新加载改成 5 | 生效；重新绑定；之后 20 条每秒不超过 5（+1），3799 毫秒 |
| BR-07 | BR907 | 3 段的长短信，每绑定 10 | 3 条 submit_sm，间隔至少 75 毫秒（76 毫秒） |
| BR-09a | BR908 | 窗口 5、每绑定 50，一个绑定窗口满了 | 另一个绑定接着发：[5, 95] |
| BR-09b | BR909 | 窗口 5、没有每绑定速率 | 和以前一样：绑定 0 窗口满，其余推迟后从绑定 1 发：[5, 95]，9009 毫秒 |

### 4.5 Core 侧的通道速率（ChannelRateIT，客户 7001 → VENDOR9）

| ID | 情况 | 预期 / 实测 |
|---|---|---|
| CR-01 | 读配置 | VENDOR9 是 2 TPS / 暂存最多 5 秒；VENDOR8 默认 0 / 21600 |
| CR-02 | 没配速率的通道 | 直接写出向流，没有暂存流 |
| CR-03 | 10 条 | 都放进 `stream:core:hold:9`，出向流里没有，暂存的门铃标记写好 |
| CR-04a～c | 第 11 条（暂存满） | 不暂存；OUT_SUBMIT CDR 记 Throttled（2600）；给客户回失败 DR |
| CR-05a～f | 放行 | 2 秒放行 4 条（应在 3～6 条）；10 条都放行，用了 4608 毫秒（至少 3.5 秒）；暂存流空了、门铃标记删掉；放行时才写 outbox；放行的内容就是原来的 MT |
| CR-06a～d | 暂存超过 5 秒 | 从暂存流取出，不放行；OUT_SUBMIT CDR 记 EXPIRED（10200），每条一个失败 DR |
| CR-07 | 放行时已经过期的 MT | 失败，不发 |
| CR-08 | 同一次处理里同一部分暂存两次 | 只有 1 个暂存项 |
| CR-09 | 同一个暂存项放行两次 | 第二次 GONE，出向流只有 1 条 |
| CR-10a/b | 暂存项在一个死掉的消费者的待确认里 | auto-claim 取回并放行 |
| CR-11a～c | 重新加载改成 10 TPS | 10 条 1008 毫秒放行完（不到 2.5 秒），比 2 TPS 快 |
| CR-12a/b | 改成 0 | 新 MT 直接进出向流；暂存里剩下的照样放行完 |
| CR-13a/b | 客户 7001 路由到 VENDOR9 | 先暂存，再放行 |
| NPE-02a/b | DR 的通道不认识、没有 timeStamp | 不出空指针，照样转发 |

### 4.6 入向限速同步（ThrottleSyncIT，两个计数器代表两个节点，限额 10）

| ID | 情况 | 预期 |
|---|---|---|
| TS-01 | 一个节点，限额 5，同时来 8 条 | 放行 5 条，不访问 Redis |
| TS-02 | 节点 A 6 条，同步后节点 B | B 放行 4 条（一共 10） |
| TS-03 | — | Redis 里集群的计数是 10 |
| TS-04 | A 同步后看到 B 的 4 条 | 下一条是第 11 条，拒绝 |
| TS-05 | 到下一秒的一半 | B 上一秒的 4 条按 2 条算，A 的第一条是第 3 条 |
| TS-06/07 | — | 同步后计数 3；计数 key 有 TTL |

### 4.7 发送节拍（SenderPaceCheck）

| ID | 情况 | 预期 / 实测 |
|---|---|---|
| SP-01 | 4 个线程一共 1000 条/秒，发 300 条 | 约 300 毫秒（实测 570 毫秒） |
| SP-02 | 空闲一段时间后 | 不突发：100 条 99 毫秒 |
| SP-03 | 0 | 不限制：4000 条 0 毫秒 |

### 4.8 等回应的存放方式（ClientPendingIT，通道 41 → 本进程的 Server）

| ID | 模式 | 预期 |
|---|---|---|
| CP-00 | — | 会话绑定上 |
| CP-01a～c | memory | 100 条都有回应和结果；100 次提交没有一次访问 pending key 的 Lua；Redis 里没有 pending key |
| CP-02a～c | redis | 20 条都有回应和结果；20 次提交 20 次 Lua（写进、取走）；没有残留 |
| CP-03a/b | hybrid | 写 pending key，回应时删掉 |
| CP-04a/b | redis 模式写的，供应商改成 memory | key 还在；它的回应照样删掉 key |
| CP-05 | 从 memory 改成 redis | 删旧的没有影响，下一条照常写 |
| CP-06a/b | memory 模式下一个无关的残留 key | 回应不会删它；扫描报它的超时结果，不丢 |

### 4.9 长时间运行（ClientLifecycleIT）

| ID | 情况 | 预期 / 实测 |
|---|---|---|
| LC-00 | — | 都绑定上 |
| CS-09 | 启动前有积压，门铃标记丢了 | 消费者启动时找到并发出 |
| CS-08a/b | 一条一直处理不完 | 3 次（`RedisStreamMaxDeliveries`）后进死信，不发 |
| TW-05a～c | `WindowSize: 1`（LV804） | 第一条发出；窗口空了第二条发出；推迟的等 4 秒的重试延迟，从推迟时算（3503 毫秒） |
| RS-09a/b | 回应和 `BufferTimeout`（LV803，1 秒）赛跑 | 每条正好一个结果：成功 65、超时 35、没有结果 0；两种都出现了 |
| FT-07 | — | 两个结果分片都有条目 |
| FT-07a～c | 结果流满了 | 照样发送；满的时候不写结果；有空间后重试写出一个结果 |
| TW-07a | LV802 2 TPS | 4 秒 8 条 |
| TW-07b～e | `RELOAD_CUSTOMER` 改成 50 TPS | 生效；改了的通道重连，别的不重连；积压很快处理完，每条一个结果 |
| CF-03a～c | 重新加载加一个通道 LV809 | 连上，MT 能发，别的通道不动 |
| CF-04a～e | 重新加载去掉一个通道，它有 2 条在等回应 | 连接关闭；等回应的回失败结果 2700；之后发给它的回 2500；别的通道不动 |
| CN-12a～d | 多节点通道 LV807，主节点 N1 | 两个节点都绑定；全走 N1（10/0）；节点连接断开后重连，约 `ReconnectInterval`（1001 毫秒），不用等 `ConnectionSilentTime` 300 秒的睡眠 |
| CN-13a～f | SMPP 3.3（LV808） | 绑定上；interface_version 0x33；MT 发出、成功结果；回执回 ROK；回执里十进制的 ID 转成十六进制（`499602d2`，3.3 的 ID 是十六进制） |

### 4.10 日志链（LogChainClientIT）

| ID | 情况 | 预期 |
|---|---|---|
| L-S0 | — | 通道 41 的 Client 会话绑定到本地 Server |
| L-S1a～d | 一条 MT | 发出 submit_sm，回应写进 `stream:core:results`；日志依次 `MT-OUT` → `SUBMIT-RESP`（seq、PDU 状态、outMsgId）→ `STREAM-PUT`；没有 WARN/ERROR，旧的 `SubmitSM` 文字没有了 |
| L-S2a～c | 一条供应商 DR | 写进 `stream:core:inbound-dr:41`；日志 `VDR-IN`（outMsgId、状态）→ `VDR-TO-CORE`，回 deliver_sm_resp ROK |
| L-S3a～c | 供应商从不回 submit_sm | `BufferTimeout` 3 秒后 `[RESP-TIMEOUT]`；WARN 写明会话和 seq；给 Core 写失败结果 |

## 5. 最近一次结果

| 时间 | 环境 | 结果 |
|---|---|---|
| 2026-11-27 | 测试服务器全量回归（提交 `32c4d33` 的代码） | 9 组共 168 项，全部通过；表里的条数、毫秒数取自这次的实测输出 |

## 6. 没有覆盖到的

| 内容 | 说明 |
|---|---|
| 多个 Client 进程共用一个通道的速率 | 只在一个进程里测；多进程的分配在 ModuleOpsIT（MO-03 只算容量） |
| 入向限速在真实的多个 Server 进程上 | ThrottleSyncIT 用两个计数器模拟 |
| 生产流量下的长时间稳定 | 内存长跑在 MemSoakIT（手动跑，不在 suites.txt） |
