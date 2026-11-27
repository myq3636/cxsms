# DR 业务测试（FakeDrIT、CoreDrBizIT、DrErrorIT）

> 日期：2026-11-27。用例代码：`regression/test/it/FakeDrIT.java`（27 项）、`CoreDrBizIT.java`（41 项）、`DrErrorIT.java`（42 项），都在 `regression/suites.txt`（FakeDrIT 是 it 模式，CoreDrBizIT 是 it-multi，DrErrorIT 是 it-server）。
> 这一份讲**客户收到什么样的 DR**：假 DR、延迟 DR、DR 失败后改道、没有 Server 在线时的 DR、DR 错误码。供应商 DR 怎么匹配回原来的 MT、怎么送到 Server，在《DR 流程测试》里。
> 设计见《假DR逻辑重新设计》《DR错误码设计》。

## 1. 测什么

| 部分 | 套件 | 内容 |
|---|---|---|
| 假 DR 的四种方式 | FakeDrIT、CoreDrBizIT | 1 收到客户消息就回；2 通道接受（submit_sm_resp 成功）就回；3 真实 DR 失败时按比例改成成功；4 通道 N 秒不回 DR 就自动回 |
| 失败不能变成功 | FakeDrIT | 没路由成功、通道拒绝、重试用完，都回失败 DR，不回假的成功 |
| 每条 MT 只回一条最终 DR | FakeDrIT、CoreDrBizIT | 回过假 DR 以后，通道的真实 DR 只记 CDR，不再发给客户 |
| 假 DR 的成功比例 | CoreDrBizIT | `SMSOptionFakeDRSuccessRatio` 0、70、不配（100） |
| 去掉的旧配置 | FakeDrIT、CoreDrBizIT | `SMSOptionSendFakeDR`、`SMSOptionSendDRBeforeOutSubmitInDC` 不再生效 |
| 延迟 DR | CoreDrBizIT | `SMSOptionDRDelayTimeMinMaxInSec`；Core 停了以后到期的 DR 留在 Redis，恢复后发出 |
| DR 失败后改道 | CoreDrBizIT | `NeedDRReroute`、`SMSOptionDRRerouteCode`、`RerouteDRDelayTimeInSec` |
| 没有 Server 在线 | CoreDrBizIT | DR 留在 RDQ，Server 上线后 MQM 重发 |
| 重复处理 | CoreDrBizIT | 一条 MT、提交结果、供应商 DR 在第一个处理者确认前被 auto-claim 读走：只处理一次 |
| DR 错误码 | DrErrorIT | 通道的错误码映射成内部码（`conf/drErrorMapping.cfg`）；内部码到客户收到的回执（`err:`、`network_error_code` TLV）；数据库列；重新加载 |

## 2. 怎么跑、怎么判定

```
regression/run.sh FakeDrIT
regression/run.sh CoreDrBizIT
regression/run.sh DrErrorIT
cat regression/.work/results/{FakeDrIT,CoreDrBizIT,DrErrorIT}.txt
```

- 环境：测试服务器上的回归环境（JDK 17，本地 Redis 16379）。进程内启动真实的 Core：`MTStreamConsumer`（路由）、`ResultStreamConsumer`（提交结果）、`InboundDRStreamConsumer`（供应商 DR）、`CoreMQMStreamConsumer`（MQM 重发）、`DelayedInDRDispatcher`（延迟 DR）、`FakeDrTimeoutScheduler`（方式 4 的计时）。
- **不连数据库**：消息存储换成记录 SQL 的 `RecordingStore`（同《发送失败重试测试》），用例从记下的 SQL 里看写没写 WDQ、RDQ。
- 本程序扮演 Client：

| 动作 | 做法 |
|---|---|
| 客户发 MT | 写进 `stream:mt:pending:{ossid}:*`，发送方 `18667991226`，接收号码 `8613800000000`（另有香港号码 `85212345678`），等它出现在通道的出向流 |
| 通道接受 | submit_resp 成功，带供应商消息 ID `OUT-n` |
| 通道拒绝 | submit_resp 失败（UNDELIVERABLE，或 2500 `COMMUNICATION_ERROR`） |
| 供应商 DR | 等 DR 查找记录 `drl:{OUT-n}` 写好后，写一条供应商 DR（DELIVERED、UNDELIVERABLE、REJECTED、EXPIRED） |
| 看客户 DR | `stream:dr:pending:{ossid}:*`，数条数、看状态码 |

- DrErrorIT 有一部分在进程内启动**真实的 MultiSmppServer**（端口 17807），用 SMPP 测试客户端绑定，看客户收到的 deliver_sm 回执里的 `stat:`、`err:` 和 TLV。

## 3. 公共配置

### 3.1 GmmsConfig 追加

| 套件 | 配置 |
|---|---|
| 三组都有 | `AsyncSqlWriterEnable=false`（SQL 同步执行，便于检查） |
| FakeDrIT | `AutoInDR.DispatcherIdleSleepMs=100`、`FakeDrTimeout.IdleSleepMs=100`（延迟 DR、方式 4 的计时轮询快一些） |
| CoreDrBizIT | `AutoInDR.DirectStreamEnable=true`、`AutoInDR.DispatcherIdleSleepMs=100` |

`RerouteDRDelayTimeInSec` 用默认的 1800 秒。

### 3.2 FakeDrIT 的 cm.cfg 和路由

每个块的公共部分是 `CCBSSID`、`A2PPlayerType: operator`、`Protocol: SMPP`。

| 块 | CCBSSID | 额外配置 | 路由 |
|---|---|---|---|
| V951（通道） | 951 | — | |
| V952（通道） | 952 | `SMSOptionSendDRByOutSubmit: true`（方式 2） | |
| V953（通道） | 953 | `SMSOptionSubmitRetryCode: 2500`<br>`SMSOptionMTRetryPolicy: 1S` | |
| C951 | 1001 | `SMSOptionSendDRByInSubmitInCU: true`（方式 1） | 86;852 → V951 |
| C952 | 1002 | `SMSOptionSendDRByInSubmitInCU: true`<br>`SMSOptionFakeDRSuccessRatio: 0` | 86;852 → V951 |
| C953 | 1003 | `SMSOptionDRSuccessRatio: 100`<br>`SMSOptionDRSuccessRatioPrefix: 8613`（方式 3） | 86;852 → V951 |
| C955 | 1005 | `SMSOptionFakeDRTimeoutSec: 3`（方式 4） | 86;852 → V951 |
| C956 | 1006 | `SMSOptionFakeDRTimeoutSec: 3`<br>`SMSOptionFakeDRTimeoutStatus: fail` | 86;852 → V951 |
| C957 | 1007 | `SMSOptionFakeDRDisable: true` | 86 → V952 |
| C958 | 1008 | — | 86 → V952 |
| C959 | 1009 | `SMSOptionSendFakeDRByInSubmitRecPrefixInCU: 8613`（方式 1，按前缀） | 86;852 → V953 |
| C960 | 1010 | `SMSOptionSendFakeDR: true`（旧配置） | 86;852 → V951 |

路由都是 `A2PMainRouting: (Priority=30,PhonePrefix=前缀,Relay=通道)`。

### 3.3 CoreDrBizIT 的 cm.cfg 和路由

| 块 | CCBSSID | 额外配置 | 路由 |
|---|---|---|---|
| V801（通道） | 801 | — | |
| V802（通道） | 802 | `SMSOptionSendDRBeforeOutSubmitInDC: true`（旧配置） | |
| V803（通道） | 803 | `NeedDRReroute: true`<br>`SMSOptionDRRerouteCode: 10400`（UNDELIVERABLE） | |
| V804～V806（通道） | 804～806 | — | |
| V807（通道） | 807 | `SMSOptionSendDRByOutSubmit: true` | |
| V808（通道） | 808 | `SMSOptionSendDRByOutSubmit: true`<br>`SMSOptionDRDelayTimeMinMaxInSec: 3,4` | |
| R810 | 810 | — | 86;852 → V807 |
| R811 | 811 | `SMSOptionFakeDRSuccessRatio: 0` | 86;852 → V807 |
| R812 | 812 | `SMSOptionFakeDRSuccessRatio: 70` | 86;852 → V807 |
| R813 | 813 | `SMSOptionSendFakeDR: true`（旧配置） | 86 → V802 |
| R814 | 814 | `SMSOptionSendDRByInSubmitInCU: true` | 86;852 → V801 |
| R815 | 815 | `SMSOptionSendFakeDRByInSubmitRecPrefixInCU: 8613` | 86;852 → V801 |
| R816 | 816 | — | 86 → V808 |
| R817 | 817 | — | 86;852 → V801 |
| R818 | 818 | — | 主 86 → V803，备份 86 → V804 |
| R819 | 819 | — | 86 → `V805;V806` |

R818 的路由：

```
CCBSSID: 818
A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V803)
A2PBackupRouting: (Priority=30,PhonePrefix=86,Relay=V804)
```

### 3.4 DrErrorIT 的配置

cm.cfg 追加 6 个客户，都是：

```
[DECUSTA]
CCBSSID: 61
A2PPlayerType: operator
Protocol: SMPP
ConnectionType: MC
WindowSize: 20
IncomingConnection: (ConnName=DECUSTA_c1,userName=decusta,password=p,URL=127.0.0.1,SessionNumber=20,MinReceiverNumber=1,MaxReceiverNumber=5,BindMode=TRX,Version=V34)
```

DECUSTB～DECUSTF 的 CCBSSID 是 62～66，用户名是块名的小写。另有一个通道：

```
[DENOPAY]
CCBSSID: 69
A2PPlayerType: operator
Protocol: SMPP
SMSOptionDRNotPaidCode: 077,030001
```

`conf/drErrorMapping.cfg`：

```
[DEFAULT]
DRErr: (Stat=UNDELIV,Err=1,Code=101)
[VENDOR8]
DRErr: (Stat=UNDELIV,Err=002~009,Code=104);(Err=088,Code=404);(Stat=REJECTD,Code=202)
DRNetworkErr: (Type=3,Value=001B,Code=103)
SubmitErr: (Status=0x0B,Code=102);(Status=*,Code=601)
[DENOPAY]
DRErr: (Err=077,Code=201);(Stat=UNDELIV,Err=5,Code=104)
DRNetworkErr: (Type=3,Value=0001,Code=103)
[DECUSTB]
DRErrDefault: passthrough
[DECUSTC]
CustomerErr: (Code=104,Err=013);(Code=100~199,Err=003)
[DECUSTD]
DRErrDefault: vendor
[DECUSTE]
DRErrDefault: 000
[DECUSTF]
DRErrDefault: passthrough
DRErrTLV: true
```

文件格式：

| 写法 | 意思 |
|---|---|
| 通道块的 `DRErr` | 供应商 DR 的 `stat`、`err` 映射到内部码；`Err` 可以是范围 `002~009`，数字比较（`5` 和 `005` 一样）；不写 `Stat` 就是任何状态 |
| `DRNetworkErr` | `network_error_code` TLV（类型、值）映射到内部码，比 `DRErr` 先看 |
| `SubmitErr` | submit_sm_resp 的 command_status 映射到内部码，`*` 是其他所有 |
| `[DEFAULT]` | 通道块里没有匹配的规则时用 |
| 客户块的 `DRErrDefault` | 回执里 `err:` 填什么：`passthrough` 填内部码；`vendor` 填通道原来的 err（3 位）；`000` 等固定值；不配就用以前的表（按状态码） |
| `CustomerErr` | 内部码（或范围）映射到给这个客户的 err |
| `DRErrTLV: true` | 回执再带一个 `network_error_code` TLV（类型 3，值是内部码） |

## 4. 用例

### 4.1 失败不能变成功（FakeDrIT）

| ID | 目的 | 客户 → 通道 | 步骤 | 预期 |
|---|---|---|---|---|
| FN-01a | 方式 1 的客户，消息没路由成功 | C951 | 发到 `4412345678`（没有路由） | 失败 DR **10402**，不是假的成功 |
| FN-02a | 方式 1，所有号码，比例不配 | C951 → V951 | 路由成功 | 马上回 DELIVERED（10000） |
| FN-02b | 之后通道拒绝 | 同上 | 通道回 UNDELIVERABLE | 不再回第二条 DR（客户只有 1 条） |
| FN-04a | 方式 1，`SMSOptionFakeDRSuccessRatio: 0` | C952 → V951 | 路由成功 | 假 DR 是失败的：**10400**（UNDELIVERABLE） |
| FN-03a | 方式 1 按前缀 8613，号码在前缀外 | C959 → V953 | 发香港号码 | 路由时不回 DR |
| FN-03b | 前缀外，重试用完（策略 1 个点）失败 | 同上 | 通道失败 2500 | 客户收到失败 DR **10405** |
| FN-03c | 号码在 8613 里 | C959 → V953 | 发 `8613…` | 路由时回 DR |
| FN-03d | 之后重试用完失败 | 同上 | 通道失败 2500 | 还是只有 1 条 DR |

### 4.2 方式 2：通道接受就回（FakeDrIT、CoreDrBizIT）

| ID | 目的 | 客户 → 通道 | 步骤 | 预期 |
|---|---|---|---|---|
| FN-05a | 客户 `SMSOptionFakeDRDisable` | C957 → V952 | 通道接受 | 不回假 DR |
| FN-05b | — | 同上 | 供应商 DR DELIVERED | 转发给客户 |
| FN-05c | 通道 `SMSOptionSendDRByOutSubmit` | C958 → V952 | 通道接受 | 回假 DR DELIVERED |
| FN-06 | 之后供应商 DR 是 EXPIRED | 同上 | — | 不回第二条，也不留在 WDQ |
| DR-07a | 比例不配（100） | R810 → V807 | 通道接受 | DELIVERED |
| DR-07b | 之后供应商 DR 失败 | 同上 | 供应商 DR UNDELIVERABLE | 客户还是只有 1 条 DR |
| DR-07c | 通道拒绝 MT | R810 → V807 | submit_resp UNDELIVERABLE | 失败 DR **10400**，不回假的成功 |
| DR-07d | `SMSOptionFakeDRSuccessRatio: 0` | R811 → V807 | 通道接受 | 假 DR 失败（10400） |
| DR-12 | `SMSOptionFakeDRSuccessRatio: 70` | R812 → V807 | 100 条，每条通道接受 | 每条都有 DR；DELIVERED 在 50～90 条之间（2026-11-27 实测 80 条） |

### 4.3 方式 1：收到客户消息就回（CoreDrBizIT）

| ID | 目的 | 客户 → 通道 | 预期 |
|---|---|---|---|
| DR-09a | `SMSOptionSendDRByInSubmitInCU` | R814 → V801 | 路由成功就回 DELIVERED |
| DR-09b | 之后通道接受、供应商 DR 失败 | 同上 | 客户只有 1 条 DR |
| DR-09c | 按前缀 8613 | R815 → V801，`8613…` | 路由时回 DR |
| DR-09d | 前缀外 | R815 → V801，香港号码 | 不回 |

### 4.4 方式 3：真实 DR 失败时按比例改成成功（FakeDrIT，C953：比例 100、前缀 8613）

| ID | 目的 | 号码 | 步骤 | 预期 |
|---|---|---|---|---|
| FN-07a | 前缀内，供应商 UNDELIVERABLE | `8613…` | 通道接受、供应商 DR UNDELIVERABLE | 客户收到 DELIVERED |
| FN-07b | CDR 记真实状态 | — | — | `OUT_DELI_REP` 是 10400 |
| FN-07c | 供应商 REJECTED | `8613…` | — | 也改成 DELIVERED（除了 DELIVERED 以外的状态都改） |
| FN-07d | 前缀外 | 香港号码 | 供应商 DR UNDELIVERABLE | 原样：10400 |
| FN-07e | 通道拒绝 MT（不是 DR） | `8613…` | submit_resp UNDELIVERABLE | 失败 DR 10400（方式 3 只改供应商 DR） |

### 4.5 方式 4：通道不回 DR 就自动回（FakeDrIT）

| ID | 目的 | 客户 | 步骤 | 预期 |
|---|---|---|---|---|
| FN-08a | 3 秒内供应商 DR 先到 | C955 | 通道接受、马上回 UNDELIVERABLE | 转发：10400 |
| FN-08b | 过了 3 秒 | 同上 | 等 4.5 秒 | 不再回第二条 |
| FN-09a | 没有供应商 DR | C955 | 通道接受 | 3 秒后回 DELIVERED（实测 3215 毫秒） |
| FN-09b | 之后供应商 DR 来了 | 同上 | 供应商 DR UNDELIVERABLE | 只记 `OUT_DELI_REP`，客户还是 1 条 |
| FN-09c | `SMSOptionFakeDRTimeoutStatus: fail` | C956 | 通道接受，不回 DR | 3 秒后回失败 10400 |
| FN-09d | 计时收尾 | — | — | `zset:fakedr:timeout:*` 都空了 |
| FN-09e | 有客户配了超时 | — | — | 计时轮询运行 |
| FN-09f | 没有客户配超时 | — | — | 不轮询（每分钟查一次配置）；读不到客户时照常轮询 |

### 4.6 去掉的旧配置（FakeDrIT、CoreDrBizIT）

| ID | 目的 | 客户 → 通道 | 预期 |
|---|---|---|---|
| FN-14a | 客户 `SMSOptionSendFakeDR: true` | C960 → V951，通道接受 | 不回假 DR |
| FN-14b | — | — | 加载时打 WARN：`SMSOptionSendFakeDR is no longer used` |
| DR-08 | 通道 `SMSOptionSendDRBeforeOutSubmitInDC: true` | R813 → V802 | 通道回应前不回 DR |
| DR-08b | 客户 `SMSOptionSendFakeDR: true` | 同上，通道接受 | 不回假 DR |
| DR-08c | — | 同上，供应商 DR DELIVERED | 转发给客户，只有 1 条 |

### 4.7 延迟 DR（CoreDrBizIT，R816 → V808：方式 2，延迟 3～4 秒）

| ID | 目的 | 步骤 | 预期 |
|---|---|---|---|
| DR-10a | 还没到时间 | 通道接受，等 1.5 秒 | 没有 DR |
| DR-10b | 到时间 | — | 收到 DR |
| DR-10c | 时间在范围里 | — | 2.5～7 秒（包括通道回应的时间；实测 3205 毫秒） |
| DR-11a | Core 停了（停掉延迟 DR 的分发线程） | 通道接受，等 6 秒 | 到期的 DR 留在 Redis，不发 |
| DR-11b | Core 恢复 | 重新启动分发线程 | DR 发出 |
| DR-11c | 一次到期 5 条，其中 1 条被一个停掉的 Core 领取了 | 停掉分发，接受 5 条，等 6 秒；把一条从 `zset:delay:outdr:*` 挪到 `zset:delay:outdr:processing:*`（领取时间已过） | 准备好 |
| DR-11d | 恢复后 | 重新启动分发 | 5 条各发 1 次（过期的领取被放回去），`delay:outdr:payload:*` 都删掉 |

### 4.8 DR 失败后改道（CoreDrBizIT，R818 → V803，备份 V804）

| ID | 目的 | 步骤 | 预期 |
|---|---|---|---|
| RE-11a | DR 状态在改道码里，在改道时间窗内 | 消息时间设为 8 小时前（按东八区算法，正好是“刚发”），供应商 DR UNDELIVERABLE | 改发备份通道 V804，不回 DR |
| RE-11b | 其他失败码 | 供应商 DR REJECTED | 回客户 DR，不改道 |
| RE-11c | 超过 `RerouteDRDelayTimeInSec`（1800 秒） | 消息时间设为 9 小时前，供应商 DR UNDELIVERABLE | 回客户 DR，不改道 |

`RerouteDRDelayTimeInSec` 的计算固定按东八区减 8 小时（见《配置项说明》），所以用例把消息时间往前推 8 小时来表示“刚发出”。

### 4.9 `Relay=a;b`（CoreDrBizIT，R819）

| ID | 目的 | 步骤 | 预期 |
|---|---|---|---|
| RT-05a | `Relay=V805;V806` | 发 MT | 发到 V805 |
| RT-05b | `;` 后面不是故障转移 | V805 失败 2500 | 回失败 DR，V806 上没有 |

### 4.10 没有 Server 在线（CoreDrBizIT，R817）

| ID | 目的 | 步骤 | 预期 |
|---|---|---|---|
| DR-06a | 客户的 DR，没有 Server 在线 | MQM 写一条 DELIVERED DR | 写入成功 |
| DR-06b | 没地方发 | — | 写进 RDQ |
| DR-06c | — | — | 客户的 DR 流里没有 |
| DR-06d | Server 上线后 MQM 重发 | 登记在线的 Server（`system:server:nodes`、`module:status:server:MultiSmppServer1:1`），MQM 从 RDQ 重发 | 写入成功 |
| DR-06e | — | — | DR 到了客户的 DR 流 |

### 4.11 重复处理（CoreDrBizIT，R817 → V801）

auto-claim 在第一个处理者确认之前读走了同一条：

| ID | 对象 | 情况 | 预期 |
|---|---|---|---|
| FT-08a～d | MT | 第一个处理者先处理完，auto-claim 的副本后处理 | 只发出 1 次、1 条 IN_SUBMIT，没有残留的 `core:mt:*` 标记，日志 `MT already acked by another consumer` |
| FT-08k/l | MT | auto-claim 先处理完，第一个处理者的旧副本后处理 | 只发出 1 次，没有残留标记 |
| FT-08e/f | MT | 第一个处理者死了，条目还在流里 | auto-claim 处理并确认 |
| FT-08g/h | 提交结果 | 同 FT-08a | 1 条 OUT_SUBMIT，日志 `Result already acked by another consumer` |
| FT-08i/j | 供应商 DR | 同 FT-08a | 客户 1 条 DR，1 条 OUT_DELI_REP |

### 4.12 DR 错误码（DrErrorIT）

**数据库**（`RecordingStore` 记下的 SQL）：

| ID | 目的 | 预期 |
|---|---|---|
| DE-14a | `DB.DrErrorCodeColumns=true` | 写 RDQ 的 INSERT 带 `VendorErrorCode='stat=UNDELIV;err=5'` 和 `DrErrorCode='104'` |
| DE-14b | `false` | SQL 里没有这两列 |
| DE-14c | `auto`，连不上数据库 | 不用这两列 |
| DE-15a | RDQ 重发时恢复状态：状态文字 `UNDELIVERY_NETWORK_ERROR`，码 104 | 10405（只看文字不是 DR 状态，DEF-C5） |
| DE-15b | 文字 UNDELIV，码 104（同类） | 10405 |
| DE-15c | 码 000（另一类） | 按文字：10400 |
| DE-15d | 没有码 | 和以前一样按文字：10400 |

**通道的码 → 内部码**（VENDOR8 的供应商 DR）：

| ID | 供应商 DR | 预期内部码 / 状态 |
|---|---|---|
| DE-01a | `stat=UNDELIV;err=5`（在 002~009 里） | 104，10405 |
| DE-01b | `err=005` | 104（005 就是 5） |
| DE-02 | `err=5;nec=3:001b` | 103（`network_error_code` 先看） |
| DE-03a | `err=77`（没有规则） | 状态不变 10400，用状态自己的码 200 |
| DE-03b | `err=1`（通道块没有） | `[DEFAULT]`：101 |
| DE-03c | — | 没映射到的码打日志 `[DR-ERR-UNMAPPED]` |
| DE-04 | `err=088`（任何状态） | 404：欠费，状态 10800（`REJECTED_BYNC`） |
| DE-04b | `stat=DELIVRD;err=088` | 还是 000、10000 |
| DE-04c | `stat=REJECTD;err=9` | 202 |
| DE-04d | 通道什么都没给，状态 EXPIRED | 状态自己的码 501，10200 |
| DE-08a | submit_sm_resp 0x0B | 102 |
| DE-08b | 其他 command_status（0x58） | 601 |
| DE-04e | DENOPAY `err=077`（在 `SMSOptionDRNotPaidCode` 里） | 先于 `DRErr` 规则：10800，404 |
| DE-04f | DENOPAY `nec=3:0001`（030001 在列表里） | 先于 `DRNetworkErr`：10800，404 |
| DE-04g | DENOPAY `err=5`（不在列表里） | 按映射规则：10405，104 |
| DE-10a/b | 经过 Redis 流的序列化 | `drErrorCode`、`vendorErrorCode` 都保留 |

**客户收到的回执**（真实 Server，DR 状态 10405、内部码 104、通道原始 `stat=UNDELIV;err=5`）：

| ID | 客户 | 配置 | 预期回执 |
|---|---|---|---|
| DE-05a | decusta | 没有客户块 | `err:016`（以前的表：10405 → 016），`stat:UNDELIV` |
| DE-05b | decustb | `passthrough` | `err:104` |
| DE-05c | decustc | `CustomerErr` 104 → 013 | `err:013` |
| DE-05d | decustd | `vendor` | `err:005`（通道的 err，3 位） |
| DE-05e | decuste | `000` | `err:000` |
| DE-05f | decustc | 码 101，在 100~199 里 | `err:003` |
| DE-05g | decustc | DELIVERED，码 000 | `err:000`，`stat:DELIVRD` |
| DE-06 | decustf | `passthrough` + `DRErrTLV` | `err:104`，`network_error_code` TLV 类型 3、值 104 |
| DE-09 | decusta | 没有码的 DR（假 DR、系统 DR），UNDELIVERABLE | 按表：`err:003` |
| DE-06b | decusta | 没配 `DRErrTLV` | 没有 TLV |

每个客户先检查能绑定上（DE-05a～e 各多一项 “bound”）。

**重新加载**：

| ID | 目的 | 预期 |
|---|---|---|
| DE-12a | 改成 `[VENDOR8] DRErr: (Stat=UNDELIV,Err=5,Code=201)` 后重新加载 | 成功 |
| DE-12b | — | 新规则生效：201 |
| DE-12c | 改成码 777（内部码表 `conf/drErrorCode.cfg` 里没有） | 拒绝加载，继续用 201 |

## 5. 最近一次结果

| 时间 | 环境 | 结果 |
|---|---|---|
| 2026-11-27 | 测试服务器全量回归（提交 `32c4d33` 的代码） | FakeDrIT 27 项、CoreDrBizIT 41 项、DrErrorIT 42 项，全部通过；表里的状态码取自这次的实测输出 |

## 6. 没有覆盖到的

| 内容 | 说明 |
|---|---|
| 方式 2 的 `SMSOptionRecPrefixForFakeDR`（通道按前缀） | 没有用例 |
| `SMSOptionDRSuccessBiasValue`（比例的随机浮动） | 没有用例 |
| 方式 4 选 `ratio` | 只测了 `success`、`fail` |
| 方式 4 遇到 DR 失败改道后重新计时 | 没有用例 |
| 方式 1 的延迟（客户的 `SMSOptionDRDelayTimeMinMaxInSec`） | 只测了通道上的延迟（方式 2） |
| 改道时间窗在非东八区服务器上 | 计算固定按东八区，已知问题 |
| DR 错误码的数据库列在真实 MySQL 上（`auto` 检测到列） | 这里不连数据库 |
