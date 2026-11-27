# Client SMPP 连接与 PDU 测试（ClientConnectionIT、ClientPduIT、ClientResultIT、ClientTlsIT）

> 日期：2026-11-27。用例代码在 `regression/test/it/`，都在 `regression/suites.txt`，模式 `it-mod:MultiSmppClient1`（进程以 MultiSmppClient1 模块的身份运行）：
>
> | 套件 | 项数 |
> |---|---|
> | ClientConnectionIT | 60 |
> | ClientPduIT | 45 |
> | ClientResultIT | 87 |
> | ClientTlsIT | 2 项通过，2 项已知缺陷 |
>
> 功能清单见 `regression/routetest/features/07_GSM7与8bit长度/Client模块功能与测试用例清单.md`。发送速率、发送过程在《Client发送速率与发送过程测试》，HTTP 通道在《Client HTTP通道测试》，Client 进程故障在《Client故障恢复测试》。

## 1. 测什么

真实的 SMPP Client（`MultiSmppClient` + `OutboundStreamConsumer`）连一个假的供应商 SMSC：

| 部分 | 套件 | 内容 |
|---|---|---|
| 连接 | ClientConnectionIT | 绑定被拒、端口不通、供应商断开、enquire_link 保活、空闲断开、绑定方式（TRX、T、R）、多个会话、窗口 |
| 多连接的发送策略 | ClientPduIT | `SubmitPolicy_Conn`：主备（primary）、负载均衡（loadbalance） |
| 发出的 submit_sm | ClientPduIT | 每个字段：地址和 TON/NPI、service_type、protocol_id、data_coding 和内容编码、UDH 和 SAR、有效期、registered_delivery、可选参数 |
| 供应商的回应 | ClientResultIT | submit_sm_resp 的 command_status 映射成内部状态；消息 ID 的格式 |
| 供应商的 DR | ClientResultIT | 回执文本（`stat:`）、TLV（`message_state`、`network_error_code`、`receipted_message_id`）映射成内部状态；原始错误码保留；地址对调 |
| 写不进 Redis 时 | ClientResultIT | DR、MO 写不进去就回 `ESME_RX_T_APPN`，让供应商重发 |
| MO | ClientResultIT | 不是 DR 的 deliver_sm 写进 MT 流；UDH 去掉 |
| 客户看到的 DR 状态 | ClientResultIT | `SMSOptionDRStatusMapping`、`SMSOptionIsNotSupportUnknowDRStatus`、默认的错误码表 |
| TLS | ClientTlsIT | `SslSmppClient` 连 TLS 的供应商 |

## 2. 怎么跑、怎么判定

```
regression/run.sh ClientConnectionIT     # 其余套件同样
cat regression/.work/results/ClientConnectionIT.txt
```

- 环境：测试服务器上的回归环境（JDK 17，本地 Redis 16379）。
- 进程内启动真实的 Client：`new MultiSmppClient().startService()` 和 `OutboundStreamConsumer`。
- **假 SMSC**（`regression/test/it/FakeSmsc.java`）：本进程里的一个 SMPP 服务端，按 system_id（用户名）分账号，每个账号可以设：拒绝绑定（返回指定的 command_status）、不回 enquire_link、扣住 submit_sm_resp、指定 submit_sm_resp 的状态和消息 ID、断开连接、主动发 deliver_sm（DR、MO）。它记下收到的每个 PDU，用例直接检查字段。
- 本程序扮演 Core：
  - 发 MT：写进通道的出向流 `stream:mt:routed:{通道}:*`；
  - 看结果：读 Client 写给 Core 的结果流 `stream:core:results:*`、供应商 DR 流 `stream:core:inbound-dr:*`、MO 写进的 `stream:mt:pending:*`。
- ClientResultIT 的“客户看到的 DR 状态”（VD-06、07、09）调用 Server 生成回执文本的代码，比对回执里的 `stat:`、`err:`。

## 3. 公共配置

### 3.1 通道块

每个套件用一个函数生成通道块，例如 ClientConnectionIT 的 CV301：

```
[CV301]
CCBSSID: 301
A2PPlayerType: operator
Protocol: SMPP
ConnectionType: MC
IsInit: true
ReconnectInterval: 1
BufferTimeout: 60
EnquireLink: false
WindowSize: 20
OutgoingConnection: (ConnName=CV301_c1,userName=u301,password=p,URL=127.0.0.1,Port=17791,SessionNumber=1,BindMode=TRX,Version=V34,IsInit=true)
```

各套件的不同：

| 套件 | 端口 | 公共部分的不同 |
|---|---|---|
| ClientConnectionIT | 17791（CV302 用 17792） | `EnquireLink`、`WindowSize`、`BindMode`、`SessionNumber` 按用例改 |
| ClientPduIT | 17796 | `BufferTimeout: 30`、`WindowSize: 100` |
| ClientResultIT | 17793 | `WindowSize: 50` |
| ClientTlsIT | 17798（TLS） | — |

下面只列每个块的**额外配置**。

### 3.2 ClientConnectionIT 的通道

| 块 | 额外配置 | 假 SMSC 账号的行为 / 用途 |
|---|---|---|
| CV301 | — | 拒绝绑定（0x0E），之后恢复 |
| CV302 | 端口 17792 | 端口一开始不通 |
| CV303 | `EnquireLink: true`、`EnquireLinkTime: 1` | 供应商断开 |
| CV304、CV305 | `EnquireLink: true`、`EnquireLinkTime: 1`、`EnquireLinkFailureNum: 3` | 回 / 不回 enquire_link |
| CV306 | `BindMode=T` | 发送 |
| CV307 | `BindMode=R` | 只接收 |
| CV308 | `SessionNumber=2` | 两个会话 |
| CV309 | `WindowSize: 2` | 窗口满 |
| CV310 | `ConnectionSilentTime: 2` | 空闲断开 |
| CV311 | — | SMSC 发 enquire_link |
| CV312 | `ConnectionSilentTime: 12` | 断开后重连（没有保活） |
| CV313 | `EnquireLink: true`、`EnquireLinkTime: 8` | 断开后重连（有保活） |
| CV314 | `EnquireLink: true`、`EnquireLinkTime: 2` | 空闲时保活 |
| CV315 | `EnquireLink: true`、`EnquireLinkTime: 2`、`KeepEnquireLink: true` | 一直保活 |
| CV316 | — | 一下子发 3000 条 DR |
| CV317 | `WindowSize: 5` | 同时等回应的 MT |

### 3.3 ClientPduIT 的通道

| 块 | 额外配置 |
|---|---|
| PV701 | —（默认） |
| PV702 | `ServiceType: CMT`、`SMSOptionSourceAddrTon: 2`、`SMSOptionSourceAddrNpi: 3`、`SMSOptionDestAddrTon: 4`、`SMSOptionDestAddrNpi: 5`、`SMSOptionProtocolId: 3F` |
| PV703 | `SMSOptionSupportOutgoingGSM7bit: 1`（不压缩） |
| PV704 | `SMSOptionSupportOutgoingGSM7bit: 2`（压缩） |
| PV705 | `ChlUdhConcatenated: true` |
| PV706 | `SMSOptionTransferValidityPeriod: true` |
| PV707 | `SMSOptionTransferValidityPeriod: true`、`SMSOptionIsRelateValidityPeriod: true` |
| PV708 | `ChlIsSupportDeliveryReport: No` |
| PV709 | `SMSOptionHttpCustomParameter: OTP` |
| PV710 | `SMSOptionOutgoingThrottlingNum: 7` |
| PV711、PV712 | — |
| PV713 | `SubmitPolicy_Conn: (Policy=primary,Primary=PV713_b)`，两个连接 PV713_a、PV713_b |
| PV714 | `SubmitPolicy_Conn: (Policy=loadbalance)`，两个连接 PV714_a、PV714_b |

两个连接的写法：

```
OutgoingConnection: (ConnName=PV713_a,userName=u713a,…);(ConnName=PV713_b,userName=u713b,…)
```

GmmsConfig 追加：`ClientStreamTaskMaxBatches=1`、`ClientStreamTaskMaxMs=2000`、`RedisStreamConsumer.ClientOutboundSubmit.WorkerMaxPoolSize=2`。

### 3.4 ClientResultIT 的通道和客户

| 块 | 额外配置 |
|---|---|
| RV401、RV403 | `SMSOptionSubmitNotPaidMapping: 88:8000`（`SUBMIT_NOTPAID`） |
| RV402、RV407 | — |
| RV404 | `ChlSMPPMsgIDParse: true` |
| RV405 | `ChlSMPPMsgIDParse: true`、`SMSOptionSMPPIsPadZero4SR: false` |
| RV406 | `SMSOptionDRStatusIsOptionPara: true` |
| RV408 | `DrSwapAddr: false` |
| RV409 | `SMSOptionRemoveUdh: true` |
| RC410（客户） | `SMSOptionDRStatusMapping: DELIVRD:ACCEPTD` |
| RC411（客户） | `SMSOptionIsNotSupportUnknowDRStatus: true` |
| RC412、RC413（客户） | — |

### 3.5 ClientTlsIT

用 keytool 生成证书 `conf/it-ks.jks`（CN=localhost，密码 tomcata2p），GmmsConfig 追加：

```
Ssl.KeyStorePath=…/conf/it-ks.jks
Ssl.TrustStorePath=…/conf/it-ks.jks
Ssl.IsClientTrustAll=true
```

通道 TV901 的协议是 SSL SMPP，端口 17798，假 SMSC 用这个证书起 TLS 服务端。

## 4. 用例

### 4.1 连接（ClientConnectionIT）

| ID | 目的 | 预期 / 2026-11-27 实测 |
|---|---|---|
| CN-03a | 绑定被拒（CV301） | 按 `ReconnectInterval`（1 秒）重试（实测 14 次） |
| CN-03b | 没有会话时的 MT | 失败结果 2500，什么都不发 |
| CN-03c/d | 供应商恢复 | 绑定上，MT 发出 |
| CN-03e | 再拒绝 3 秒 | 每 1 秒试一次（3 次），不会一直狂试 |
| CN-04a～c | 端口不通（CV302）→ 打开 | 先回 2500；打开后连上、绑定、发出 |
| CN-05a～c | 发 2 条，供应商不回应 | 还没有结果 |
| CN-05d | 供应商断开 | 两条等回应的 MT 都回失败结果 **2700** |
| CN-05e/f | — | 按 `ReconnectInterval` 重连，之后的 MT 发出 |
| CN-05g/h | — | 等回应的那两条 Client 不会自己重发；每条一个结果 |
| CN-05i～l | 有保活（CV313，`EnquireLinkTime` 8 秒），保活线程刚发完 enquire_link 在睡，供应商断开 | 约 1 秒重连（实测 1001 毫秒），不用等保活睡完 |
| CN-05m/n | 没有保活（CV312，`ConnectionSilentTime` 12 秒），供应商断开 | 约 1 秒重连（1001 毫秒） |
| CN-06/06b | 空闲链路（CV304） | 按 `EnquireLinkTime` 发 enquire_link（实测 17 次），有回应，连接保持 |
| CN-07a～d | 供应商不回 enquire_link（CV305） | 连续 3 次（`EnquireLinkFailureNum`）没回应就断开；重连；会话线程自己断开的，照样等 `ReconnectInterval`（1001 毫秒） |
| CN-08a/b | SMSC 发 enquire_link（CV311） | 回应 |
| CN-20a～d | 空闲时保活（CV314，2 秒） | 空闲时发；发一条 MT（submit_resp 也算流量）；下一次在最后一次流量之后 2 秒（实测 1915 毫秒），不会到两倍 |
| CN-21a/b | `KeepEnquireLink: true`（CV315） | MT 每 500 毫秒一条时也约每 2 秒发一次（7 秒 4 次） |
| CN-22 | 保活线程 | 共用，没有每个会话一个 `ClientSessionTrd_` 线程 |
| CN-23a～e | CV316 一下子发 3000 条 DR，同时 CV317（窗口 5）发 20 条 MT | 20 条 MT 期间都有回应（382 毫秒）；3000 条 deliver_sm 都回应 ROK，3000 条 DR 都到 Core；队列满了也不拒绝（`ClientInboundPerSsidQueueSize=50`，接收线程自己处理多出来的）；共用的接收线程池，没有每个通道一个 `CustReceiver_` |
| CN-09a/b | `ConnectionSilentTime: 2`，没有流量（CV310） | 断开，然后重连 |
| CN-02a/b | `BindMode=T`（CV306） | 拆成一个发送连接和一个接收连接：bind_transmitter 和 bind_receiver |
| CN-02c | `BindMode=R`（CV307） | 只有 bind_receiver |
| CN-02d/e | — | T 的会话能发；R 的会话不发，回失败结果 2500 |
| CN-10a～c | `SessionNumber=2`（CV308） | 两次绑定；20 条都发出，两个会话各 10 条 |
| TW-04a～h | `WindowSize: 2`（CV309），供应商先不回应 | 只发出 2 条；等着的不回失败结果；回应来了其余的发出；6 条每条只发一次，都有成功结果；流空了，没有残留标记 |

### 4.2 多连接的发送策略（ClientPduIT）

| ID | 目的 | 预期 / 实测 |
|---|---|---|
| CN-11a | `Policy=primary,Primary=PV713_b` | 10 条全走 b（a 0 条） |
| CN-11b | 主连接断开 | 另一个连接接替（a 5 条） |
| CN-11c/d | 主连接恢复 | 绑定上，又用它 |
| CN-11e | `Policy=loadbalance`（PV714） | 两个都用（13 / 7） |

### 4.3 发出的 submit_sm（ClientPduIT）

| ID | 通道 | 消息 | 预期（假 SMSC 收到的） |
|---|---|---|---|
| PD-00 | 全部 | — | 16 个账号都绑定上 |
| PD-01a/b | PV701 | 发送方 `18667991226`，接收 `8613800000000` | source、dest 原样；short_message 是内容 |
| PD-06a | PV701 | — | TON/NPI 默认 1/1 |
| PD-01c | PV701 | — | 默认没有 service_type，protocol_id 0，esm_class 0，没有有效期 |
| PD-01d | PV702 | — | service_type `CMT` |
| PD-06b | PV702 | — | source 2/3，dest 4/5 |
| PD-06c | PV702 | — | protocol_id 0x3F（63） |
| PD-06d | PV701 | 字母发送方 | TON 5，NPI 0 |
| PD-02a | PV701 | UCS2 `你好` | data_coding 8，UTF-16BE 字节 `4F60597D` |
| PD-02b | PV703 | UCS2 | data_coding 8 |
| PD-02c | PV701 | ASCII | data_coding 0 |
| PD-02d | PV703（7-bit 不压缩） | `hello` | 不压缩的 GSM：`68656C6C6F` |
| PD-02e | PV704（7-bit 压缩） | `hello` | 压缩：`E8329BFD06` |
| PD-02f | PV701 | ISO-8859-1 `Café` | data_coding 3，Latin-1 `436166E9` |
| PD-02g | PV701 | 二进制 | data_coding 4，`0102FF` |
| PD-02h | PV701（不支持 7-bit） | 带 7-bit 标志的 `Café` | 按 Latin-1 发，data_coding 3 |
| PD-03a/b | PV705（`ChlUdhConcatenated`） | 长短信的一段 | esm_class 0x40，UDH `050003090201` 在内容前面；没有 SAR 参数 |
| PD-03c | PV701 | 同上 | 默认用 SAR 参数：参考号 0009、总数 02、序号 01 |
| PD-04a | PV706 | 有效期 | 绝对时间：`261127091020000+` |
| PD-04b | PV707 | 有效期 | 相对时间：`000000085959000R` |
| PD-04c | PV706 | 2 小时后过期 | 发出的时刻（标成 GMT）正好是 2 小时后（误差 0 秒） |
| PD-04d | PV707 | 2 小时后过期 | `02:00:00`（以前在东八区少 8 小时） |
| PD-04e | PV707 | 36 小时 | 1 天 12 小时（以前丢了天数） |
| PD-04f | PV707 | 已经过期 | 1 分钟，不是负数 |
| PD-04g | PV707 | 没有过期时间 | 默认 `MessageExpireTime`（1380 分钟，`000000230000000R`） |
| PD-05a | PV701 | — | registered_delivery 1 |
| PD-05b | PV708（`ChlIsSupportDeliveryReport: No`） | — | registered_delivery 0 |
| PD-07 | PV709（`SMSOptionHttpCustomParameter: OTP`） | — | 放在一个可选参数里（tag 5140） |
| PD-09/09b | PV711 | 构造不出 PDU 的消息 | 不发，失败结果 2500；通道照常 |
| ID-08a/b | PV711 | 同一个消息 ID：Core 的第二次尝试、另一部分 | 都发出（3 条）；标记删掉 |

### 4.4 发送过程的细节（ClientPduIT）

| ID | 目的 | 预期 |
|---|---|---|
| TW-06 | 限速通道（PV710，7 TPS） | 一次读 7 条 |
| TW-06b | 不限速 | 正常的一批 |
| TW-05 | `ClientOutboundWindowFullRetryMs` 设为 1、999999、bad | 限制在 10～60000：10、60000；无效时 100 |
| CS-06a～c | `ClientStreamTaskMaxBatches=1`，PV712 积压 3000 条 | 期间 PV711 的小通道照样有发出；积压也发完 3000 条；任务在批之间重新排队，没有发两次的 |

### 4.5 供应商的 submit_sm_resp（ClientResultIT）

**RS-02**：command_status 映射成内部状态（写给 Core 的结果）：

| command_status | 内部状态 |
|---|---|
| 0x00 | 0（成功） |
| 0x03 | 2010 |
| 0x01 | 2100 |
| 0x45 | 2300 |
| 0x0A | 2110 |
| 0x0B | 2120 |
| 0x14、0x58 | 2600 |
| 0x11、0xFE | 2500 |
| 0x08 | 3000 |
| 0x66 | 2200 |
| 0x0E | 5000 |
| 0x400 | 9000 |

| ID | 目的 | 预期 |
|---|---|---|
| RS-00 | — | 都绑定上 |
| RS-03a | `SMSOptionSubmitNotPaidMapping: 88:8000`，供应商回 0x58（88） | 8000 |
| RS-03b | 不在映射里的码 | 正常的状态 |
| RS-04a | 默认 | 消息 ID 原样：`1A2B` |
| RS-04b | `ChlSMPPMsgIDParse`（RV404） | 十六进制转十进制并补零：`0000006699`（`SMSOptionSMPPIsPadZero4SR` 默认跟着它） |
| RS-04c | `SMSOptionSMPPIsPadZero4SR: false`（RV405） | 不补零：`6699` |
| RS-04d | 之后的 DR | 带同样的 ID，Core 能匹配上 |

### 4.6 供应商的 DR（ClientResultIT）

每条 deliver_sm 都回 `deliver_sm_resp ROK`（输出里的 “deliver_sm_resp ROK, seq …” 行）。

| ID | 目的 | 预期 |
|---|---|---|
| VD-05a | 回执文本的 `stat:` | DELIVRD 10000、UNDELIV 10400、EXPIRED 10200、DELETED 10300、REJECTD 10500、UNKNOWN 10900、ACCEPTD 10001 |
| VD-05b | 回执里没有 `sub:` | 取 `id:` 后面的 10 个字符 |
| VD-09a | `DrSwapAddr` 默认 true | source、dest 对调（DR 的 from 是原 MT 的发送方） |
| VD-09b | `DrSwapAddr: false`（RV408） | 按供应商发的 |
| VD-04a | `SMSOptionDRStatusIsOptionPara`（RV406），TLV `message_state` | 2、6 → 10000；3 → 10200；4 → 10300；5 → 10400；8 → 10500；9 → 10900 |
| VD-04b | 只有 `network_error_code` | UNDELIVERABLE |
| VD-04c | 只有 `receipted_message_id` | DELIVERED |
| VD-04d | 没配这个选项，内容为空 | 也从 TLV 读：10400 |
| VD-08a | 原始错误码保留 | `stat=UNDELIV;err=088` |
| VD-08c | `network_error_code` 保留 | `stat=UNDELIV;err=001;nec=3:001B` |
| VD-03a～c | 回执里没有 ID | 会话不断；回应 ROK；写进去但没有 ID，Core 匹配不上 |
| VD-02a～d | DR 写不进 Redis（打开 `RedisStreamBackPressureEnable`，`RedisStreamMonitor.BlockXLEN=1`，DR 流已有条目） | 回 **0x64**（`ESME_RX_T_APPN`），什么都不写，让供应商重发；恢复后下一次接受 |
| VD-10a/b | MO（不是 DR 的 deliver_sm） | 回 ROK，写进 `stream:mt:pending`，ossid 是通道 |
| VD-10c | MO 写不进去 | 0x64 |
| VD-11a～c | 带 UDH 的 MO | 接受；`SMSOptionRemoveUdh`（RV409）去掉 UDH；没有这个选项的通道保留（7 字节） |

### 4.7 客户看到的 DR 状态（ClientResultIT）

| ID | 客户 | 预期回执 |
|---|---|---|
| VD-06a | RC410，`SMSOptionDRStatusMapping: DELIVRD:ACCEPTD` | DELIVRD 显示成 `stat:ACCEPTD err:000` |
| VD-06b | 同上，不在映射里的状态 | 不变 |
| VD-07a | RC411，`SMSOptionIsNotSupportUnknowDRStatus` | UNKNOWN 显示成 `stat:UNDELIV err:003` |
| VD-07b | 同上 | DELETED 也显示成 UNDELIV |
| VD-07c | 没有这个选项 | `stat:UNKNOWN err:004` |
| VD-09d | 没有 DR 错误码映射 | 默认表：10400 → `err:003` |
| VD-09e | — | 10200 → `err:001` |
| VD-09f | 以前没有 `SMSOptionSmppMapErrCod4DR` 的客户（以前是 `err:000`） | 也用默认表：`err:003` |

### 4.8 TLS（ClientTlsIT）

| ID | 目的 | 结果 |
|---|---|---|
| CN-14a | TLS 通道握手、绑定 | 通过 |
| CN-14b | MT 经 TLS 到达供应商 | **已知缺陷 DEF-C7**：`OutboundStreamConsumer` 只从 `MultiSmppClientFactory` 取会话，SSL SMPP 通道没有对应的发送策略，MT 发不出去 |
| CN-14c | 成功结果 | **已知缺陷 DEF-C7**：回了失败结果 2500 |
| CN-14d | 连接断开 | 用 TLS 重连 |

已知缺陷记在 `regression/known-defects.txt`，回归判定时算 known，不算失败。

## 5. 最近一次结果

| 时间 | 环境 | 结果 |
|---|---|---|
| 2026-11-27 | 测试服务器全量回归（提交 `32c4d33` 的代码） | ClientConnectionIT 60、ClientPduIT 45、ClientResultIT 87 项全部通过；ClientTlsIT 2 项通过、2 项已知缺陷（DEF-C7） |

## 6. 没有覆盖到的

| 内容 | 说明 |
|---|---|
| SSL SMPP 通道发 MT | DEF-C7 没修 |
| SMPP 3.3、5.0 | 只测了 V34 |
| 真实供应商的 SMSC | 只有假 SMSC |
| `data_coding` 其他值（如 0xF0 闪信） | 没有用例 |
| 供应商发 `unbind`、`generic_nack` | 没有用例 |
