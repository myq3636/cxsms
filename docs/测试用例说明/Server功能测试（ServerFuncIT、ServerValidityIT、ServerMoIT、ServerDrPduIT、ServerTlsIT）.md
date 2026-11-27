# Server 功能测试（ServerFuncIT、ServerValidityIT、ServerMoIT、ServerDrPduIT、ServerTlsIT）

> 日期：2026-11-27。用例代码在 `regression/test/it/`，都在 `regression/suites.txt`：
>
> | 套件 | 模式 | 项数 |
> |---|---|---|
> | ServerFuncIT | it-server | 105 项通过，22 项已知缺陷，1 项已修好（FIXED） |
> | ServerValidityIT | it-server | 19 |
> | ServerMoIT | it-server | 9 |
> | ServerDrPduIT | it-server | 25 |
> | ServerTlsIT | it-tls | 7 项通过，1 项已知缺陷 |
>
> 判定依据是 SMPP 3.4 规范和《配置项说明》，不改产品代码。早期的完整报告见《SMPP-Server功能测试报告》。Server 内部的连接、DR 发送机制在《Server连接与DR发送测试》，多个 Server 进程在《多Server与故障恢复测试》。

## 1. 测什么

真实的 `MultiSmppServer`（TLS 是 `SslSmppServer`）在本进程里启动，用例用一个**原始的 SMPP 客户端**连上去：读到的每个 PDU 都按顺序记下，不自动回应，所以能看到 Server 回了什么、没回什么。

| 部分 | 套件 | 内容 |
|---|---|---|
| 绑定 | ServerFuncIT | 密码、system_id、IP、版本、绑定方式、会话数、`SmppServerModules`、绑定超时、连接数上限 |
| 保活和断开 | ServerFuncIT | enquire_link、unbind、空闲断开、Server 主动保活、客户直接断开时清理登记 |
| 提交 | ServerFuncIT | 消息 ID、写进 MT 流、编码、长短信（UDH、SAR）、message_payload、长度、空内容、拆包和粘包、消息 ID 格式 |
| 限流和计费 | ServerFuncIT | 入向限速、按网关计费的库存、Redis 不通时也回应、入向队列满 |
| DR | ServerFuncIT、ServerDrPduIT | deliver_sm 回执的格式、状态文字、地址对调、多个连接、窗口、DR 等待回应的存放、客户不回应、客户没连上、只接收的绑定 |
| 协议错误 | ServerFuncIT | 绑定前提交、未知命令、截断的 PDU、错误的 command_length、不支持的命令 |
| 安全 | ServerFuncIT | 日志里的密码、错误密码的限制、PDU 日志开关 |
| 重新加载 | ServerFuncIT | 改密码只影响这个客户 |
| 有效期 | ServerValidityIT | 客户 submit_sm 的 validity_period：绝对、相对时间，范围检查，`SMSOptionValidityPeriodNoRange` |
| MO | ServerMoIT | Core 交来的 MO 作为 deliver_sm（esm_class 0）发给客户，客户的回应报给 Core |
| DR 重发 | ServerDrPduIT | 第一次和重发时回执里的 `stat:`，客户不回应时的结果 11005 |
| TLS | ServerTlsIT | 握手、绑定、提交、DR、错误密码、明文连 TLS 端口 |

## 2. 怎么跑、怎么判定

```
regression/run.sh ServerFuncIT     # 其余套件同样
cat regression/.work/results/ServerFuncIT.txt
```

- 环境：测试服务器上的回归环境（JDK 17，本地 Redis 16379）。Server 端口：ServerFuncIT 17776、ServerValidityIT 17789、ServerMoIT 17787、ServerDrPduIT 17785、ServerTlsIT 17778（TLS）。
- 提交：看 Server 回的 submit_sm_resp，再从 `stream:mt:pending:{客户}:*` 读出 Server 写进去的消息检查字段。
- DR：本程序扮演 Core，把 DR 写进客户的 DR 流 `stream:dr:pending:{客户}:{Server模块}:*`，看客户收到的 deliver_sm。
- **已知缺陷**（`regression/known-defects.txt`）输出 KNOWN，不算失败；清单里的检查通过了输出 FIXED，提示可以从清单里删掉。

## 3. 公共配置

### 3.1 ServerFuncIT 的客户

公共部分：

```
[F70]
CCBSSID: 70
A2PPlayerType: operator
Protocol: SMPP
ConnectionType: MC
WindowSize: 20
（额外配置）
IncomingConnection: (ConnName=F70_c1,userName=f70,password=pw70,URL=127.0.0.1,SessionNumber=20,MinReceiverNumber=1,MaxReceiverNumber=5,BindMode=TRX,Version=V34)
```

用户名是 `f{ssid}`，密码是 `pw{ssid}`。

| 块 | 额外配置 / 改动 | 用于 |
|---|---|---|
| F70 | — | 大多数用例 |
| F71 | `SMSOptionIncomingThrottlingNum: 5` | T-01 |
| F72 | `SMSOptionChargeByGateWay: true` | T-03 |
| F73 | `SMSOptionChargeCountryByGateWay: true` | T-04 |
| F74 | `SessionNumber=2` | B-11 |
| F75 | `URL=10.0.0.1` | B-06 |
| F90 | 两个连接，`URL=10.0.0.1`、`10.0.0.2` | B-06b |
| F91 | 两个连接，`URL=127.0.0.1`、`10.0.0.2` | B-06c |
| F76 | `Version=V33` | B-07 |
| F77 | `BindMode=R` | B-08 |
| F78 | `BindMode=T` | B-09、D-10 |
| F79 | `SMSOptionSMPPIsGenHexMsgId: true` | S-18a |
| F80 | `SMSOptionSupportMaxInMsgID: true` | S-18b |
| F81 | `SMSOptionSMPPNeedReceiptedMessageId: true`、`DrSwapAddr: false` | D-03、D-04 |
| F82 | `SmppServerModules: OtherServer9` | B-12 |
| F83 | `ServerEnquireLink: true`、`EnquireLinkTime: 1`、`FailEnquireLinkTime: 1`、`EnquireLinkFailureNum: 2` | K-04 |
| F84 | `ConnectionSilentTime: 3` | K-03 |
| F85 | `WindowSize: 2` | D-06 |
| F86 | `SMPPDRPendingMode: redis` | D-07 |
| F87 | `BufferTimeout: 2` | D-08 |
| F88、F89 | — | 其他 |

`BindMode` 是从网关的角度说的：`R` 表示网关只接收，客户用 bind_transmitter；`T` 表示网关只发送，客户用 bind_receiver。

测试里改的 GmmsConfig：`SmppServerBindTimeoutMs`（1500）、`SmppServerMaxConnectionsPerIp`（1）、`SmppServerMaxConnections`（1）、`SmppServerSessionKeeperShared`（false）、`SmppServerInboundPerSsidQueueSize`（20）、`SmppServerInboundMaxInFlightPerSsid`（1）、`SmppServerInboundDispatchBatchPerSsid`（1）、`SmppServerInboundQueueFullCommandStatus`（20），用完改回去。

### 3.2 其他套件的客户

| 套件 | 块 | 额外配置 |
|---|---|---|
| ServerValidityIT | V95 | `SMSOptionParseValidityPeriod: true`、`ExpireTime: 5` |
| ServerValidityIT | V96 | `ExpireTime: 5` |
| ServerValidityIT | V97 | `SMSOptionValidityPeriodNoRange: true`、`ExpireTime: 5` |
| ServerMoIT、ServerDrPduIT | SMPPCUST31（基础配置） | 见下 |
| ServerTlsIT | TLS95 | `Protocol: SSLSMPP`，用户 t95、密码 pw95 |

```
[SMPPCUST31]
CCBSSID: 31
A2PPlayerType: operator
Protocol: SMPP
ConnectionType: MC
WindowSize: 20
BufferTimeout: 3
IncomingConnection: (ConnName=SMPPCUST31_c1,userName=c31,password=p31,URL=127.0.0.1,SessionNumber=200,MinReceiverNumber=1,MaxReceiverNumber=5,BindMode=TRX,Version=V34)
```

ServerTlsIT 的 GmmsConfig 追加 `Ssl.KeyStorePath`、`Ssl.TrustStorePath`（回归脚本生成的证书）、`SslSmppServer1.Port=17778`。

## 4. 用例

### 4.1 绑定（ServerFuncIT）

| ID | 情况 | 预期 | 结果 |
|---|---|---|---|
| B-01a～c | f70 正确绑定 bind_transceiver | 接受；回应带 system_id 和 sc_interface_version 0x34 | 通过 |
| B-02a/b | 密码错 | `ESME_RINVPASWD`（0x0E），然后关闭连接 | 通过 |
| B-03 | 不认识的 system_id | `ESME_RINVSYSID`（0x0F） | 通过 |
| B-04 | 空密码 | 0x0E | 通过 |
| B-04b/c | 密码、system_id 超过 SMPP 允许的长度 | 回错误的 bind_resp | **已知 DEF-05**：不回 |
| B-05 | 1000 次被拒的绑定 | 每次关闭前都收到 bind_resp | **已知 DEF-03**：关闭时可能丢（这次丢了 23 次，竞争，有时通过） |
| B-06 | 客户端 IP 不在客户的 URL 里（F75 是 10.0.0.1） | 拒绝 | **待定（OPEN）**：单个连接时不检查 URL，接受了 |
| B-06b | 同一用户多个连接，没有一个是这个 IP（F90） | 拒绝（0x0F） | 通过 |
| B-06c | 同一用户多个连接，有一个是这个 IP（F91） | 接受 | 通过 |
| B-07a～c | `Version=V33`（F76） | 3.3 绑定接受；3.4 绑定 `ESME_RBINDFAIL`（0x0D）；V34 的客户用 3.3 绑定也是 0x0D | 通过 |
| B-08a～c | `BindMode=R`（F77） | bind_transmitter 接受；bind_receiver、bind_transceiver 拒绝 | 通过 |
| B-09a/c | `BindMode=T`（F78） | bind_receiver 接受；bind_transceiver 拒绝 | 通过 |
| B-09b | `BindMode=T` 用 bind_transmitter | 拒绝 | **待定（OPEN）**：接受了 |
| B-10a/b | `BindMode=TRX` | bind_receiver、bind_transmitter 拒绝 | 通过 |
| B-11a～c | `SessionNumber=2`（F74） | 两个接受；第三个拒绝（0x0D）；关掉一个后又可以 | 通过 |
| B-12 | `SmppServerModules` 不含本模块（F82） | 拒绝（0x0D） | 通过 |
| B-13 | 已绑定的连接再绑定 | `ESME_RALYBND`（0x05） | **已知 DEF-08**：回的是 0x08 |
| B-14 | `SmppServerBindTimeoutMs` 内没有绑定 | 关闭连接 | 通过 |
| B-15a～c | `SmppServerMaxConnectionsPerIp=1`、`SmppServerMaxConnections=1` | 第二个连接被关；重新加载去掉限制后能绑定 | 通过 |

### 4.2 保活和断开（ServerFuncIT）

| ID | 情况 | 预期 / 实测 |
|---|---|---|
| K-01 | enquire_link | 回应，sequence 相同 |
| K-02a～c | unbind | 回应后关闭；100 次都在关闭前收到 unbind_resp（丢 0） |
| K-03a/b | `ConnectionSilentTime=3`（F84） | 有流量 4 秒不断；没流量 4898 毫秒后关闭 |
| K-04a/b | Server 主动保活（F83，1 秒） | 有回应：3 次，连接保持；不回应，`EnquireLinkFailureNum=2`：2100 毫秒后关闭 |
| K-05a/b | 会话登记 | 绑定后登记；客户直接断开（不 unbind）后删掉 |
| K-06 | `SmppServerSessionKeeperShared=false` | 会话用自己的线程，照常工作 |

### 4.3 提交（ServerFuncIT，f70）

| ID | 情况 | 预期 / 实测 |
|---|---|---|
| S-01a | submit_sm | 接受，十进制的 message_id（如 `5651010001`） |
| S-01b～d | 看 `stream:mt:pending:70` | ssid、发送方 `SENDER`、接收号码、内容 `hello` 都对；记下了 Server 模块（DR 回来时用） |
| S-02 | 200 条 | 200 个不同的 message_id |
| S-03 | UCS2（dcs 8） | 解出 `你好` |
| S-04 | Latin-1（dcs 3） | 解出 `café` |
| S-05 | 二进制（dcs 4），客户没有 `SMSOptionSupportIncomingBinary` | 接受 |
| S-06 | message_payload TLV，sm_length 0 | 取 payload 的内容 |
| S-07 | UDH 长短信（esm 0x40） | UDH `05 00 03 07 02 01` 和内容 `part1` 分开 |
| S-08 | SAR TLV | 总数 / 序号 3/2 |
| S-09 | registered_delivery 0 / 1 | 原样保留 |
| S-10 | 254 字节 | 完整接受 |
| S-11 | 空内容 | 回应（0） |
| S-12 | 空接收号码 | 协议上应回 `ESME_RINVDSTADR` | **待定（OPEN）**：接受了（Core 会再检查） |
| S-13 | 非数字接收号码 | 同上 | **待定（OPEN）** |
| S-14 | 空发送方 | 回应（0） |
| S-15 | PDU 一个字节一个字节地发 | 正常回应 |
| S-16 | 一个包里 3 个 PDU | 3 个回应（seq 710、711、712） |
| S-17 | 客户发 deliver_sm | 回应 |
| S-18a | `SMSOptionSMPPIsGenHexMsgId`（F79） | 十六进制 ID `63a66001` |
| S-18b | `SMSOptionSupportMaxInMsgID`（F80） | 13 位 ID |
| S-18c | SMPP 3.3 客户 | 十六进制 ID |

### 4.4 限流和计费（ServerFuncIT）

| ID | 情况 | 预期 / 实测 |
|---|---|---|
| T-01 | `SMSOptionIncomingThrottlingNum=5`（F71），一下子 60 条 | 5 条接受，55 条限流 |
| T-02a～c | Redis 里的 `thcon` 开关 | 已经不再读：没有 key 也接受，enquire_link 照常回应 |
| T-03a | `SMSOptionChargeByGateWay`（F72），`stock:72` = 2 | 前 2 条接受，第 3 条 0x59；库存变成 0 |
| T-03b | 没有库存 key | 0x59 |
| T-04a | `SMSOptionChargeCountryByGateWay`（F73），`stock:73:CN` = 1 | 第一条接受，第二条 0x59 |
| T-04b | 计费客户发了无效号码 | 也有回应（0x59） |
| T-05a/b | Redis 不回应 | 照样回应（0x08）；Redis 恢复后同一个连接接受 |
| T-06 | `SmppServerInboundPerSsidQueueSize=20`，一下子 2000 条 | 2000 条都有回应：284 接受，1716 队列满 |
| T-07 | `SmppServerInboundQueueFullCommandStatus=20`（0x14） | 队列满时回 0x14（1700 条） |

### 4.5 DR（ServerFuncIT）

| ID | 情况 | 预期 |
|---|---|---|
| D-01a～e | DELIVERED 的 DR | deliver_sm，esm_class 4；回执里有 `stat:DELIVRD` 和消息 ID；默认对调地址（src 是接收号码，dst 是发送方）；默认没有 receipted_message_id |
| D-02 | 状态文字 | 10000 → `DELIVRD err:000`；10200 → `EXPIRED err:001`；2300 → `UNDELIV err:003`；10300 → `UNDELIV err:002` |
| D-03 | `SMSOptionSMPPNeedReceiptedMessageId`（F81） | 带 receipted_message_id |
| D-04 | `DrSwapAddr: false`（F81） | 不对调 |
| D-05 | 3 个连接，12 条 DR | 每条一次，每个连接 4 条 |
| D-06a/b | `WindowSize=2`（F85），客户先不回 | 线上只有 2 条；回了以后 6 条都发出，没有重复 |
| D-07a/b | `SMPPDRPendingMode: redis`（F86） | DR 发出；回应被接受，没有残留的 pending key |
| D-08a/b | `BufferTimeout=2`（F87），客户不回 | 超时，打日志 |
| D-09a/b | 客户没连上 | 日志“会话找不到”；流里的条目确认，不留在待确认 |
| D-10a | bind_receiver 的客户（F78） | 收到 DR |
| D-10b | 同一客户的 bind_transmitter 连接 | 这个连接上没有 DR |

### 4.6 协议错误（ServerFuncIT）

| ID | 情况 | 预期 | 结果 |
|---|---|---|---|
| P-01a/b | 绑定前 submit_sm | `ESME_RINVBNDSTS`（0x04），关闭 | 通过 |
| P-02 | 绑定前 enquire_link | 0x04 | **FIXED**：这次通过了（DEF-03，关闭前的回应可能丢，竞争） |
| P-03 | 未知命令 | generic_nack `ESME_RINVCMDID` | **已知 DEF-04**：不回 |
| P-04 | 截断的 submit_sm | 回错误 | **已知 DEF-04** |
| P-05 | 300 个 0 字节当作 submit_sm | 回错误 | **已知 DEF-04** |
| P-06a/b/d | query_sm、cancel_sm、data_sm（不支持） | `ESME_RINVCMDID`（0x03） | **已知 DEF-08**：回的是 0x08 |
| P-06c | replace_sm | 同上 | **已知 DEF-04**：不回 |
| P-07a～c | 客户发来意外的 submit_sm_resp、generic_nack | 忽略，会话照常 | 通过 |
| P-08 | command_length 为 8、0、15、-1 | generic_nack `ESME_RINVCMDLEN` 或关闭 | **已知 DEF-04**：不回 |
| P-09 | command_length 2GB，再 50MB | 不按它分配内存（堆 17 → 17 MB） | 通过 |
| P-10 | 在 bind_receiver 的会话上提交 | 0x04 | **已知 DEF-06**：接受了 |

### 4.7 安全、重新加载（ServerFuncIT）

| ID | 情况 | 预期 | 结果 |
|---|---|---|---|
| SEC-01 | 绑定密码 | 不写进日志 | **已知 DEF-01**：PDU 日志里有（1 行） |
| SEC-02 | 反复用错密码 | 被限制 | **已知 DEF-07**：没有限制（300 次 102 毫秒） |
| SEC-03 | 之后用对的密码 | 照样能绑定 | 通过 |
| SEC-04 | `SmppPduLogEnable=false` | 没有 PDU 日志 | 通过 |
| R-01a～d | `RELOAD_CUSTOMER` 改一个客户的密码 | 旧密码被拒、新密码接受；另一个客户的会话不受影响 | 通过 |

### 4.8 有效期（ServerValidityIT）

客户发的 submit_sm 带 validity_period，看 Server 算出的过期时间离现在多少分钟。客户的 `ExpireTime: 5`（5 小时 = 300 分钟）；客户能指定的范围是 `MinMessageExpireTimeFromCust` ～ `MaxMessageExpireTimeFromCust`（默认 60～10080 分钟）。

| ID | 客户 | validity_period | 预期（分钟） |
|---|---|---|---|
| VP-00 | V95（`SMSOptionParseValidityPeriod`） | — | 绑定 |
| VP-01 | V95 | 绝对时间，东八区，2 小时后 | 120 |
| VP-02 | V95 | 绝对时间，东五区半（nn 22） | 120 |
| VP-03 | V95 | 绝对时间，西五区（nn 20，`-`） | 120 |
| VP-04 | V95 | 相对 2 小时 | 120 |
| VP-05 | V95 | 相对 1 天 12 小时 | 2160 |
| VP-06 | V95 | 相对 30 分钟（小于 60） | 用 `ExpireTime`：300 |
| VP-06b | V95 | 相对 5 分钟 | 300 |
| VP-07 | V95 | 相对 8 天（大于 10080） | 300 |
| VP-08 | V95 | 没有 | 300 |
| VP-09 | V95 | 看不懂的格式 | 300 |
| VP-10a/b | V96（没有这个选项） | 相对 2 小时 | 不读：300 |
| VP-11a | V97（`SMSOptionValidityPeriodNoRange`，没有 `SMSOptionParseValidityPeriod`） | — | 绑定 |
| VP-11b | V97 | 相对 5 分钟 | 不检查范围：5 |
| VP-11c | V97 | 绝对时间 5 分钟后 | 5 |
| VP-11d | V97 | 相对 8 天 | 11520 |
| VP-11e/f | V97 | 没有；看不懂 | 300 |

过期时间按 GMT 墙上时间保存（`getGMTTime`）。以前解析出的时间按真实时刻保存，比默认值和所有检查晚了时区差（东八区 8 小时），已修复。

### 4.9 MO（ServerMoIT，客户 31）

| ID | 情况 | 预期 |
|---|---|---|
| SM-00 | — | 客户绑定 |
| SM-01/02 | Core 把 MO 写进客户的流 | `stream:dr:pending:31:MultiSmppServer1:1`，写入成功 |
| SM-03 | — | 客户收到 deliver_sm，内容 `hello from a phone` |
| SM-04 | — | esm_class 0（MO，不是回执） |
| SM-05 | — | source 是手机 `8613800000001`，destination 是客户的号码 `106601` |
| SM-06 | 客户回 deliver_sm_resp | 报给 Core 一个成功的 submit_resp（Core 再写 OUT_SUBMIT CDR，和 MT 一样） |
| SM-07 | 客户不回（`BufferTimeout` 3 秒） | 报给 Core 失败的 submit_resp 2700 |
| SM-08 | 客户没绑定 | 失败的 submit_resp 2500 |

### 4.10 DR 重发时的回执（ServerDrPduIT，客户 31，`BufferTimeout` 3 秒）

**第一次发（SD-01）**：

| 状态 | 回执的 `stat:` |
|---|---|
| 10000 | DELIVRD |
| 10400、10405、10402 | UNDELIV |
| 10200 | EXPIRED |
| 10500 | REJECTD |
| 10900、10801 | UNKNOWN |

**客户不回应，重发（SD-02、SD-03）**：每次客户不回，结果都是 **11005**，真实状态留在状态文字里（如 `11005 Delivered`）；按 RDQ 重发时的做法（`getResendDR`）恢复状态码后重发：

| 原来的状态 | 恢复后 | 回执的 `stat:` |
|---|---|---|
| 10000 | 10000 | DELIVRD |
| 10400 | 10400 | UNDELIV |
| 10200 | 10200 | EXPIRED |
| 10500 | 10500 | REJECTD |
| 10900 | 10900 | UNKNOWN |
| 10405 | 10405 | UNDELIV |
| 10402 | 10402 | UNDELIV |
| 10801（发送方黑名单） | 10900 | UNKNOWN |

### 4.11 TLS（ServerTlsIT）

| ID | 情况 | 预期 | 结果 |
|---|---|---|---|
| TLS-00 | 启动 `SslSmppServer` | 启动 | 通过 |
| TLS-01 | 握手 | TLSv1.3 | 通过 |
| TLS-02/03 | 绑定、提交 | 都成功 | 通过 |
| TLS-04 | — | 会话登记在 `smpp:server:active:95:SslSmppServer1` | 通过 |
| TLS-05 | DR 经 TLS 发给客户 | 收到 deliver_sm | **已知 DEF-02**：`SslSmppServer` 不启动 DR 消费者 |
| TLS-06 | 错误密码 | 0x0E | 通过 |
| TLS-07 | 明文 TCP 连 TLS 端口 | 不能绑定 | 通过 |

## 5. 最近一次结果

| 时间 | 环境 | 结果 |
|---|---|---|
| 2026-11-27 | 测试服务器全量回归（提交 `32c4d33` 的代码） | ServerFuncIT 105 项通过、22 项已知缺陷、P-02 FIXED；ServerValidityIT 19、ServerMoIT 9、ServerDrPduIT 25 项全部通过；ServerTlsIT 7 项通过、1 项已知缺陷 |

## 6. 已知缺陷和没有覆盖到的

**已知缺陷**（没修，回归里记为 KNOWN）：

| 编号 | 内容 | 检查 |
|---|---|---|
| DEF-01 | 绑定密码写进 PDU 日志 | SEC-01 |
| DEF-02 | `SslSmppServer` 不启动 DR 消费者，TLS 客户收不到 DR | TLS-05 |
| DEF-03 | 关闭连接时 bind_resp 等回应可能丢（竞争） | B-05、P-02 |
| DEF-04 | 未知、畸形的 PDU 不回应 | P-03、P-04、P-05、P-06c、P-08 |
| DEF-05 | 超长的密码、system_id 不回 bind_resp | B-04b、B-04c |
| DEF-06 | bind_receiver 会话上的提交被接受 | P-10 |
| DEF-07 | 错误密码没有次数限制 | SEC-02 |
| DEF-08 | 再次绑定、不支持的命令回的状态码不对 | B-13、P-06a、P-06b、P-06d |
| OPEN | 单个连接不检查 URL；`BindMode=T` 也接受 bind_transmitter；空的、非数字的接收号码被接受 | B-06、B-09b、S-12、S-13 |

**没有覆盖到的**：

| 内容 | 说明 |
|---|---|
| HTTP 方式接入的客户 | 只测了 SMPP |
| outbind | 没有用例 |
| SMPP 5.0 | 没有用例 |
