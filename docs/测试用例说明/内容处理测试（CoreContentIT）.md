# 内容处理测试（CoreContentIT）

> 日期：2026-11-27。用例代码：`regression/test/it/CoreContentIT.java`，套件 `CoreContentIT`（`regression/suites.txt`，it 模式），36 项检查。
> 功能清单见《Core模块功能与测试用例清单》第二节（CK-16～20）、第四节“外发与内容处理”、第八节“配置热加载”。

## 1. 测什么

Core 把 MT 交给 Client 之前对内容做的处理，以及几项和内容有关的检查：

| 部分 | 内容 |
|---|---|
| 长短信拆分 | 通道的拆分长度；客户不允许拆分 |
| 编码 | 转 GSM 7-bit、强制转 ASCII、UCS2 不变 |
| UDH | 通道不支持时去掉；单条消息的分段头去掉 |
| 内容替换 | `SMSOptionContentReplacementPolicy` 的 `prefix=all` 和按前缀 |
| 模板替换 | 路由的 `ReplaceContentByTemplate`；没有验证码时回失败 DR 或成功 DR |
| 发送方替换 | `SMSOptionAdvancedSenderReplacement` + `senderReplacement.cfg` |
| HTTP 通道 | 写到 HTTP 的出向流 |
| 检查 | 入向、出向反垃圾关键字；service type id 上限；有效期的默认值；定时发送 |
| 重新加载 | 内容模板、接收号码规则、反垃圾关键字、发送方黑名单修改后对新消息生效 |

## 2. 怎么跑、怎么判定

```
regression/run.sh CoreContentIT
cat regression/.work/results/CoreContentIT.txt
```

- 环境：测试服务器上的回归环境（JDK 17，本地 Redis 16379）。进程内启动真实的 Core：`MTStreamConsumer`、`ResultStreamConsumer`。
- **不连数据库**：消息存储换成记录 SQL 的 `RecordingStore`，写 SMQ（定时发送队列）的 SQL 只记下来不执行，用例检查记下的 SQL。
- 发 MT：写进 `stream:mt:pending:{ossid}:*`，默认发送方 `18667991226`，接收号码 `8613800000000`。
- 看结果：从通道的出向流 `stream:mt:routed:{通道}:*` 里读出这条 MT，检查它的内容、编码、UDH、发送方、有效期，也就是 Core 交给 Client 的样子；被拒绝的看客户 DR 流 `stream:dr:pending:{ossid}:*` 里的 DR 状态码。
- 重新加载：向 Redis 频道 `channel:gmms:control` 发命令，等 1.5 秒。

## 3. 公共配置

### 3.1 cm.cfg

每个块的公共部分：

```
[名称]
CCBSSID: ssid
A2PPlayerType: operator
Protocol: SMPP
（额外配置）
```

**通道**：

| 块 | CCBSSID | 额外配置 |
|---|---|---|
| V701 | 701 | — |
| V702 | 702 | `ChlMsgSplit: yes`<br>`SMSOptionAsciiMsgSplitLen: 100`<br>`MsgSplitLen: 100` |
| V703 | 703 | `SMSOptionConvert2GSM7bit: true`<br>`SMSOptionSupportOutgoingGSM7bit: 1` |
| V704 | 704 | `SMSOptionForceTrans2ASCII: true` |
| V705 | 705 | `SmsOptionIsSupportUDH: No` |
| V706 | 706 | `SMSOptionSupportOutgoingAntiSpam: true` |
| V707 | 707 | HTTP 通道（完整写法见下） |

```
[V707]
CCBSSID: 707
A2PPlayerType: operator
Protocol: HTTP
ChlQueue: CommonHttpClient
ChlInit: true
ChlURL: http://127.0.0.1:18080/sms
```

**客户**：

| 块 | CCBSSID | 额外配置 |
|---|---|---|
| R200 | 200 | — |
| R201 | 201 | `SupportMsgSplit: no` |
| R202～R204 | 202～204 | — |
| R205 | 205 | `SMSOptionContentReplacementPolicy: (prefix=all,value=foo:bar);(prefix=86,value=baz:qux)` |
| R206 | 206 | — |
| R207 | 207 | `DrSuccForTemplateReplaceFailed: true` |
| R208 | 208 | `SMSOptionSupportIncomingAntiSpam: true` |
| R209 | 209 | — |
| R210 | 210 | `ExpireTime: 10`（小时） |
| R211 | 211 | `SMSOptionAdvancedSenderReplacement: true` |
| R212、R213 | 212、213 | — |

### 3.2 路由

| 客户 | 路由 |
|---|---|
| R200、R201 | `A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V702)` |
| R202 | `... Relay=V703` |
| R203 | `... Relay=V704` |
| R204 | `... Relay=V705` |
| R205 | `A2PMainRouting: (Priority=30,PhonePrefix=86;852,Relay=V701)` |
| R206、R207 | `A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V701,ReplaceContentByTemplate=T1)` |
| R208、R210、R211、R213 | `... Relay=V701` |
| R209 | `... Relay=V706` |
| R212 | `... Relay=V707` |

（`...` 是 `A2PMainRouting: (Priority=30,PhonePrefix=86,`，每个客户一个文件，`CCBSSID` 是客户的 ssid。）

### 3.3 其他文件

| 文件 | 内容 |
|---|---|
| `conf/contentTemplate.cfg` | `[T1]`<br>`English: your code is $shortcode$ ok` |
| `conf/antiSpam.cfg` | `[A208]` / `CCBSSID: 208` / `IN_ASCII: spamword`（客户 208 收到的内容）<br>`[A706]` / `CCBSSID: 706` / `OUT_ASCII: outbad`（发往通道 706 的内容） |
| `conf/senderReplacement.cfg` | `[S211]` / `CCBSSID: 211` / `SenderReplacement: (PhonePrefix=86,SenderAddress=OLDID,ReplaceTo=NEWID,Ossid=211,Rssid=-1)` |
| `conf/senderactiondir/-1_sender_blacklist.cfg`、`-1_sender_whitelist.cfg` | 空（4.9 会改黑名单） |
| `Gmms/GmmsConfig.properties` | 追加 `AsyncSqlWriterEnable=false`（SQL 同步执行，便于检查） |

## 4. 用例

### 4.1 长短信拆分

| ID | 目的 | 客户 → 通道 | 输入 | 预期 |
|---|---|---|---|---|
| OUT-03a | 按通道的拆分长度拆 | R200 → V702（100） | 250 个 ASCII 字符 | 拆成 3 条 |
| OUT-03b | 拆开的内容拼回去和原文一样 | 同上 | — | 3 条的内容依次拼接等于原文 |
| OUT-04 | 客户不允许拆分 | R201（`SupportMsgSplit: no`）→ V702 | 250 个字符 | 只有 1 条，不拆 |

### 4.2 编码

| ID | 目的 | 客户 → 通道 | 输入 | 预期 |
|---|---|---|---|---|
| OUT-05a | 通道要求转 GSM 7-bit | R202 → V703 | ASCII `hello` | 交给 Client 时是 GSM 7-bit |
| OUT-05b | 通道强制 ASCII，不支持 7-bit | R203 → V704 | 标成 7-bit 的 `hello` | 交给 Client 时是 ASCII，不是 7-bit |
| OUT-05c | 默认设置的通道，UCS2 不变 | R205 → V701 | UCS2 `你好` | 仍是 UCS2 |

### 4.3 UDH

| ID | 目的 | 客户 → 通道 | 输入 | 预期 |
|---|---|---|---|---|
| OUT-06a | 通道不支持 UDH | R204 → V705 | 带端口寻址的 UDH（`06 05 04 0B 84 23 F0`） | UDH 去掉 |
| OUT-06b | 通道没配这个选项 | R202 → V703 | 同上 | UDH 保留 |
| OUT-06c | 单条消息带着分段头 | R202 → V703 | UDH `05 00 03 09 01 01`（1 条里的第 1 条） | 去掉，不当作 UDH 发 |

### 4.4 内容替换

| ID | 目的 | 客户 | 接收号码 / 内容 | 预期 |
|---|---|---|---|---|
| OUT-07a | `prefix=all` 和 `prefix=86` 都生效 | R205 | 默认 / `foo baz` | `bar qux` |
| OUT-07b | 其他国家只用 `prefix=all` | R205 | `85212345678` / `foo baz` | `bar baz` |

### 4.5 模板替换

| ID | 目的 | 客户 | 内容 | 预期 |
|---|---|---|---|---|
| OUT-08a | 内容里有验证码：换成模板 | R206 | `code 1234 here` | `your code is 1234 ok` |
| OUT-08b | 没有验证码：不发，回失败 DR | R206 | `no number` | 不发出；DR **10503**（`REJECT_TEMPLATE_ERROR`） |
| OUT-08c | 没有验证码，客户配了 `DrSuccForTemplateReplaceFailed` | R207 | `no number` | 不发出；DR 是 **DELIVERED（10000）** |

验证码用默认正则 `(?:[0-9-]){4,}` 从内容里取（客户可以用 `SMSOptionTemplateCodeRegex` 改），取到的编码去掉 `-` 和空格后填进 `$shortcode$`。

### 4.6 发送方替换

| ID | 目的 | 客户 | 发送方 | 预期 |
|---|---|---|---|---|
| OUT-10a | 按 `senderReplacement.cfg` 替换 | R211 | `OLDID` | 发出的发送方是 `NEWID` |
| OUT-10b | 其他发送方不变 | R211 | `OTHER` | `OTHER` |
| OUT-10c | 客户没开 `SMSOptionAdvancedSenderReplacement` | R205 | `OLDID` | `OLDID` |

### 4.7 HTTP 通道

| ID | 目的 | 客户 → 通道 | 预期 |
|---|---|---|---|
| OUT-02 | HTTP 通道写到 HTTP 的出向流 | R212 → V707 | 出现在 `stream:http:submit:707:*` |
| OUT-02b | 不写 SMPP 的出向流 | 同上 | `stream:mt:routed:707:*` 里没有 |

### 4.8 检查：反垃圾、service type、有效期、定时发送

| ID | 目的 | 客户 | 输入 | 预期 |
|---|---|---|---|---|
| CK-16a | 客户入向关键字（`IN_ASCII: spamword`，客户开了 `SMSOptionSupportIncomingAntiSpam`） | R208 → V701 | `this spamword here` | 不发出，DR **10700**（`REJECTED_BYANTISPAM`） |
| CK-16b | 不含关键字 | R208 → V701 | `clean text` | 发出 |
| CK-16c | 通道出向关键字（`OUT_ASCII: outbad`，通道开了 `SMSOptionSupportOutgoingAntiSpam`） | R209 → V706 | `this outbad here` | 不发出，10700 |
| CK-17a | service type id 超过 `MaxServiceTypeID`（默认 3） | R205 | service type id 5 | 不发出，DR **10500**（`REJECTED`） |
| CK-17b | service type id 3 | R205 | service type id 3 | 发出 |
| CK-18a | 消息没带有效期：用客户的 `ExpireTime`（单位小时） | R210 | 不带有效期 | 有效期约 36000 秒（10 小时） |
| CK-18b | 客户也没配：用全局 `MessageExpireTime` | R205 | 不带有效期 | 有效期等于全局默认值（误差 2000 秒以内） |
| CK-20a | 定时发送时间在将来 | R213 | 1 小时后 | 写 SMQ（定时发送队列） |
| CK-20b | 还没到时间不发 | R213 | 同上 | 出向流里没有 |
| CK-20c | 定时发送时间已过 | R213 | 1 分钟前 | 直接发出 |

有效期按 GMT 墙上时间（`getGMTTime()`）比较，见《配置项说明》的有效期部分。

### 4.9 重新加载对新消息生效

每一组都是：先发一条看现在的行为，改文件，发重新加载命令，再发一条。

| ID | 文件和改动 | 命令 | 预期 |
|---|---|---|---|
| CF-03 | `contentTemplate.cfg` 的 T1 改成 `new text $shortcode$` | `RELOAD_CONTENT_TEMPLATE` | R206 发 `code 5678 here` → `new text 5678` |
| CF-05a | （改之前） | — | R205 发 `85299990000` → 发出 |
| CF-05b | `recipientRuleFile.cfg` 写入 `[{"cc":"852","lens":[11],"allowPrefixList":["8521"]}]` | `RELOAD_RECIPIENT_RULE` | R205 发 `85299990000`（不以 8521 开头）→ 拒绝 |
| CF-06a | （改之前） | — | R208 发 `newbad text` → 发出 |
| CF-06b | `antiSpam.cfg` 的 A208 改成 `IN_ASCII: newbad` | `RELOAD_ANTISPAM` | R208 发 `newbad text` → 拒绝 |
| CF-08a | （改之前） | — | R205 用发送方 `NEWBADSND` → 发出 |
| CF-08b | `senderactiondir/-1_sender_blacklist.cfg` 写入 `86:NEWBADSND` | `RELOAD_SENDER_BLACKLIST` | R205 用发送方 `NEWBADSND` → 拒绝，DR **10801** |

## 5. 最近一次结果

| 时间 | 环境 | 结果 |
|---|---|---|
| 2026-11-26 | 测试服务器全量回归（提交 `79173c7`） | 36 项全部通过 |
| 2026-11-27 | 测试服务器全量回归（本次修改后） | 36 项全部通过；表里的 DR 状态码取自这次的实测输出 |

## 6. 没有覆盖到的

| 内容 | 说明 |
|---|---|
| Client 实际发给供应商的 PDU（`data_coding`、UDH、TLV） | 这组只看 Core 交给 Client 的消息；PDU 在 ClientPduIT |
| 长短信的合并（客户发来分段） | 在 Core 的长短信用例（CSM） |
| 定时发送到时间后由 MQM 从 SMQ 取出 | 要查数据库，没有覆盖 |
| `{@phone#N}` 号码后 N 位、模板验证码去空格 | 在 FunctionSyncIT（《号码限额与发送方检查测试说明》） |
| 反垃圾的 `IN_UNICODE`、`OUT_UNICODE` | 只测了 ASCII |
| `vendorTemplateReplacement.cfg`（按供应商的模板替换） | 没有用例 |
