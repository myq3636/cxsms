# 替换功能测试（ReplacementIT）

> 日期：2026-11-27。用例代码：`regression/test/it/ReplacementIT.java`，56 项，在 `regression/suites.txt`（it 模式，排在 CoreContentIT 后面）。
> 测发往通道之前的内容、地址处理：内容替换 `SMSOptionContentReplacementPolicy`、模板替换 `ReplaceContentByTemplate`、发送方长度控制 `SMSOptionOriNumberLenControl`、发送方替换（`senderReplacement.cfg` 的 `SenderReplacement`，客户侧和通道侧）、`…Prefix4NewMT` / `…Prefix4DRMO`、通道的发送方 / 接收号码 / 内容名单，以及改道、重发时不叠加。
> 设计和修复说明见《通道外发处理恢复与替换缺陷修复设计》。第一版（2026-11-27 上午）测出 13 项失败，修复后全部通过；第一版的结果见第 5 节。

## 1. 各功能在哪里做

| 功能 | 配在 | 在哪里做 |
|---|---|---|
| `SMSOptionContentReplacementPolicy` | 客户 | Core `MessageProcessorThread.replaceContent`，路由之后 |
| `ReplaceContentByTemplate`（+ `contentTemplate.cfg`、`SMSOptionTemplateCodeRegex`、`IsReplaceTemplateBySender`） | 路由规则 | 同上，内容替换之后；只用路由这条 MT 的那条规则的模板 |
| `SenderReplacement`，客户侧（客户 `SMSOptionAdvancedSenderReplacement`） | `senderReplacement.cfg` 里 CCBSSID 为客户的块 | Core 入向（`CoreMessageProcessor`），路由之前 |
| `SMSOptionOriNumberLenControl` | 通道 | `ChannelOutboundPreparer`，模板之后、拆分之前 |
| 通道的名单（2111 / 2112 / 2121 / 2201） | `senderactiondir`、`recipientactiondir`、`contentactiondir` 里通道 ssid 的文件，退到 `-2` | 同上 |
| `SenderReplacement`，通道侧（通道 `SMSOptionAdvancedSenderReplacement`） | `senderReplacement.cfg` 里 CCBSSID 为通道的块 | 同上 |
| `…Prefix4NewMT` | 通道 | 同上 |
| `…Prefix4DRMO` | 客户 | 回给客户的 DR 恢复客户原地址之后（`transactDR`、`DirectInDRDispatchService`） |

每次进入发往通道的处理（第一次、改走备份、从 RMQ 重发），先把内容、发送方、接收号码恢复成基准（`inTextContent`、`inSenderAddress`、`originalRecipientAddr`），再按这一次的通道做一遍。

`A2PSenderAddrReplace` / `A2PRecipientAddrReplace` 不再支持，不测。

## 2. 怎么跑、怎么判定

```
regression/run.sh ReplacementIT
cat regression/.work/results/ReplacementIT.txt
```

- 环境：测试服务器上的回归环境（JDK 17，本地 Redis 16379）。进程内启动真实的 Core（`MTStreamConsumer`、`ResultStreamConsumer`、`CoreMQMStreamConsumer`、`InboundDRStreamConsumer`），不连数据库（`RecordingStore`）。
- 客户的 MT 写进 `stream:mt:pending:{客户}:*`，看通道的出向流 `stream:mt:routed:{通道}:*` 里那条消息的内容、内容类型、长度、发送方、接收号码（就是 Client 发给供应商的）。被通道名单拒绝的，看 OUT_SUBMIT CDR 的状态码。
- 改道：通道回 2500，Core 改发备份通道。RMQ 重发：通道回 2500 写 RMQ 后，按 RMQ 的 SQL 还原消息写进 MQM 流。DR：通道接受后写一条供应商 DR，看客户的 DR 流。
- 接收号码：`8613812345678`（N13）、`8615912345678`（N15）、`85212345678`（HK）。

## 3. 配置

### 3.1 cm.cfg

通道（公共部分 `CCBSSID`、`A2PPlayerType: operator`、`Protocol: SMPP`；`retry` = `SMSOptionSubmitRetryCode: 2500`、`SMSOptionMTRetryPolicy: 1S,1S`）：

| 块 | 额外配置 |
|---|---|
| V801、V808、V809 | — |
| V802 | `SMSOptionOriNumberLenControl: (Min=8,Max=11,Suffix=00)` |
| V803 | `SMSOptionAdvancedSenderReplacement: true` |
| V804 | `ChlMsgSplit: yes`、`SMSOptionAsciiMsgSplitLen: 70`、`MsgSplitLen: 70` |
| V805 | `SMSOptionAddOriPrefix4NewMT: 00`、`SMSOptionRemoveRecPrefix4NewMT: 86` |
| V806 | 同 V804 的拆分，加 `SMSOptionAdvancedSenderReplacement: true` |
| V807 | retry |
| V815 | `CustomerTypeBySender: VIP` |
| V816 | — |
| V817 | retry，`SMSOptionAddOriPrefix4NewMT: 11` |
| V818 | `SMSOptionAddOriPrefix4NewMT: 22` |

客户：

| 块 | 额外配置 |
|---|---|
| R810 | `SMSOptionContentReplacementPolicy: (prefix=all,value=foo:bar\|hello^world:hi\|spam\|你好:您好);(prefix=86,value=baz:qux\|\{@phone#(\d+)\}:\{@phone#(\d+)\});(prefix=8613,value=one:two);(prefix=852,value=hk:HKG\|ab:AB)` |
| R811 | `…(prefix=all,value=price:$5\|foo:bar)` |
| R812 | `…(prefix=all,value=[0-9]{6}:******)` |
| R813 | `…(prefix=all,value=LONGWORD:0123456789012345678901234567890123456789)` |
| R814、R851 | `…(prefix=all,value=foo:foofoo)` |
| R852 | `…(prefix=all,value=([0-9]{3})[0-9]{4}:$1****)` |
| R826 | `SMSOptionTemplateCodeRegex: [A-Z]{2}[0-9]{4}` |
| R840 | `SMSOptionAdvancedSenderReplacement: true` |
| R850 | `SMSOptionAddOriPrefix4DRMO: 99` |
| 其余（R820～R830、R841、R842、R845～R849、R853） | — |

（`…` 是 `SMSOptionContentReplacementPolicy: `。）

### 3.2 路由（都是 `A2PMainRouting`，`Priority=30`）

| 客户 | 规则 |
|---|---|
| R810～R812 | `86;852 → V801` |
| R813 | `86 → V804` |
| R814 | 主 `86 → V807`，备份 `86 → V808` |
| R820、R826 | `86 → V801`，`ReplaceContentByTemplate=T1` |
| R821 / R822 / R827 / R829 | 同上，模板 T2 / T3 / T5 / T9 |
| R823 | `(86 → V801, T86);(8613 → V801, T8613)` |
| R824 | `(86 → V801, T1);(8613 → V801，没有模板)` |
| R825 | `(Priority=30,86,Sender=SND1 → V801, T1, IsReplaceTemplateBySender=true);(Priority=31,86 → V801)` |
| R828 | 主 `86 → V807`（T1），备份 `86 → V808` |
| R830 | `86 → V802` |
| R840 | `86;852 → V801` |
| R841、R852 | `86 → V801` |
| R842、R850 | `86 → V803` |
| R845 / R846 / R847 / R848 / R853 | `86 →` V805 / V806 / V809 / V816 / V815 |
| R849 | 主 `86 → V817`，备份 `86 → V818` |
| R851 | `86 → V817` |

### 3.3 文件

`conf/contentTemplate.cfg`：

```
[T1]
English: your code is $shortcode$ ok
UNICODE: 验证码$shortcode$

[T2]
English: sender tpl $shortcode$
Sender: TPLSENDER

[T3_a]
English: pool a $shortcode$
Sender: POOLSENDER

[T86]
English: T86 $shortcode$

[T8613]
English: T8613 $shortcode$

[T5]
UNICODE: 只有中文$shortcode$
```

`conf/senderReplacement.cfg`：

```
[S840]
CCBSSID: 840
SenderReplacement: (PhonePrefix=86,SenderAddress=OLD1,ReplaceTo=NEW1,Ossid=840,Rssid=-1);(PhonePrefix=8613,SenderAddress=OLD1,ReplaceTo=NEW13,Ossid=840,Rssid=-1);(PhonePrefix=all,SenderAddress=all,ReplaceTo=ALLNEW,Ossid=840,Rssid=-1)

[S841]
CCBSSID: 841
SenderReplacement: (PhonePrefix=86,SenderAddress=OLD1,ReplaceTo=NEW1,Ossid=841,Rssid=-1)

[S803]
CCBSSID: 803
SenderReplacement: (PhonePrefix=86,SenderAddress=OLD3,ReplaceTo=NEW3,Ossid=842,Rssid=803);(PhonePrefix=86,SenderAddress=OLD3,ReplaceTo=NEW3X,Ossid=-1,Rssid=803)

[S806]
CCBSSID: 806
SenderReplacement: (PhonePrefix=86,SenderAddress=OLD6,ReplaceTo=NEW6,Ossid=-1,Rssid=806)
```

通道的名单：

| 文件 | 内容 |
|---|---|
| `senderactiondir/809_sender_blacklist.cfg`、`815_sender_blacklist.cfg` | `86:VBAD` |
| `senderactiondir/816_sender_whitelist.cfg` | `86:wl:VGOOD:x` |
| `recipientactiondir/809_recipient_blacklist.cfg` | `8613899999999` |
| `contentactiondir/809_content_blacklist.cfg` | `86:vendorbad` |

## 4. 用例和结果（2026-11-27，测试服务器，全部通过）

### 4.1 内容替换

| ID | 情况 | 预期 / 实测 |
|---|---|---|
| CR-01 | `foo hello world spam end` → N15 | `bar hi  end`（`^` 是空格，只写 key 删掉） |
| CR-02a | `baz hk` → N15 | 只有 86 组：`qux hk` |
| CR-02b | `baz hk` → HK | 只有 852 组：`baz HKG` |
| CR-02c | `ab` → HK，`ab:AB` | `AB`（只改大小写的规则按普通替换） |
| CR-03 | `baz one` → N13 | 86 组和 8613 组都执行：`qux two` |
| CR-04a | `code {@phone#4} end` → N15 | `code 5678 end` |
| CR-04b | 同上 → N13 | `code 5678 end`（86 组先于 8613 组，`one:two` 不会先改掉占位符） |
| CR-05 | `baz` → `+8615…` | `qux` |
| CR-06 | UCS2 `你好世界` | `您好世界`，长度 8 |
| CR-07 | `[0-9]{6}:******` | key 是正则：`code ****** end` |
| CR-08 | `price:$5\|foo:bar`，`price foo` | `$5 bar`（key 没有分组，`$5` 按字面） |
| CR-08b | `([0-9]{3})[0-9]{4}:$1****`，`call 1234567 now` | `call 123**** now`（分组引用照常） |
| CR-09 | 替换后 86 字符，通道按 70 拆 | 先替换后拆分：2 段 |
| CR-10a/b | `foo:foofoo`，主通道 V807 失败改走 V808 | 主通道、备份通道都是 `foofoo` |

### 4.2 模板替换

| ID | 情况 | 预期 / 实测 |
|---|---|---|
| TP-01 / TP-02 | `your otp 1234` / `otp 12-34` | `your code is 1234 ok` |
| TP-03 | UCS2 `验证码 5678` | `验证码5678`，UCS2 |
| TP-04 | 模板只有 UNICODE，ASCII 消息 | `只有中文4321`，UCS2 |
| TP-05a/b | 模板 T2（带 `Sender: TPLSENDER`） | `sender tpl 1234`，发送方 TPLSENDER |
| TP-06 | T3 不存在，模板池 T3_a | `pool a 1234`，发送方 POOLSENDER |
| TP-07 | 规则 86（T86）、8613（T8613） | N13 → `T8613 1234`，N15 → `T86 1234` |
| TP-08 | 规则 86 有 T1、规则 8613 没有模板，发 N13 | 不替换：`otp 1234` |
| TP-09 | `IsReplaceTemplateBySender`，`Sender=SND1` | SND1 替换，SND2 不替换 |
| TP-10 | `SMSOptionTemplateCodeRegex` | `your code is AB1234 ok` |
| TP-11a/b | 主通道有模板，失败改走没有模板的备份 | 主通道替换；备份收到 `otp 2468` |
| TP-12 | 模板 T9 哪里都没有 | 不发，DR 10503 |

### 4.3 发送方长度控制（R830 → V802：Min 8、Max 11、Suffix 00）

| ID | 发送方 | 预期 / 实测 |
|---|---|---|
| OL-01 | `1234567` | `123456700` |
| OL-02 | `123456789012` | `12345678901` |
| OL-03 | `12345678` | 不变 |
| OL-05 | `ABC` | `ABC00` |
| OL-04 | 没配这个选项的通道 | 不变 |

### 4.4 发送方替换、前缀

| ID | 情况 | 预期 / 实测 |
|---|---|---|
| SR-01～04 | 客户侧（R840）：OLD1 → N13 / N15，OTHER → HK / N15 | NEW13、NEW1、ALLNEW、ALLNEW |
| SR-05 | 客户 R841 没开 `SMSOptionAdvancedSenderReplacement` | 不替换 |
| SR-06 | 通道侧（V803），R842 的 OLD3 | NEW3 |
| SR-06b | 通道侧没有这个发送方的规则 | 不变：OTHER |
| SR-06c | 通道侧 `Ossid=-1`，客户 R850 | NEW3X |
| PX-01 | V805：`AddOriPrefix4NewMT 00`、`RemoveRecPrefix4NewMT 86` | 发送方 `0012345`，接收号码 `15912345678` |
| LS-01 | 150 字符发往 V806（按 70 拆，通道侧 OLD6 → NEW6） | 3 段，每段发送方 NEW6 |

### 4.5 通道的名单

| ID | 情况 | 预期 / 实测 |
|---|---|---|
| LT-01 | 发送方 VBAD → V809（通道黑名单） | 不发，2111 |
| LT-02 | 接收号码 `8613899999999` → V809 | 不发，2121 |
| LT-03 | 内容含 `vendorbad` → V809 | 不发，2201 |
| LT-04 | 都不命中 → V809 | 发出 |
| LT-05 | V816 有白名单，发送方 OTHER | 不发，2112 |
| LT-06 | 发送方 VGOOD（在白名单里） | 发出 |
| LT-07 | V815 是 VIP，发送方 VBAD 在它的黑名单里 | 不查发送方名单，发出 |

### 4.6 改道、重发、DR

| ID | 情况 | 预期 / 实测 |
|---|---|---|
| RB-01 | R849 → 主通道 V817（AddOri 11） | `11S` |
| RB-02 | 失败改走备份 V818（AddOri 22） | `22S`（不是 `2211S`） |
| RB-03 | R851 → V817，内容 `foo` | `11S`，`foofoo` |
| RB-04 | 失败写 RMQ，MQM 重发 | 还是 `11S`、`foofoo`（不是 `1111S`、`foofoofoofoo`） |
| DR-01 | R850 的 OLD3 → V803 发出为 NEW3X；供应商 DR DELIVERED | 客户收到的 DR 发送方：客户自己的 OLD3，再加 `AddOriPrefix4DRMO 99`：`99OLD3` |

## 5. 第一版的结果（修复前）

2026-11-27 上午第一版（31 项通过、13 项失败）的失败项和原因：

| 用例 | 原因 | 修复 |
|---|---|---|
| OL-01、OL-02 | 长度控制在流模式下没人调用 | 恢复（`ChannelOutboundPreparer`） |
| SR-06 | 通道侧 `SenderReplacement` 没人调用 | 恢复 |
| SR-07、SR-08 | `A2PSenderAddrReplace` / `A2PRecipientAddrReplace` 没人调用 | 不再支持，用例去掉 |
| CR-02c | 号码占位符按忽略大小写判断 | 要完全相同且含 `@phone#` |
| CR-04b | 前缀组、组内规则在 HashMap 里，先后不定 | 规定顺序 |
| CR-08 | `$5` 被当分组引用，异常后整个客户的替换都放弃 | 只有 key 有这个分组才算引用；一条出错只跳过这一条 |
| CR-10b | 改道时内容再替换一次 | 每次从客户原文算 |
| TP-05b | 模板 ID 正好存在时不读 Sender | 都读 |
| TP-08 | 按前缀选模板，不看路由的规则 | 按路由的规则选 |

## 6. 最近一次结果

| 时间 | 环境 | 结果 |
|---|---|---|
| 2026-11-27 | 测试服务器全量回归（本次修改） | ReplacementIT 56 项全部通过；全量 2445 项通过、0 失败、26 项已知缺陷 |

## 7. 没有覆盖到的

| 内容 | 说明 |
|---|---|
| HTTP 通道 | 通道处理对 HTTP 通道同样执行，没有单独的用例 |
| 从数据库读回 `InSenderAddr` | 不连数据库；测试服务器 MySQL 加列后可用 WdqExpiryIT 那样的手动套件验证 |
| 重发已拆过的分段 | 跳过内容替换、模板和通道处理，没有用例 |
| 模板的 `$srdm$` 随机码 | 没有用例 |
| 通道名单 NonPA + 65 只查 `-2` | 没有用例 |
