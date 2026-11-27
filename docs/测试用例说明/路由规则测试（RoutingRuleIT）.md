# 路由规则测试（RoutingRuleIT）

> 日期：2026-11-27。用例代码：`regression/test/it/RoutingRuleIT.java`，套件 `RoutingRuleIT`（`regression/suites.txt`，it 模式），56 项检查。
> 规则说明见《路由配置说明》，设计见《路由重构设计》，用例清单见《路由重构测试用例清单》（文档里的 ID，例如 E01、J03，和这里的 RR-E01、RR-J03 对应）。同组：《路由优先级测试（CoreRouteIT）》《路由重新加载测试（IncrementalRoutingIT）》。

## 1. 测什么

| 部分 | 内容 |
|---|---|
| 路由文件 | 各种配置名能读进来；写错的规则被拒绝；旧配置名和 11 级备份只告警不生效 |
| 主路由选择 | 分组由长到短、组内按 Priority、`Sender` 不区分大小写、排除条件、号码层优先、默认路由 |
| 选路时用备份 | 主路由的通道不能用（不在 cm.cfg、内容超长）或主路由没有规则命中时，从备份 1 级找 |
| 发送失败后改道 | 按通道的 `SMSOptionSubmitRetryCode` 决定是否改道；逐级改道；级别用完后重试主路由；`SMSOptionMaxReroute` |
| 系统路由替换 | `vendorRoutingReplacement.cfg` 替换主路由和备份的通道 |
| 记录 | 消息里的 `routingLevel`、`routingRule`；CDR 的 MsgID；Redis 里的生效规则 |

## 2. 怎么跑、怎么判定

### 2.1 运行

```
regression/run.sh RoutingRuleIT
cat regression/.work/results/RoutingRuleIT.txt
```

- 环境：测试服务器上的回归环境（JDK 17，本地 Redis 16379）。进程内启动真实的 Core：`MTStreamConsumer`（路由）、`ResultStreamConsumer`（处理发送结果）、`CoreMQMStreamConsumer`（MQM 重发回来的消息）。
- **不连数据库**：消息存储换成记录 SQL 的 `RecordingStore`，写 RMQ（重试队列）的 SQL 只记下来不执行，用例检查记下的 SQL。
- 用例启动前，自己写入第 3、4 节的配置。

### 2.2 怎么模拟发送和失败

| 动作 | 做法 |
|---|---|
| 发一条 MT | 写进 `stream:mt:pending:{ossid}:*`。默认接收号码 `8613800000000`，内容 `hello` 或 `x`；表里的“输入”写发送方和与默认不同的号码、内容 |
| 看路由结果 | 出向流 `stream:mt:routed:{通道 ssid}:*` 里出现这条 MT（同 CoreRouteIT） |
| 通道发送失败 | 把这条 MT 作为 submit_resp 写进结果流，状态 **2500**（`COMMUNICATION_ERROR`，在重试码里）或 **2700**（`SUBMIT_RESP_ERROR`，不在重试码里），就像 Client 收到了供应商的失败应答 |
| 重试 | 级别用完后 Core 写 RMQ（记下的 SQL）；用例按这条 SQL 把消息还原，再写进 MQM 流，模拟 MQM 到时间重发 |
| 失败 DR | `stream:dr:pending:{ossid}:*` 里有给客户的 DR |

## 3. 公共配置

### 3.1 cm.cfg 块的写法

```
[名称]
CCBSSID: ssid
A2PPlayerType: operator
Protocol: SMPP
（额外配置）
```

### 3.2 通道 V901～V924

| 通道 | 额外配置 |
|---|---|
| V901～V916、V919～V924 | `SMSOptionSubmitRetryCode: 2500`<br>`SMSOptionMTRetryPolicy: 1S,1S,1S` |
| V917 | 无（**没有重试码**：发送失败不改道） |
| V918 | 同上面的两行，再加 `SMSSpecialSupportContentLengthByCountry: (prefix=86,value=50::deny)`（发往 86 的内容超过 50 字节不能用这个通道） |

`SMSOptionMTRetryPolicy: 1S,1S,1S`：级别用完后按 1 秒的间隔重试主路由。V999 **不在 cm.cfg 里**，用来测“通道不存在”。

### 3.3 GmmsConfig 与其他

| 文件 | 内容 | 作用 |
|---|---|---|
| `Gmms/GmmsConfig.properties` | 追加 `AsyncSqlWriterEnable=false`、`DB.RoutingLevelColumn=true` | SQL 同步执行，便于检查；RMQ 记 `RoutingLevel` 列 |
| `conf/vendorRoutingReplacement.cfg` | `86:958:921:922` | 客户 958 发往 86 时，通道 921 换成 922（4.7） |
| 发送方黑白名单 | 不配 | `conf/senderactiondir` 是空目录；客户都是默认的 NonPA，发往 86、77、852、44 不受发送方限制 |

## 4. 用例

### 4.1 路由文件的读取和检查（单元，不启动 Core）

**做法**：把下面的内容写进一个临时的 `_routingInfo.cfg`，按 Core 的方式解析（客户 ssid 9999，通道名 `V数字` 换成 ssid 数字）。

**能读进来的文件**：

```
CCBSSID: 9999
A2PMainRouting: (Priority=10,PhonePrefix=86;852,Sender=BANK;OTP,ExcludeKeywordId=3,Relay=V1:70,V2:30);(Priority=50,PhonePrefix=86,Relay=V3)
A2PBackupRouting: (Priority=10,PhonePrefix=all,Relay=V4)
A2PBackup10Routing: (Priority=10,PhonePrefix=86,Relay=V5)
A2POPRouting: (Priority=10,Opid=860001,Relay=V6)
A2PNumberRouting: (Priority=1,Number=8613800000000,Relay=V7)
A2PRoutingRelay: (PhonePrefix=86,Relay=V8)
A2PBackup11Routing: (Priority=1,PhonePrefix=86,Relay=V9)
```

| ID | 目的 | 预期 |
|---|---|---|
| RR-A01 | 各层和备份 1 级、10 级都能读 | 6 条规则（主路由 2 条，备份 1 级、10 级、运营商、号码各 1 条） |
| RR-A15 | 旧配置名、11 级备份 | `A2PRoutingRelay`、`A2PBackup11Routing` 告警，不生效 |

**写错的规则**（每个都单独写成一个文件 `CCBSSID: 9999` + 这一行）：

| ID | 写法 | 预期 |
|---|---|---|
| RR-A-05 | `A2PMainRouting: (PhonePrefix=86,Relay=V1)`（没有 Priority） | 拒绝，给出原因 |
| RR-A-06 | `(Priority=0,...)`、`(Priority=101,...)` | 拒绝（范围 1～100） |
| RR-A-07 | `A2PMainRouting: (Priority=10,PhonePrefix=86)`（没有 Relay） | 拒绝 |
| RR-A-08 | 同一个前缀 86 下两条 `Priority=10` | 拒绝 |
| RR-A-11 | `Sender=BANK,ExcludeSender=bank` | 拒绝（同一发送方既要又排除） |
| RR-A-12 | `PhonePrefix=86,ExcludePhonePrefix=852` | 拒绝（排除的号段要在 PhonePrefix 之下） |
| RR-number rule without Number | `A2PNumberRouting: (Priority=10,Relay=V1)` | 拒绝 |

| ID | 写法 | 预期 |
|---|---|---|
| RR-A09 | 不同前缀（7、77）或不同级别各用 `Priority=1` | 可以（优先级只在同一分组里不能重复） |

### 4.2 记录字段（单元）

| ID | 目的 | 做法 | 预期 |
|---|---|---|---|
| RR-P01 | CDR 的 MsgID | 一条 MT（MsgID `CDRM1`），按发过的通道和 CDR 类型取 MsgID | 只发过一个通道：`CDRM1`；备份通道 913：`CDRM1-913`；又回到主路由通道（重试）：`CDRM1`；IN_DELI_REP：`CDRM1` |
| RR-N01 | 消息带着路由级别和规则 | `routingLevel=2`、`routingRule=op|main:2:86:10` 写进 Redis 流再读回 | 两个值不变（消息的第 62、63 段） |

### 4.3 主路由的选择

**配置**

cm.cfg：`[C951]`，CCBSSID 951，没有额外配置。

`conf/routing/C951_routingInfo.cfg`：

```
CCBSSID: 951
A2PMainRouting: (Priority=1,PhonePrefix=7,Relay=V901);(Priority=2,PhonePrefix=77,Sender=BANK,Relay=V902);(Priority=5,PhonePrefix=77,Relay=V903);(Priority=9,PhonePrefix=all,Relay=V909);(Priority=10,PhonePrefix=86,ExcludeSender=BANK;OTP,Relay=V904);(Priority=20,PhonePrefix=86,Relay=V905);(Priority=10,PhonePrefix=8613,ExcludeSender=PAY,Relay=V906);(Priority=10,PhonePrefix=852,ExcludePhonePrefix=8529,Relay=V907)
A2PNumberRouting: (Priority=1,Number=8613800000077,Relay=V908);(Priority=1,Number=8613800000078,Relay=V999)
```

按分组整理：

| 层 | 分组 | 规则 |
|---|---|---|
| 号码 | 8613800000077 | P1 → V908 |
| 号码 | 8613800000078 | P1 → V999（不在 cm.cfg） |
| 普通 | 8613 | P10 排除发送方 PAY → V906 |
| 普通 | 852 | P10 排除号段 8529 → V907 |
| 普通 | 86 | P10 排除发送方 BANK、OTP → V904；P20 → V905 |
| 普通 | 77 | P2 发送方 BANK → V902；P5 → V903 |
| 普通 | 7 | P1 → V901 |
| 普通 | all | P9 → V909 |

C951 没有备份路由。

**用例**（客户 C951）

| ID | 目的 | 发送方 / 接收号码 | 预期 | 依据 |
|---|---|---|---|---|
| RR-E01 | 77 分组里的发送方规则 | BANK / 77012345678 | V902 | 77 分组 P2 |
| RR-E02 | 77 分组的普通规则 | OTP / 77012345678 | V903 | P2 不满足，P5 |
| RR-F02 | `Sender` 不区分大小写 | bank / 77012345678 | V902 | |
| RR-E03 | 没有 77 时用更短的前缀 | OTP / 70012345678 | V901 | 7 分组；P1 虽然更小，但只在 7 分组里比 |
| RR-E04 | 都不匹配时用 all | OTP / 44012345678 | V909 | |
| RR-G01a | 排除发送方：不在排除列表里 | PAY / 8620000000000 | V904 | 86 P10 |
| RR-G01b | 排除发送方：被排除 | BANK / 8620000000000 | V905 | 跳过 P10，用 86 的下一条 P20 |
| RR-G02a | 8613 分组 | OTP / 8613800000001 | V906 | |
| RR-G02b | 8613 里被排除，退到更短的前缀 | PAY / 8613800000001 | V904 | 8613 没有规则命中，到 86 分组；PAY 不在 86 P10 的排除列表里 |
| RR-G07a | 排除号段 | OTP / 85291234567 | V909 | 8529 被 852 规则排除，852 分组没有其他规则，到 all |
| RR-G07b | 852 普通号码 | OTP / 85212345678 | V907 | |
| RR-B02 | 号码层在前缀之前 | BANK / 8613800000077 | V908 | 号码层 |
| RR-I01 | 规则指向不在 cm.cfg 的通道，又没有备份 | OTP / 8613800000078 | 路由失败，回失败 DR | 从备份找（没有），不再看其他主路由规则 |

### 4.4 Redis 里的生效规则

**做法**：调用 `loadRoutingInfoToRedis("CoreEngine1")`，读 Redis 哈希 `routingInfoCoreEngine1.Rules:{客户块名}`（《路由配置说明》7.1）。字段名是 `层:级别:分组:优先级`，值是规则的 JSON。

| ID | 看哪里 | 预期 |
|---|---|---|
| RR-Q09a | `Rules:C951` | `_file` 是 `CCBSSID=951`；字段 `main:0:77:2` 的 `Relay` 是 `902`（ssid）、`Sender` 是 `bank`（转成小写）、`Source` 以 `C951 A2PMainRouting` 开头 |
| RR-Q09b | `Rules:C951` | 没有 `main:0:86;852:10` 这种合并的分组（每个前缀各自一个分组）；`number:0:8613800000078:1` 的 `Relay` 是 `-1`（通道不在 cm.cfg） |
| RR-Q09c | `Rules:C954`（配置见 4.6） | `main:2:86:10` 的 `Relay` 是 `914:50,915:50`（备份 2 级，带权重） |
| RR-Q09d | `Rules:C963`（配置见 4.5） | `_warnings` 里有 `A2PRoutingRelay`，旧配置没有生成规则 |
| RR-Q09e | `Rules:SVC9_default` | `_file` 是 `default of service type SVC9` |

### 4.5 选路时就用备份

**配置**

cm.cfg：

| 块名 | CCBSSID | 额外配置 |
|---|---|---|
| C952 | 952 | `CustomerServiceType: SVC9` |
| C961 | 961 | — |
| C962 | 962 | — |
| C963 | 963 | — |

routing：

```
# C961_routingInfo.cfg   主路由通道 V918 限制 86 的内容长度（3.2）
CCBSSID: 961
A2PMainRouting: (Priority=10,PhonePrefix=86,Relay=V918)
A2PBackupRouting: (Priority=10,PhonePrefix=86,Relay=V919)

# C962_routingInfo.cfg   主路由通道不在 cm.cfg
CCBSSID: 962
A2PMainRouting: (Priority=10,PhonePrefix=86,Relay=V999)
A2PBackupRouting: (Priority=10,PhonePrefix=86,Relay=V919)

# C963_routingInfo.cfg   主路由没有 852；还有一行旧配置名
CCBSSID: 963
A2PMainRouting: (Priority=10,PhonePrefix=86,Relay=V911)
A2PBackupRouting: (Priority=10,PhonePrefix=852,Relay=V919)
A2PRoutingRelay: (PhonePrefix=86,Relay=V901)

# SVC9_default_routingInfo.cfg   业务类型 SVC9 的默认路由（C952 没有自己的文件）
A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V910)
```

**用例**

| ID | 目的 | 客户 | 输入 | 预期 | 依据 |
|---|---|---|---|---|---|
| RR-I02a | 内容长度允许：用主路由 | C961 | 内容 `short` | V918 | |
| RR-I02b | 内容超过主路由通道的限制：选路时就用备份 1 级 | C961 | 内容 `this text is longer than the fifty bytes channel V918 takes`（超过 50 字节） | V919 | 《路由配置说明》3.3、例 8 |
| RR-I02c | 记录了级别 | 同上 | — | 出向流里这条 MT 的 `routingLevel` 是 1 | |
| RR-I01b | 主路由通道不在 cm.cfg：备份 1 级 | C962 | — | V919 | |
| RR-I06b | 主路由没有 852 的规则：备份 1 级 | C963 | 接收 85212345678 | V919 | |
| RR-D-default | 没有自己的路由文件：用业务类型的默认路由 | C952 | — | V910 | |
| RR-I06 | 主路由、备份、默认路由都没有：路由失败 | C952 | 接收 44012345678 | 不路由，回失败 DR | |

### 4.6 发送失败后逐级改道

**配置**

cm.cfg：`[C954]`，CCBSSID 954，没有额外配置。通道的重试码见 3.2（V911～V916 有 `SMSOptionSubmitRetryCode: 2500`）。

`conf/routing/C954_routingInfo.cfg`：

```
CCBSSID: 954
A2PMainRouting: (Priority=10,PhonePrefix=86,Relay=V911)
A2PBackupRouting: (Priority=10,PhonePrefix=86,Sender=BANK,Relay=V912);(Priority=20,PhonePrefix=86,Relay=V913)
A2PBackup2Routing: (Priority=10,PhonePrefix=86,Relay=V914:50,V915:50)
A2PBackup3Routing: (Priority=10,PhonePrefix=all,Relay=V916)
```

| 级别 | 规则 |
|---|---|
| 主路由（0） | V911 |
| 备份 1 | 发送方 BANK → V912；其他 → V913 |
| 备份 2 | V914 / V915 各 50% |
| 备份 3 | V916 |

**用例 A：一条 MT 走完所有级别**（客户 C954，发送方 OTP，接收 8613800000000。下面每一步都接着上一步）

| ID | 步骤 | 预期 |
|---|---|---|
| RR-Q05 | 发出 | V911；`routingLevel` 0，`routingRule` 以 `main|main:0:86:10` 开头 |
| RR-J01 | V911 失败（2500） | 改道到备份 1 级的 V913（OTP 不满足 P10，用 P20）；`routingLevel` 1；**不回 DR** |
| RR-J03 | V913 失败（2500） | 备份 2 级：V914 或 V915，`routingLevel` 2 |
| RR-J04 | 这个通道失败（2500） | 备份 3 级 V916，`routingLevel` 3；备份 2 级的**另一个通道不会再试** |
| RR-K05a | V916 失败（2500） | 级别用完：写 RMQ 等重试 |
| RR-K05b | 看 RMQ 的 SQL | `RSsID=911`（重试主路由的通道），`RoutingLevel=3` |
| RR-K05c | MQM 重发（模拟） | 写进 MQM 流成功 |
| RR-K05d | Core 处理重发 | 又发到 V911（这条 MT 第 2 次出现在 V911 的出向流） |
| RR-K06 | V911 又失败（2500） | 不再走备份（V913 只出现过 1 次）；更新 RMQ 的那一行（UPDATE）等下一次重试；日志 `no backup level after level 3 (rssid=911` |
| RR-P07 | 看 CDR | OUT_SUBMIT 的 MsgID：主路由两次（首次和重试）是原 MsgID；备份是 `MsgID-913`、`MsgID-916` 各 1 条 |

**用例 B：备份按自己的规则选**

| ID | 输入 | 步骤 | 预期 |
|---|---|---|---|
| RR-J02 | 发送方 BANK | V911 失败（2500） | 备份 1 级的 V912（P10 发送方 BANK） |

### 4.7 重试码、改道次数、系统路由替换、备份通道不能用

**配置**

cm.cfg：

| 块名 | CCBSSID | 额外配置 |
|---|---|---|
| C955 | 955 | — |
| C956 | 956 | — |
| C957 | 957 | `SMSOptionMaxReroute: 1` |
| C958 | 958 | — |
| C959 | 959 | — |

routing：

```
# C955：主路由通道 V917 没有重试码
CCBSSID: 955
A2PMainRouting: (Priority=10,PhonePrefix=86,Relay=V917)
A2PBackupRouting: (Priority=10,PhonePrefix=86,Relay=V913)

# C956：备份 1 级的 V917 没有重试码
CCBSSID: 956
A2PMainRouting: (Priority=10,PhonePrefix=86,Relay=V911)
A2PBackupRouting: (Priority=10,PhonePrefix=86,Relay=V917)
A2PBackup2Routing: (Priority=10,PhonePrefix=86,Relay=V913)

# C957：只允许改道 1 次
CCBSSID: 957
A2PMainRouting: (Priority=10,PhonePrefix=86,Relay=V911)
A2PBackupRouting: (Priority=10,PhonePrefix=86,Relay=V912)
A2PBackup2Routing: (Priority=10,PhonePrefix=86,Relay=V913)

# C958：主路由和备份 1 级都是 V921，被系统路由替换成 V922
CCBSSID: 958
A2PMainRouting: (Priority=10,PhonePrefix=86,Relay=V921)
A2PBackupRouting: (Priority=10,PhonePrefix=86,Relay=V921)
A2PBackup2Routing: (Priority=10,PhonePrefix=86,Relay=V923)

# C959：备份 1 级的通道不在 cm.cfg
CCBSSID: 959
A2PMainRouting: (Priority=10,PhonePrefix=86,Relay=V911)
A2PBackupRouting: (Priority=10,PhonePrefix=86,Relay=V999)
A2PBackup2Routing: (Priority=10,PhonePrefix=86,Relay=V913)
```

`conf/vendorRoutingReplacement.cfg`：`86:958:921:922`（客户 958、前缀 86：通道 921 → 922）。

**用例**（发送方 OTP，接收 8613800000000）

| ID | 目的 | 客户 | 步骤 | 预期 |
|---|---|---|---|---|
| RR-K04 | 发送通道没有重试码：不改道 | C955 | V917 失败（2500） | 回失败 DR；没有改道到 V913 |
| RR-K02 | 备份通道的失败码不在它的重试码里：不再往下 | C956 | V911 失败（2500）→ V917 → V917 失败（2500） | 回失败 DR；没有到备份 2 级的 V913 |
| RR-K01 | 失败码不在重试码里：不改道 | C954 | V911 失败，状态 **2700** | 回失败 DR；没有改道到 V913 |
| RR-J15 | `SMSOptionMaxReroute: 1` | C957 | V911 失败（2500）→ V912 → V912 失败（2500） | 只改道 1 次：不到 V913，写 RMQ 重试主路由（`RSsID=911`） |
| RR-M01 | 系统路由替换主路由 | C958 | 发出 | V922（不是 V921） |
| RR-M03 | 备份替换后是已经发过的通道：跳过这一级 | C958 | V922 失败（2500） | 备份 1 级 V921 → 替换成 V922，已经发过，跳到备份 2 级 V923；V922 只出现过 1 次 |
| RR-J09 | 备份通道不在 cm.cfg：跳到下一级 | C959 | V911 失败（2500） | 备份 2 级 V913 |

## 5. 最近一次结果

| 时间 | 环境 | 结果 |
|---|---|---|
| 2026-11-26 | 测试服务器全量回归（提交 `79173c7` 的代码） | 56 项全部通过 |

## 6. 没有覆盖到的

| 内容 | 说明 |
|---|---|
| 运营商层的选路、二次路由、`Relay=V1,V2` 轮流 | 2026-11-27 补在《路由补充测试（RoutingExtraIT）》（ENUM 用模拟程序）；发现运营商通道发送失败后不走运营商层的备份（OP-06） |
| DR 失败后改道（`NeedDRReroute`、`SMSOptionDRRerouteCode`） | 在 CoreDrBizIT（DR 改道） |
| 排除关键字（`ExcludeKeywordId`）、修改 `vendorRoutingReplacement.cfg` 后 `RELOAD_SYSTEM_REPLACE` 生效 | 2026-11-27 补在《路由补充测试（RoutingExtraIT）》，都通过 |
