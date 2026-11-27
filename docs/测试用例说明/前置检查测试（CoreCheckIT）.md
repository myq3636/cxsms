# 前置检查测试（CoreCheckIT）

> 日期：2026-11-27。用例代码：`regression/test/it/CoreCheckIT.java`，套件 `CoreCheckIT`（`regression/suites.txt`，it 模式），51 项检查。
> 功能清单见《Core模块功能与测试用例清单》第二节“前置检查”；配置项见《配置项说明》。按国家的 PA / NonPA、号码 24 小时上限的按国家设置，另见《号码限额与发送方检查测试说明》（FunctionSyncIT）。

## 1. 测什么

Core 收到客户的 MT 后、路由之前做的检查。不通过的 MT 不路由，写 IN_SUBMIT CDR；客户要 DR 时回一个失败 DR：

| 检查 | 配置 |
|---|---|
| 发送方黑名单、白名单（全局和按客户、按国家），VIP / PA / NonPA | `conf/senderactiondir/`，客户的 `CustomerTypeBySender` |
| 内容黑名单（全局和按客户） | `conf/contentactiondir/` |
| 接收号码：必须是数字、客户允许的前缀和长度、全局接收号码规则 | 客户的 `PhonePrefix`、`SMSOptionRecNumberLenList`；`conf/recipientRuleFile.cfg` |
| 发送方号码：允许的前缀、最小长度 | `SMSOptionAllowOriPrefixList`、`SMSOptionOriMinNumberLen` |
| 单条消息长度 | `NeedCheckMsgSize` |
| 同一号码 24 小时上限 | `SMSOptionRecipientMaxSendCountIn24H` |
| 重复消息 | `DuplicateMessagePeriod`、`DuplicateMessageServiceTypeIDList`、`CheckDuplicateMessageContent` |

## 2. 怎么跑、怎么判定

### 2.1 运行

```
regression/run.sh CoreCheckIT
cat regression/.work/results/CoreCheckIT.txt
```

- 环境：测试服务器上的回归环境（JDK 17，本地 Redis 16379），进程内启动真实的 Core（`MTStreamConsumer`），不需要数据库。
- 用例启动前，自己写入第 3、4 节的配置。

### 2.2 怎么判定

- **发送**：模拟 SMPP Server，把 MT 写进 `stream:mt:pending:{ossid}:{分片}`，类型 Submit，要 DR。默认发送方 `18667991226`，接收号码 `8613800000000`，内容 `hello`；表里的“输入”只写和默认不同的字段。
- **结果**：

| 结果 | 怎么判断 |
|---|---|
| 通过检查（路由出去） | 出现在通道 V401 的出向流 `stream:mt:routed:401:*`，客户的 DR 流里没有它 |
| 被拒绝 | 出向流里没有；客户的 DR 流 `stream:dr:pending:{ossid}:*` 里有它的失败 DR，表里写了 DR 的状态码 |
| CDR | IN_SUBMIT CDR 的条数（拒绝的也写 1 条） |
| 已确认 | 日志里有这条 MT 的 `[STREAM-ACK] flow=server->core`（拒绝的也确认，不会重复消费） |

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

| 块 | CCBSSID | 额外配置 | 用在 |
|---|---|---|---|
| V401 | 401 | — | 唯一的通道 |
| C300 | 300 | — | 普通客户（默认 NonPA） |
| C301 | 301 | — | 有自己的发送方黑名单 |
| C302 | 302 | `CustomerTypeBySender: VIP` | VIP |
| C303 | 303 | — | 有自己的发送方白名单（86、65） |
| C304 | 304 | — | 有自己的内容黑名单 |
| C305 | 305 | `SMSOptionAllowOriPrefixList: ^1866.*` | 发送方前缀 |
| C306 | 306 | `SMSOptionOriMinNumberLen: 8` | 发送方最小长度 |
| C307 | 307 | `SMSOptionRecipientMaxSendCountIn24H: 2` | 同一号码 24 小时上限 |
| C308 | 308 | `DuplicateMessagePeriod: 60`<br>`DuplicateMessageServiceTypeIDList: 0`<br>`CheckDuplicateMessageContent: true` | 重复消息 |
| C309 | 309 | `PhonePrefix: (Prefix=86)`<br>`SMSOptionRecNumberLenList: 13` | 接收号码前缀和长度 |
| C310 | 310 | `NeedCheckMsgSize: false` | 不检查长度 |
| C311 | 311 | — | （未用） |
| C312 | 312 | — | 有自己的 65 白名单和黑名单 |
| C313 | 313 | `CustomerTypeBySender: PA` | PA |
| C314 | 314 | — | 白名单只有 86 |
| C315 | 315 | — | 65 的替换（rp）规则 |
| C316 | 316 | — | 白名单写 `all`（所有国家） |

### 3.2 路由

每个客户一个路由文件，内容相同（CCBSSID 各自的），都路由到 V401，所以“路由出去”就等于“通过了检查”：

```
CCBSSID: 300
A2PMainRouting: (Priority=30,PhonePrefix=86;852;65,Relay=V401)
```

### 3.3 发送方名单（`conf/senderactiondir/`）

文件名 `{ssid}_sender_blacklist.cfg`、`{ssid}_sender_whitelist.cfg`，`-1` 是全局（默认）的。

- 黑名单一行一条：`国家前缀:发送方`。
- 白名单一行一条：`国家前缀:wl:发送方:x`（放行），或 `国家前缀:rp:发送方:替换成`（替换后放行）；国家前缀写 `all` 是所有国家。
- 发送方和条目完全相等才算命中（区分大小写，特殊字符、空格按原样比较）。

| 文件 | 内容 |
|---|---|
| `-1_sender_blacklist.cfg` | `86:GLOBALBAD`<br>`65:SGBAD` |
| `301_sender_blacklist.cfg` | `86:CUSTBAD`<br>`65:CUSTBAD65` |
| `-1_sender_whitelist.cfg` | `65:wl:DEFOK:x` |
| `312_sender_whitelist.cfg` | `65:wl:GOODSG:x`<br>`65:wl:SGBAD:x`<br>`65:wl:CUST312:x` |
| `312_sender_blacklist.cfg` | `65:CUST312` |
| `313_sender_blacklist.cfg` | `65:PABAD` |
| `314_sender_whitelist.cfg` | `86:wl:ONLY86:x` |
| `315_sender_whitelist.cfg` | `65:rp:RPOLD:RPNEW` |
| `316_sender_whitelist.cfg` | `all:wl:ANYCC:x` |
| `303_sender_whitelist.cfg` | `86:wl:GOODONE:x`<br>`65:wl:GOODSG:x` |

### 3.4 内容黑名单（`conf/contentactiondir/`）

一行一条 `国家前缀:词`，内容里**包含**这个词就命中（区分大小写）。

| 文件 | 内容 |
|---|---|
| `-1_content_blacklist.cfg` | `86:globalforbidden` |
| `304_content_blacklist.cfg` | `86:custforbidden` |

### 3.5 接收号码规则（`conf/recipientRuleFile.cfg`）

JSON 数组，每个元素是一个国家的规则：`cc` 国家码，`lens` 允许的号码长度，`allowPrefixList` 允许的前缀；另外还可以写 `formatRegex`（格式正则）、`blackPrefixList`（禁止的前缀）。只检查以 `cc` 开头的号码。

```json
[
{"cc":"86","lens":[13],"allowPrefixList":["8613","8615"]}
]
```

即发往 86 的号码必须是 13 位，并且以 8613 或 8615 开头。

## 4. 用例

### 4.1 发送方黑名单、VIP

| ID | 目的 | 客户 | 输入 | 预期 |
|---|---|---|---|---|
| CK-00 | 正常的 MT | C300 | 默认 | 路由出去，1 条 IN_SUBMIT CDR，已确认 |
| CK-01a | 全局黑名单 | C300 | 发送方 `GLOBALBAD` | 拒绝，DR **10801**（`REJECTED_SDBL`），1 条 CDR，已确认 |
| CK-01b | 名单按国家 | C300 | 发送方 `GLOBALBAD`，接收 `85212345678` | 路由出去（黑名单只配了 86） |
| CK-02a | 客户自己的黑名单 | C301 | 发送方 `CUSTBAD` | 拒绝，10801 |
| CK-02b | 只对这个客户生效 | C300 | 发送方 `CUSTBAD` | 路由出去 |
| CK-03 | VIP 不查发送方名单 | C302 | 发送方 `GLOBALBAD` | 路由出去 |

### 4.2 发往新加坡（65）：NonPA、PA、VIP

客户没配 `SMSOptionCustomerTypeCountry` 时，`CustomerTypeBySender` 只对 65 生效（默认 NonPA）。规则：

- **NonPA**：黑名单只查全局的，白名单只查客户自己的，没命中就拒绝，不看默认白名单。
- **PA**：黑名单查客户的和全局的；白名单先查客户自己的，没有再查默认的。

| ID | 目的 | 客户 | 发送方 / 接收号码 | 预期 |
|---|---|---|---|---|
| CK-04a | NonPA、没有自己的白名单 | C300 | `18667991226` / `6591234567` | 拒绝，DR **10802**（`REJECTED_SDWL`） |
| CK-04b | VIP 发新加坡 | C302 | `18667991226` / `6591234567` | 路由出去 |
| CK-04c | NonPA 不查客户自己的黑名单，由白名单决定 | C301 | `CUSTBAD65` / `6591234567` | 拒绝 10802（不是 10801） |
| CK-04d | 号码写成 `+65…`、`0065…`、`065…` | C300 | `18667991226` / 三种写法 | 都按 65 处理，拒绝 10802（3 项） |
| CK-04e | 65 只在号码中间 | C300 | `18667991226` / `8613650000000` | 路由出去（不是发往新加坡） |
| CK-04f | NonPA 不看默认白名单 | C300 | `DEFOK` / `6591234567` | 拒绝 10802（`DEFOK` 只在默认白名单里） |
| CK-04g | NonPA，发送方在自己的 65 白名单里 | C312 | `GOODSG` / `6591234567` | 路由出去 |
| CK-04h | NonPA，自己的 65 白名单里没有这个发送方 | C312 | `OTHER` / `6591234567` | 拒绝 10802 |
| CK-04i | NonPA 不查客户黑名单：`CUST312` 在它的黑名单里，也在白名单里 | C312 | `CUST312` / `6591234567` | 路由出去 |
| CK-04j | 全局黑名单优先于白名单 | C312 | `SGBAD` / `6591234567` | 拒绝 10801（`SGBAD` 在全局黑名单，也在 C312 的白名单） |
| CK-04k | NonPA，白名单只有 86 | C314 | `ONLY86` / `6591234567` | 拒绝 10802 |
| CK-04l | 同一客户、同一发送方发 86 | C314 | `ONLY86` / 默认 | 路由出去 |
| CK-04m | 替换（rp） | C315 | `RPOLD` / `6591234567` | 路由出去，发出的发送方是 `RPNEW` |
| CK-04n | 白名单 `all`：所有国家 | C316 | `ANYCC` / `6591234567` | 路由出去 |
| CK-04o | 白名单 `all`，发送方不在里面 | C316 | `OTHER` / `6591234567` | 拒绝 10802 |
| CK-04p | PA、没有自己的白名单：由默认白名单决定 | C313 | `18667991226` / `6591234567` | 拒绝 10802 |
| CK-04q | PA，发送方在默认白名单里 | C313 | `DEFOK` / `6591234567` | 路由出去 |
| CK-04r | PA 要查客户自己的黑名单 | C313 | `PABAD` / `6591234567` | 拒绝 10801 |

### 4.3 发送方白名单（发往其他国家）

| ID | 目的 | 客户 | 发送方 / 接收号码 | 预期 |
|---|---|---|---|---|
| CK-05a | 65 的白名单 | C303 | `GOODSG` / `6591234567` | 路由出去 |
| CK-05b | 86 的白名单里有这个发送方 | C303 | `GOODONE` / 默认 | 路由出去 |
| CK-05c | 客户有 86 的白名单，发送方不在里面 | C303 | `OTHER` / 默认 | 拒绝 10802 |

### 4.4 内容黑名单

| ID | 目的 | 客户 | 内容 | 预期 |
|---|---|---|---|---|
| CK-06a | 全局内容黑名单 | C300 | `this is globalforbidden text` | 拒绝，DR **10803**（`REJECTED_CTBL`） |
| CK-06b | 客户自己的内容黑名单 | C304 | `this is custforbidden text` | 拒绝 10803 |
| CK-06c | 只对这个客户生效 | C300 | `this is custforbidden text` | 路由出去 |

### 4.5 不要 DR 的 MT 被拒绝

| ID | 目的 | 客户 | 输入 | 预期 |
|---|---|---|---|---|
| CK-07 | 被拒绝但客户没要 DR | C300 | 发送方 `GLOBALBAD`，`registered_delivery=0` | 不路由，**不回 DR**，只写 1 条 IN_SUBMIT CDR |

### 4.6 接收号码、发送方号码

| ID | 目的 | 客户 | 输入 | 预期 |
|---|---|---|---|---|
| CK-08 | 接收号码不是数字 | C300 | 接收 `abc123` | 不路由，DR **10402**（`UNDELIVERY_RECIPIENT_ERROR`） |
| CK-09a | 客户的前缀和长度都符合 | C309 | 默认（`8613800000000`，13 位） | 路由出去 |
| CK-09b | 长度不对 | C309 | 接收 `86138000`（8 位） | 不路由，10402 |
| CK-09c | 前缀不在 `PhonePrefix` 里 | C309 | 接收 `8521234567890` | 不路由，10402 |
| CK-10a | 发送方符合 `^1866.*` | C305 | 默认发送方 `18667991226` | 路由出去 |
| CK-10b | 发送方不符合 | C305 | 发送方 `13900000000` | 不路由，DR **10404**（`UNDELIVERY_SENDER_ERROR`） |
| CK-10c | 发送方短于 8 位 | C306 | 发送方 `1234567` | 不路由，10404 |
| CK-11a | 前缀不在 `recipientRuleFile` 允许的列表里 | C300 | 接收 `8619900000000` | 不路由，10402 |
| CK-11b | 长度不符合 `recipientRuleFile` | C300 | 接收 `86138000000`（11 位） | 不路由，10402 |

### 4.7 消息长度

| ID | 目的 | 客户 | 输入 | 预期 |
|---|---|---|---|---|
| CK-12a | 非 ASCII 超过 140 字节 | C300 | `UnicodeBigUnmarked`，141 字节 | 不路由，DR **10401**（`UNDELIVERY_MSG_FORMAT_ERROR`；提交状态 2250） |
| CK-12b | 正好 140 字节 | C300 | `UnicodeBigUnmarked`，140 字节 | 路由出去 |
| CK-12c | `NeedCheckMsgSize: false` | C310 | `UnicodeBigUnmarked`，141 字节 | 路由出去 |

### 4.8 同一号码 24 小时上限

| ID | 目的 | 客户 | 输入 | 预期 |
|---|---|---|---|---|
| CK-13a | 上限 2：前两条 | C307 | 接收 `8613900000013`，发 2 条 | 都路由出去 |
| CK-13b | 第 3 条 | C307 | 同一号码再发 1 条 | 不路由，DR **10403**（`UNDELIVERY_DAILY_LIMIT`；提交状态 2122） |
| CK-13c | 另一个号码不受影响 | C307 | 接收 `8613900000014` | 路由出去 |

计数器是 Redis 状态库里的 `Duplicate:{号码}`，24 小时过期，按号码计、不分客户。

### 4.9 重复消息

| ID | 目的 | 客户 | 输入 | 预期 |
|---|---|---|---|---|
| CK-14a | 第一条 | C308 | 接收 `8613900000020`，内容 `same` | 路由出去 |
| CK-14b | 60 秒内同一号码、同一内容 | C308 | 同上 | 不路由（重复），DR **10500**（`REJECTED`） |
| CK-15 | 内容不同 | C308 | 接收 `8613900000020`，内容 `different` | 路由出去（`CheckDuplicateMessageContent: true`：要内容也相同才算重复） |

## 5. 最近一次结果

| 时间 | 环境 | 结果 |
|---|---|---|
| 2026-11-26 | 测试服务器全量回归（提交 `79173c7`） | 51 项全部通过 |
| 2026-11-27 | 测试服务器全量回归（本次修改后） | 51 项全部通过；表里的 DR 状态码取自这次的实测输出 |

## 6. 没有覆盖到的

| 内容 | 说明 |
|---|---|
| 内容白名单（`_content_whitelist.cfg`） | 没有用例 |
| 接收方黑名单（`conf/recipientactiondir/`） | 没有用例 |
| 全局黑名单、白名单（`conf/blacklist.cfg`、`whitelist.cfg`） | 没有用例 |
| `recipientRuleFile` 的 `formatRegex`、`blackPrefixList` | 只测了 `lens` 和 `allowPrefixList` |
| 重复消息：超过 `DuplicateMessagePeriod` 后再发、不在 `DuplicateMessageServiceTypeIDList` 里的 service type | 没有用例 |
| 名单修改后 `RELOAD_SENDER_BLACKLIST` 等命令生效 | CoreContentIT 测了发送方黑名单的重新加载，其他名单没有 |
| 按国家的 PA / NonPA（`SMSOptionCustomerTypeCountry`）、按国家的号码上限、名单里的特殊字符和空格 | 在 FunctionSyncIT（《号码限额与发送方检查测试说明》） |
