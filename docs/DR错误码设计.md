# DR 错误码设计

> 日期：2026-11-11。状态：**已实现**（第 11 节是实现说明和测试结果）。第 9 节的问题已确认（2026-11-11），正文已按确认结果修改。第 1 节来自读当前 `main` 的代码（`Smpp.java` 的 DR 解析和生成、`GmmsStatus`、HTTP 接口的 `StatusMapping`）。

## 1. 现在的做法

### 1.1 通道 DR 进来（Client）

| 来源 | 读什么 | 怎么用 |
|---|---|---|
| SMPP 回执文本 `stat:XXXXXXX` | 7 个字符的状态词 | `getCodeFromState`：DELIVRD / UNDELIV / EXPIRED / REJECTD / DELETED / UNKNOWN / ACCEPTD → 内部状态 10000 / 10400 / 10200 / 10500 / 10300 / 10900 / 10001 |
| SMPP 回执文本 `err:XXX` | 3 个字符 | **只用来判断欠费**：在通道的 `SMSOptionDRNotPaidCode` 里就把状态改成 REJECTED_BYNC（10800），否则**丢掉** |
| SMPP TLV `network_error_code`（0x0423） | 3 字节 | 同上，只判断欠费，否则丢掉（打一行 INFO） |
| SMPP TLV `message_state`（`SMSOptionDRStatusIsOptionPara`） | 1～8 | 对应到上面几个内部状态 |
| HTTP 通道 DR | 接口 XML 里 `StatusMapping/DR` 配的字段 | 通道的值 → 内部状态 |

结论：**通道给的具体错误原因（err、network_error_code）除了欠费以外都没有保留**，CDR 里也看不到，只剩 7 个大类状态。

### 1.2 给客户的 DR（Server）

| 方向 | 怎么生成 err |
|---|---|
| SMPP 回执 `err:` | 客户配了 `SMSOptionSmppMapErrCod4DR=true` 时，按代码里写死的表从内部状态换算（`getErrCodFromStatusCode`：10000→000、10200→001、10400→003、10402→012、10405→016、10500→005、10800→008……），否则一律 `000` |
| SMPP 回执 `stat:` | 内部状态 → 状态词；`SMSOptionDRStatusMapping` 可以改显示的状态词 |
| HTTP 客户 | 接口 XML 里 `StatusMapping/DR`：内部状态 → 客户的 code / text |

结论：客户拿到的 err 只能区分十几种内部状态，看不到“号码不存在”“用户关机”“运营商拒收”之类的真实原因。

### 1.3 其他相关的

- 外发失败（submit_sm_resp 错误）转成的失败 DR：`GmmsStatus.SubmitConvertErrorDRStatus` 按失败文本换成 10401～10406。
- DEF-C5：DR 重试后，细分状态（10405、10402）不能从文本恢复，客户收到 UNKNOWN（`known-defects.txt` SD-03a/b）。
- 假 DR、方式 3 改写（`docs/假DR逻辑重新设计.md`）只改状态，不涉及 err。

## 2. 目标

1. 系统内部定义一套 **DR 错误码**，每个码有明确的含义和对应的 DR 状态。
2. 每个通道配置“**通道错误码 → 内部错误码**”的对照：通道 DR 里的 err / network_error_code / HTTP 字段 / submit_sm_resp 错误都能换成内部错误码。
3. 给客户的 DR（SMPP 的 `err:` 和 TLV，HTTP 的字段）里**带上错误码**：默认是内部错误码，客户可以配置自己的对照表。
4. 通道原始的错误码和内部错误码写进日志和消息字段，方便排查（CDR 不变）。

## 3. 内部错误码

**3 位数字（000～999）**，原因：SMPP 回执的 `err:` 字段按规范就是 3 位，内部码可以原样放进去，不用再换算；`network_error_code` TLV 也放得下。

| 段 | 含义 | DR 状态 | 例 |
|---|---|---|---|
| 000 | 成功 | DELIVRD | 000 已送达 |
| 001～099 | 中间状态 | ACCEPTD / ENROUTE | 001 已接受、002 投递中 |
| 100～199 | 号码问题 | UNDELIV | 101 号码不存在、102 号码格式错、103 用户停机、104 用户关机 / 不在服务区、105 号码已携出 |
| 200～299 | 网络、运营商 | UNDELIV | 201 网络故障、202 运营商拒绝、203 漫游不支持、204 短信中心忙 |
| 300～399 | 内容、发送方 | REJECTD | 301 内容含违禁词、302 发送方未报备、303 模板不匹配、304 长度超限 |
| 400～499 | 平台、策略拒绝 | REJECTD | 401 黑名单、402 反垃圾、403 发送频率超限、404 欠费（现在的 REJECTED_BYNC）、405 白名单不通过 |
| 500～599 | 过期、超时 | EXPIRED | 501 有效期内未送达 |
| 600～699 | 通道、系统错误 | UNDELIV | 601 通道提交失败、602 通道超时、603 通道限流 |
| 900～999 | 未知 | UNKNOWN | 999 未知原因 |

- 码表放在 `conf/drErrorCode.cfg`（新增），每个码一行：码、DR 状态、内部状态（`GmmsStatus`，比如 104 对应 10405）、英文说明、中文说明。上表是初始建议，具体码以这个文件为准；新增码只改配置，不改代码。
- 每个内部错误码对应一个 DR 状态和一个内部状态。现在的内部状态（10000、10400、10402……）保留，用来决定重试、改道、CDR 的状态列，和现在一样。
- 系统自己产生的 DR 也有内部错误码：Core 拒绝（黑名单 401、反垃圾 402……）、外发失败（按 submit 错误对照，见 4.2）、长短信过期（501）、假 DR 成功（000）、假 DR 失败（200，含方式 4 选 `fail`，5.3）。

## 4. 通道错误码 → 内部错误码

### 4.1 对照表

放在 `conf/drErrorMapping.cfg`（新增），按通道分块。格式、匹配顺序、例子见**第 10 节**。简单说：

- `DRNetworkErr`：`network_error_code` TLV（类型 + 值）→ 内部码；
- `DRErr`：回执的 `stat` 和 / 或 `err` → 内部码（只写 `stat` 的规则就是“这个状态的默认码”）；
- `SubmitErr`：submit_sm_resp 的 command_status（HTTP 通道是 submit 失败码）→ 内部码；
- 通道自己没配的，看 `[DEFAULT]` 块，再没有就按状态给系统默认码。

HTTP 通道用同样的块，`DRErr` 的 `Err` 填 HTTP DR 里错误码字段的值（字段名在接口 XML 里配，见 6.2）。

### 4.2 在哪里换

- **Client 只取原始值**：收到通道 DR 时，把原始的 `stat`、`err`、`network_error_code`（或 HTTP 字段）原样存进消息的新字段 `vendorErrorCode`（例：`stat=UNDELIV;err=001;nec=03:0001`）。Client 不做对照。
- **Core 换成内部码**：Core 处理通道 DR（`handleInboundDRInternal`，和匹配原消息同一处）时，按通道的对照表算出内部错误码，写进新字段 `drErrorCode`，并按码表设置内部状态（`GmmsStatus`）。现在 Client 上的欠费判断（cm.cfg 通道的 `SMSOptionDRNotPaidCode`）也挪到这一步，**保留这个配置，并且最先判断**：回执 `err` 或 `network_error_code`（6 位十六进制，如 `030001`）按原样在列表里，就是欠费（REJECTED_BYNC，码 404），不再看对照表。也可以不用这个配置，直接在对照表里写 `Code=404`。
- **外发失败**：Core 处理 submit 结果时，按 `SubmitErr` 把 submit_sm_resp 的 command_status 换成内部码（对不上用 601）。这条消息之后回给客户的失败 DR 就带这个码。现在按文本换算的 `SubmitConvertErrorDRStatus` 改成按内部码取内部状态。
- 放在 Core 的原因：对照表只需要 Core 加载、热加载；SMPP 和 HTTP 通道走同一段代码；Client 升级前后格式一致。

### 4.3 消息上的两个新字段

| 字段 | 内容 | 写在哪 |
|---|---|---|
| `vendorErrorCode` | 通道给的原始值（字符串） | Stream、Redis 的 DR 查找记录、DB 各队列表（不写 CDR） |
| `drErrorCode` | 内部错误码（3 位） | 同上 |

两个字段追加在 Stream 序列化格式的最后（第 58、59 段，和 `modulePath` 一样，旧版本读到会忽略），并写进 DB 的各个队列表（第 12 节）。

## 5. 内部错误码 → 客户

### 5.1 客户对照表

同样放在 `conf/drErrorMapping.cfg`，按客户分块（格式见**第 10 节**）：`CustomerErr` 把内部码（可以写范围）换成客户的码，没列出的按 `DRErrDefault` 处理：

| `DRErrDefault` | 客户收到的 err |
|---|---|
| `legacy`（默认） | 按现在的错误码表（`getErrCodFromStatusCode`，即现在 `SMSOptionSmppMapErrCod4DR=true` 时客户收到的码），由内部码对应的内部状态换算。客户收到的码和现在一样 |
| `passthrough` | 内部错误码（000～999） |
| `vendor` | 通道原始的 `err`（没有、或不是 1～3 位数字时退回内部码）。给要求“原样透传供应商错误码”的客户 |
| `000` | 永远 000（现在不配 `SMSOptionSmppMapErrCod4DR` 时的行为） |

### 5.2 放进 DR 的哪里

| 客户协议 | 位置 |
|---|---|
| SMPP | 回执文本 `err:XXX`（3 位，不足补 0）。客户配了 `SMSOptionDRNetworkErrorTLV=true`（新增）时，另外带 `network_error_code` TLV（类型 3 = SMPP，值 = 错误码） |
| HTTP | 接口 XML 的 DR 推送里新增变量 `errorCode`（和现有的 `statusCode`、`statusText` 一样配字段名）；客户的接口没配这个字段就不带 |

`stat:` 仍按内部错误码对应的 DR 状态生成，`SMSOptionDRStatusMapping` 照旧可以改显示的状态词。

### 5.3 几种 DR 给客户的码

| DR | 内部错误码 |
|---|---|
| 通道真实 DR | 4.2 换出来的码 |
| 假 DR 方式 1、2、4（成功） | 000（现在的表里 DELIVERED 对应 000） |
| 假 DR 失败（比例没命中、方式 4 选 `fail`） | 200（内部状态 UNDELIVERABLE 10400，现在的表里对应 003，默认客户收到的仍是 003） |
| 方式 3 把失败改成成功 | 000（`OUT_DELI_REP` 里仍是真实的码） |
| `SMSOptionSubmitErrorCodesForFakeDR` 改成功 | 000 |
| Core 拒绝 | 对应的 400 段（黑名单 401、反垃圾 402、欠费 404……） |
| 外发失败 | 4.2 的 `SubmitErr` 对照 |
| 过期 | 501 |

## 6. CDR、日志、配置加载

### 6.1 CDR

**CDR 不变（已确认）**。原始码和内部码只写日志（`[DR-ERR]`）和消息字段（Stream、Redis、DB），排查时按 msgId 查日志。

### 6.2 HTTP 接口 XML

- 通道侧（Client 解析通道 DR）：`DRRequest` 里新增 `<errorCode>字段名</errorCode>`，取到的值存进 `vendorErrorCode`。
- 客户侧（Server 推 DR）：同样新增 `<errorCode>`。

### 6.3 加载和热加载

- `conf/drErrorCode.cfg`、`conf/drErrorMapping.cfg` 由 Core（对照）和 Server（客户对照）加载；Client 不需要。
- 新命令 `RELOAD_DRERROR` 热加载这两个文件（已确认单独命令）；格式错误时拒绝、保持旧配置（和 `RELOAD_ROUTING` 一样）。
- 生效配置写进 Redis `config:files:*`，和其他配置文件一样。

### 6.4 日志和指标

- Core：`[DR-ERR] rssid=... vendor=stat=UNDELIV;err=001 code=101`。
- **对不上的通道错误码**：每个通道 + 原始码 10 分钟一条 WARN 汇总（`[DR-ERR-UNMAPPED] rssid=8 err=077 count=35`），并计入指标 `dr_err_unmapped`，方便运营补对照表。
- 指标：按通道、内部错误码统计 DR 数。

## 7. 去掉和替换的

| 现在 | 新设计 |
|---|---|
| `SMSOptionDRNotPaidCode`（cm.cfg 通道） | **保留**（2026-11-11 确认），判断从 Client 挪到 Core，优先级最高，命中后不再看对照表 |
| `SMSOptionSmppMapErrCod4DR`（cm.cfg 客户） | 不再使用，读到时打 WARN。现在的错误码表成为所有客户的默认（`DRErrDefault: legacy`）；**原来没开这个开关、一直收 `000` 的客户，升级后会收到表里的码**，要保持 `000` 的给它配 `DRErrDefault: 000` |
| `getErrCodFromStatusCode` 写死表 | 保留，作为 `legacy` 的换算表 |
| `Smpp.getCodeFromState` 在 Client 上定状态 | Client 只取原始值，Core 按对照表定（状态词 → 默认码见 4.1） |
| `SubmitConvertErrorDRStatus` 按文本换细分状态 | 按内部错误码取内部状态 |

## 8. 测试

真实 Client（`FakeSmsc` 发 deliver_sm）、真实 Core、真实 Server（测试进程扮演客户），延用 `PathChainIT` 的三进程做法，加上 Core 进程内的用例：

| 编号 | 场景 | 预期 |
|---|---|---|
| DE-01 | 回执 `stat:UNDELIV err:001`，通道对照 001→101 | Core 的 `drErrorCode`=101、内部状态 UNDELIV；客户收到 `err:101` |
| DE-02 | 同时带 `network_error_code`，有对照 | 用 TLV 的对照 |
| DE-03 | err 对不上 | 按 stat 的默认码；WARN 汇总和指标计数 |
| DE-04 | 欠费码 `Code=404` | 内部状态 REJECTED_BYNC，客户收到 404（或客户对照后的码） |
| DE-05 | 客户 `CustomerErr` 对照、`DRErrDefault` 四种取值、没有客户块 | 分别是对照码、旧表、内部码、通道原始 err、000；没有客户块时和现在的表一致 |
| DE-06 | 客户 `SMSOptionDRNetworkErrorTLV=true` | deliver_sm 带 TLV |
| DE-07 | HTTP 通道 DR 带错误码字段；HTTP 客户配了 `errorCode` | 两边都对 |
| DE-08 | submit_sm_resp 0x0B，`SubmitErr` 对照 102 | 客户的失败 DR 带 102 |
| DE-09 | 假 DR（成功 / 失败）、方式 3 改写、Core 拒绝、长短信过期 | 码按 5.3；默认客户收到的 err 和现在一样（假 DR 成功 000、失败 003） |
| DE-10 | 两个字段在 Stream 格式里 | 写入、读回一致 |
| DE-11 | CDR | 格式不变；`[DR-ERR]` 日志有原始码和内部码 |
| DE-12 | `RELOAD_DRERROR` 改对照表；写错格式 | 下一条 DR 生效；格式错时保持旧配置 |
| DE-13 | 旧配置 `SMSOptionSmppMapErrCod4DR` | 不生效，打 WARN；`SMSOptionDRNotPaidCode` 照旧生效且优先于对照表 |

## 9. 已确认（2026-11-11）

1. 内部错误码用 3 位。
2. 码表按第 3 节的分段。
3. 默认给客户返回错误码，**以现在的错误码表为默认**（`DRErrDefault: legacy`，5.1、10.5）。需要内部码的客户配 `passthrough`，需要一律 `000` 的配 `000`。
4. CDR 不加列（6.1）。
5. 单独的热加载命令 `RELOAD_DRERROR`（6.3）。
6. 假 DR 回现在对应的 err：成功 000，失败按 UNDELIVERABLE 对应的码（默认客户收到 003）（5.3）。
7. 对照表用单独的文件 `conf/drErrorMapping.cfg`。
8. DR 查询接口保持现状，不改。

**升级时要注意**：原来没开 `SMSOptionSmppMapErrCod4DR`、一直收 `err:000` 的 SMPP 客户，升级后失败的 DR 会收到表里的码（例如 UNDELIV 收到 003）。对接方只认 `000` 的客户，升级前在对照表里给它配 `DRErrDefault: 000`。

## 10. 两个配置文件的格式和匹配

### 10.1 `conf/drErrorCode.cfg`：内部错误码表

系统级，一个文件。一行一个码，`|` 分隔，`#` 开头是注释：

```text
# 码  | 状态词   | 内部状态 | 说明（英文）                 | 说明（中文）
000   | DELIVRD | 10000   | Delivered                    | 已送达
001   | ACCEPTD | 10001   | Accepted                     | 已接受
002   | ENROUTE | 10105   | Enroute                      | 投递中
101   | UNDELIV | 10402   | Unknown subscriber           | 号码不存在
102   | UNDELIV | 10402   | Invalid number format        | 号码格式错误
103   | UNDELIV | 10402   | Subscriber barred            | 用户停机 / 被禁
104   | UNDELIV | 10405   | Absent subscriber            | 用户关机 / 不在服务区
200   | UNDELIV | 10400   | Network error                | 网络 / 运营商错误
201   | UNDELIV | 10405   | Network failure              | 网络故障
202   | REJECTD | 10500   | Rejected by operator         | 运营商拒收
301   | REJECTD | 10503   | Content blocked              | 内容含违禁词
302   | REJECTD | 10404   | Sender not registered        | 发送方未报备
400   | REJECTD | 10500   | Rejected                     | 被拒绝
401   | REJECTD | 10502   | Recipient in blacklist       | 号码在黑名单
404   | REJECTD | 10800   | No credit                    | 欠费
501   | EXPIRED | 10200   | Expired                      | 有效期内未送达
601   | UNDELIV | 10400   | Submit failed                | 通道提交失败
999   | UNKNOWN | 10900   | Unknown                      | 未知
```

| 列 | 说明 |
|---|---|
| 码 | 3 位数字，不能重复 |
| 状态词 | 给客户的 SMPP 回执 `stat:` 用：DELIVRD / ACCEPTD / ENROUTE / UNDELIV / EXPIRED / REJECTD / DELETED / UNKNOWN |
| 内部状态 | 现有的 `GmmsStatus` 码。决定重试、改道（`SMSOptionDRRerouteCode`）、CDR 的状态列、HTTP 客户的 `StatusMapping`，和现在一样 |
| 说明 | 运营和对接文档用，不参与匹配 |

**加载时检查**（不通过就拒绝这次加载，保持旧表）：码是 3 位数字且不重复；状态词在上面的列表里；内部状态是存在的 `GmmsStatus`；**必须有** 000、001、002、200、400、501、999（系统默认码要用）。

### 10.2 `conf/drErrorMapping.cfg`：通道 / 客户对照表

写法和 cm.cfg 一样：`[shortName]` 开一个块，块里每个配置是一组 `(...)`，组之间 `;` 分隔。一个块要么是通道，要么是客户（按 shortName 在 cm.cfg 里的角色认，同一个 shortName 两种配置都写也可以，各用各的）。另有一个 `[DEFAULT]` 块，给没有单独配置的通道用（放各家通用的 GSM / SMPP 错误码）。

**通道块：**

```text
[VENDOR_X]
DRNetworkErr: (Type=3,Value=0001,Code=101);(Type=3,Value=001B,Code=104)
DRErr: (Stat=UNDELIV,Err=001,Code=101);(Stat=UNDELIV,Err=002~009,Code=104);(Err=088,Code=404);(Stat=UNDELIV,Code=200);(Stat=REJECTD,Code=202)
SubmitErr: (Status=0x0B,Code=102);(Status=0x58,Code=601);(Status=*,Code=601)
```

| 配置 | 属性 | 说明 |
|---|---|---|
| `DRNetworkErr` | `Type`、`Value`、`Code` | `network_error_code` TLV：`Type` 是第 1 字节（1 = ANSI-136，2 = IS-95，3 = GSM），`Value` 是后 2 字节，十六进制 4 位 |
| `DRErr` | `Stat`（可选）、`Err`（可选）、`Code` | 回执的 `stat`、`err`（HTTP 通道是接口 XML 里配的状态字段和错误码字段）。`Err` 可以写范围 `002~009`。两个都写 = 这个状态下的这个 err；只写 `Err` = 不管状态；只写 `Stat` = 这个状态下其他 err 都给这个码 |
| `SubmitErr` | `Status`、`Code` | submit_sm_resp 的 command_status（十进制，或 `0x` 开头的十六进制）；HTTP 通道是 submit 失败码。`Status=*` = 其他所有失败 |

**客户块：**

```text
[CUST_A]
CustomerErr: (Code=101,Err=011);(Code=104,Err=013);(Code=100~199,Err=003);(Code=404,Err=088)
DRErrDefault: passthrough       # 不写就是 legacy
DRErrTLV: true
```

| 配置 | 属性 | 说明 |
|---|---|---|
| `CustomerErr` | `Code`（可以写范围）、`Err` | 内部码 → 给客户的码。SMPP 客户的 `Err` 最多 3 位（加载时超过 3 位打 WARN，发送时截断）；HTTP 客户不限 |
| `DRErrDefault` | `legacy` / `passthrough` / `vendor` / `000` | `CustomerErr` 没列出的码怎么给（5.1），默认 `legacy` |
| `DRErrTLV` | `true` / `false` | SMPP 客户的 deliver_sm 另外带 `network_error_code` TLV（类型 3，值 = 给客户的码），默认 `false` |

### 10.3 取值的规范化（通道侧）

匹配前先把通道给的值整理一下，避免同一个码写法不同对不上：

| 值 | 规范化 |
|---|---|
| `stat` | 去空格、转大写；`REJECTED`→`REJECTD`、`DELIVERED`→`DELIVRD`、`UNDELIVERABLE`→`UNDELIV` 这类常见长写法换成标准的 7 个字符 |
| `err` | 去空格；**都是数字时按数值比较**（`1`、`01`、`001` 视为同一个）；不是数字时不区分大小写按原样比较 |
| `network_error_code` | 类型按十进制，值按 4 位十六进制大写（`0x1b`、`1B`、`001B` 视为同一个） |
| command_status | 十进制或 `0x` 十六进制都换成数值 |

### 10.4 通道 DR 的匹配顺序（Core）

输入：通道（rssid）、规范化后的 `stat`、`err`、`network_error_code`（可能没有）。

```text
0. 通道的 SMSOptionDRNotPaidCode（cm.cfg）：err 原样相等，或 network_error_code 的 6 位十六进制相等
   → 欠费（REJECTED_BYNC，码 404），结束
for 块 in [通道自己的块, DEFAULT 块]:          ← 通道块整块优先于 DEFAULT 块
    1. DRNetworkErr：(Type, Value) 完全相等             （有 TLV 时）
    2. DRErr：Stat 相等 且 Err 相等
    3. DRErr：Stat 相等 且 Err 在范围里（范围小的优先）
    4. DRErr：没写 Stat，Err 相等
    5. DRErr：没写 Stat，Err 在范围里
    6. DRErr：Stat 相等，没写 Err
    命中就结束
系统默认：DELIVRD→000，ACCEPTD→001，ENROUTE→002，UNDELIV→200，EXPIRED→501，
          REJECTD→400，DELETED→200，UNKNOWN 或认不出的状态词→999
```

- **通道块整块优先**：通道自己配的任何规则都比 `[DEFAULT]` 块里的规则优先，哪怕 `[DEFAULT]` 里的规则更具体（例如通道只配了 `(Stat=UNDELIV,Code=200)`，`[DEFAULT]` 有 TLV 规则，也用通道的）。这样每个通道的行为只看它自己的块就能确定，不会被公共配置意外改掉。
- **块内越具体越优先**：TLV 是标准化的码，最先看；`stat + err` 比只有 `err` 具体；只有 `stat` 的规则相当于这个状态的默认码，最后看。
- **DELIVRD**：回执状态是 DELIVRD 时，只有显式写了 `Stat=DELIVRD` 的规则才会把它换成别的码；没写就是 000，`err` 里有什么都不管（有些通道送达时 err 也填非 0）。
- **对不上**（落到系统默认，但通道给了非 0 的 `err` 或 TLV）：记 `[DR-ERR-UNMAPPED]` WARN 汇总和指标（6.4）。
- 结果：内部码 → 查 10.1 的码表得到内部状态，设置到消息上（替代现在 Client 上的 `getCodeFromState`）。

`SubmitErr` 的匹配：通道块 `Status` 相等 → 通道块 `Status=*` → `[DEFAULT]` 块同样两步 → 601。

### 10.5 给客户的匹配（Server 生成 DR 时）

输入：客户（ossid）、内部码、通道原始 `err`。

```text
1. 客户块 CustomerErr：Code 相等
2. 客户块 CustomerErr：Code 在范围里（范围小的优先）
3. 按 DRErrDefault：
     passthrough → 内部码
     vendor      → 通道原始 err（1～3 位数字时，补 0 到 3 位）；否则内部码
     legacy      → 现在的错误码表（按内部状态）（默认）
     000         → 000
4. 没有客户块：同 DRErrDefault=legacy（默认，客户收到的码和现在的错误码表一致）
```

- SMPP 回执：`err:` = 结果（3 位，不足补 0）；`stat:` = 码表里这个内部码的状态词（`SMSOptionDRStatusMapping` 照旧可以改显示）。
- HTTP：接口 XML 的 `errorCode` 字段 = 结果；状态字段照旧按 `StatusMapping` 用内部状态换。

### 10.6 一个完整的例子

```text
drErrorCode.cfg:    104 | UNDELIV | 10405 | Absent subscriber | 用户关机
drErrorMapping.cfg: [VENDOR_X]  DRErr: (Stat=UNDELIV,Err=002~009,Code=104)
                    [CUST_A]    CustomerErr: (Code=104,Err=013)

1. 通道回执：stat:UNDELIV err:5
2. Client：vendorErrorCode = "stat=UNDELIV;err=5"
3. Core：VENDOR_X 块，第 3 步命中（UNDELIV，5 在 002~009 里）→ 内部码 104 → 内部状态 10405
   日志 [DR-ERR] rssid=... vendor=stat=UNDELIV;err=5 code=104
   CDR OUT_DELI_REP：状态 10405（CDR 格式不变）
4. Server：CUST_A 块，第 1 步命中 → 013
   客户收到 deliver_sm：... stat:UNDELIV err:013 ...
   换成没配客户块的客户（默认 legacy）→ 内部状态 10405 在现在的表里是 016 → 收到 err:016
```

### 10.7 加载、热加载、内存

- 两个文件一起加载：先校验码表，再校验对照表（`Code` 必须在码表里；同一块里同一个键重复、范围重叠都算错），有错整次拒绝、保持旧配置，日志写明哪个块哪一组。对照表里的 shortName 在 cm.cfg 里找不到时只打 WARN（通道可能还没上线）。
- 加载后每个块建成几张哈希表（`stat|err`、`*|err`、`stat|`、`type:value`、command_status）加一个按范围大小排好的范围列表，匹配是几次哈希查找。
- 热加载（`RELOAD_DRERROR`）：整套表建好后一次替换；已经算好的 `drErrorCode` 随消息走，不受之后改表的影响；给客户的对照在 Server 发 DR 时按当时的表算。
- Core 用通道部分和码表，Server 用客户部分和码表；两边都加载整个文件，简单。

## 11. 实现说明和测试结果

**新增：**
- `domain/DrErrorConfig`：加载两个文件（码表没有文件时用内置的初始表，即 10.1 的表加 `209 DELETED 10300`）、通道侧匹配（`applyVendorDr`、`applySubmitResult`）、客户侧匹配（`customerErr`、`customerTlv`）、现在的错误码表（`legacyErr`，从 `Smpp.getErrCodFromStatusCode` 原样搬过来）。第一次用到时加载，`RELOAD_DRERROR` 重新加载；有错误时保持在用的表。
- `GmmsMessage.vendorErrorCode`、`drErrorCode`，Stream 格式第 58、59 段。

**改动：**
- Client（`Smpp`）：回执里的 `stat`、`err`（到下一个空格为止）、`network_error_code`（`类型:值`）原样存进 `vendorErrorCode`；submit_sm_resp 失败时存 `submit=command_status`（`MultiSmppPduProcessor`）。原来 Client 上按 `SMSOptionDRNotPaidCode` 改成欠费的判断挪到 Core。
- Core：`handleInboundDRInternal` 匹配到原消息后调 `applyVendorDr`；`handleOutSubmitRes` 开头调 `applySubmitResult`；假 DR 的码是状态对应的默认码（成功 000、失败 200）；方式 3 改成成功时码改成 000。
- Server（`Smpp.createSmppDeliverSM4dr`、`createSmppSubmitSM4dr`）：回执 `err:` 改为 `customerErr`（3 位）；客户配了 `DRErrTLV` 时加 `network_error_code` TLV。`SMSOptionIsNotSupportUnknowDRStatus` 的处理照旧在后面。
- HTTP：通用的 `CommonDeliveryReportHttpHandler` 支持接口 XML 里的 `errorCode` 参数——通道推来的 DR 里取这个字段存进 `vendorErrorCode`，给客户推的 DR 里放 `customerErr`。其他 70 多个定制 handler 没有改。
- `RedisControlSubscriber`：新命令 `RELOAD_DRERROR`。
- `A2PCustomerInfo`：`SMSOptionSmppMapErrCod4DR` 不再读取，读到时打 WARN。`SMSOptionDRNotPaidCode` 保留，`DrErrorConfig.applyVendorDr` 在对照表之前先按它判断（原样比较回执 err，或 6 位十六进制的 network_error_code）。

**和设计稿不同的地方：**
- **没有规则命中时不改状态**：状态保持 Client 按状态词定的（和现在一样），码取这个状态的默认码（码表里第一个是这个状态的码）。只有规则命中时才按码表改状态。这样不配对照表时，所有 DR 的状态都和现在完全一样。
- 两个字段写进 DB、RDQ 重试按码恢复状态，见第 12 节（DEF-C5 已修复）。
- 对照表里的 shortName 在 cm.cfg 里不存在时没有打 WARN（加载时客户表可能还没准备好）。

**测试：**
- `DrErrorIT`（新，`suites.txt`）35 项全部通过：`SMSOptionDRNotPaidCode` 的 err 和 network_error_code 优先于对照表规则、不在列表里时按对照表；范围、`005` 和 `5` 视为相同、TLV 优先、没有规则时状态不变、通道块没有命中时看 `[DEFAULT]`、欠费 404 改成 REJECTED_BYNC、DELIVRD 带 err 仍是 000、只按状态的规则、没有原始值时按状态、submit 失败的两种对照、没对照上的码打 WARN、两个字段的 Stream 格式；真实 Server 给 6 个客户发 DR：没有客户块（现在的表，10405→016）、`passthrough`、`CustomerErr` 精确和范围、`vendor`、`000`、`DRErrTLV`、没有码的 DR（假 DR / 系统 DR）按状态（003）；热加载生效、码不在表里时拒绝并保持旧规则。
- `ClientResultIT` 改写：VD-08 改为检查 `vendorErrorCode` 原样保留（原来测的是 Client 上的欠费判断）；VD-09d/f 改为按新的默认（没开 `SMSOptionSmppMapErrCod4DR` 的客户也按表，UNDELIV 收到 003）。87 项全部通过。

## 12. DB 和 DR 重试（2026-11-11）

### 12.1 改的表和列

存消息的表都加两列，SQL 在 `docs/sql/2026-11-11_dr_error_code.sql`（含回滚）：

| 表 | 用途 |
|---|---|
| `messagestore` | 消息存储 |
| `PMQ`、`PMQ_yyyyMMdd_0N`（按天建） | 待处理 |
| `RMQ` | MT 重试 |
| `SMQ` | 存储模式、ROP 忙 |
| `WDQ` | 等 DR |
| `QDQ` | DR 查询 |
| `RDQ` | **DR 重试** |
| `SDQ` | 先到的 DR，等以后匹配 |
| `IQDQ` | REST DR 查询 |
| `CSMQ` | 长短信（旧模式） |

| 列 | 类型 | 内容 |
|---|---|---|
| `VendorErrorCode` | `varchar(64) NULL` | 通道原始值，如 `stat=UNDELIV;err=5;nec=3:001B`、`submit=11` |
| `DrErrorCode` | `varchar(3) NULL` | 内部错误码 |

- 写：`makeUpdateStmt`（所有队列表的 INSERT 和 UPDATE）、`makeUpdateForSDQ`（SDQ），以及按文件备份 SQL 的 `PmqMessageWriter`、`MessageBackupWriter`（RMQ、RDQ）、`SdqMessageWriter`。按天新建的 PMQ 表（`createPMQTable`）直接带这两列。
- 读：`assignValue`（所有队列表）、`assignValue4SDQ`。

### 12.2 没执行 SQL 的库也能跑

GmmsConfig `DB.DrErrorCodeColumns`（热加载）：

| 值 | 行为 |
|---|---|
| `auto`（默认） | 第一次用到时对每张表查一次 `SELECT VendorErrorCode, DrErrorCode FROM 表 WHERE 1=0`：所有存在的表都有这两列才写、读；有表缺列时打 ERROR 列出这些表、不写这两列（不会因为列不存在而写库失败）。库里不存在的表跳过。连不上库时也不用。`RELOAD_DRERROR` 会重新检查（执行完 SQL 后不用重启） |
| `true` | 一定写、读（确认 SQL 已执行时） |
| `false` | 不写、不读 |

### 12.3 DR 重试按码恢复状态（修复 DEF-C5）

RDQ 里的 DR 重发时（`getResendDR`），原来只按状态文本恢复状态（`getCodeFromState`），细分状态（10405、10402）的文本对不上，恢复成 UNKNOWN（DEF-C5）。现在（`restoreResendDrStatus`）：

```text
按文本得到状态 byText
DR 带着内部码，并且（byText 是 UNKNOWN，或码的状态和 byText 同一类（同一个百位））→ 用码的状态
否则 → byText（和原来一样）
```

例：10405 的 DR 第一次发送失败进 RDQ，`DrErrorCode=104` → 重发时恢复成 10405，客户收到 `stat:UNDELIV`。

为了让所有 DR 都带着码：Server 给客户发 DR 时，DR 上没有码就补上**这个状态自己的码**（码表里状态完全相同的第一个码，没有就不补）；Core 处理通道 DR 没有规则命中时也一样。只补“状态完全相同”的码，避免把 10801 这类没有自己码的状态恢复成别的状态（`ServerDrPduIT` SD-02g）。

### 12.4 测试

- `DrErrorIT` 新增：`DB.DrErrorCodeColumns=true` 时 RDQ 的 INSERT 带两列；`false` 时不带；`auto` 连不上库时不用；重试恢复的 4 种情况（文本不是 DR 状态、同一类、不同类、没有码）。
- `ServerDrPduIT`：重试的恢复改用 `restoreResendDrStatus`，SD-03a、SD-03b（DEF-C5）通过，从 `known-defects.txt` 删掉；SD-02g（10801）和原来一样。
- 注意：测试里 DB 是录 SQL 的假实现，真实库上的写入和读回要在测试环境执行 SQL 后验证一次（12.2 的 `auto` 会在日志里写明检测结果）。

