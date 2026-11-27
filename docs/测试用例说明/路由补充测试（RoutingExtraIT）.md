# 路由补充测试（RoutingExtraIT）

> 日期：2026-11-27。用例代码：`regression/test/it/RoutingExtraIT.java`，套件 `RoutingExtraIT`（`regression/suites.txt`，it 模式）；模拟程序：`regression/test/it/EnumMock.java`。
> 补的是另外三组路由用例没有覆盖的内容（各文档第 6 节）：运营商层（ENUM 查询）、ENUM 和 IR21 查询本身、二次路由、轮流选通道、排除关键字、系统路由替换的重新加载、黑洞比例。

## 1. 结论

| 结果 | 内容 |
|---|---|
| 通过 | 运营商层的选路（具体运营商、国家级、`-2`、通道不能用时的运营商备份）；运营商层压过更长的前缀；ENUM 只对配置的号段查、没配地址时用全局地址；ENUM 失败、HTTP 500、不是 JSON、没有数据、号码不存在、超时都按前缀路由，消息不丢；40 条并发查询没有串号；二次路由；`Relay=V1,V2,V3` 轮流；`ExcludeKeywordId`；`RELOAD_SYSTEM_REPLACE`；按通道和按比例进黑洞；通道的发送时间窗 |
| **测出并已修复** | **OP-06**：运营商通道发送失败后不走运营商层的备份（同时 CDR 的运营商列一直是 0）；**SR-06**：`SMSOptionSecondRoutingRecPrefix: all` 不生效。见第 6 节 |
| 后补 | IR21 的成功路径：测试服务器 11-27 装上 `dig` 后测试通过（4.3）；ENUM 超时改成配置项 `EnumQueryTimeoutMs`，默认 5 秒（4.2）；多个 Core 同时重新加载路由（《路由重新加载测试（IncrementalRoutingIT）》第 7 节） |

## 2. 模拟程序 EnumMock

### 2.1 模拟什么

Core 路由前会按客户配置查接收号码的运营商（`IOSMSDispatcher`、`DNSClient`），有两种查法：

| 查询 | Core 怎么查 | 模拟程序怎么答 |
|---|---|---|
| ENUM / MNP | HTTP POST `{"number":"8613800000001"}` 到客户的 `SMSOptionEnumRoutingURL`，没配时用 GmmsConfig 的 `mnpqueryurl`；连接和读超时都是 30 秒 | `{"errmsg":"SUCCESS","data":[{"8613800000001":{"country_code":86,"operator_id":1,"mcc":"460","mnc":"00","operator":"ChinaMobile","type":"mobile"}}]}` |
| IR21 | 号码在 `SMSOptionIR21EnumQueryRecPrefix` 里时，执行系统命令 `dig @{SMSOptionEnumIR21RoutingURL} +time={秒} +tries=1 -t NAPTR 1.0.0.0.0.0.0.0.8.3.1.6.8.e164.arpa.`（没配时用 GmmsConfig 的 `DNSAddress`） | DNS NAPTR 应答：`100 100 "u" "E2U+a2p" "!^.*$!a2p:8613800000001@cc86.opid1.mnc00.mcc460.gprs!" .` |

Core 拿到结果后，把运营商记成 `国家码 × 10000 + operator_id`（例如 860001），用来匹配 `A2POPRouting` 的 `Opid`。

### 2.2 规则文件

每行一条，按号码的最长前缀匹配；文件修改后自动重新读取：

```
# 前缀       应答                              [delay=毫秒]
8613800    86,1,460,00,ChinaMobile           # 国家码,运营商,mcc,mnc,名称
8613801    86,2,460,01,ChinaUnicom
8613803    86,3,460,03,ChinaTelecom
8613811    852,5,454,05,HK5
8613902    fail                              # errmsg=FAIL，没有 data
8613903    86,1,460,00,ChinaMobile  delay=35000   # 35 秒后才回（Core 只等 EnumQueryTimeoutMs，默认 5 秒）
8613904    http500                           # HTTP 500
8613905    badjson                           # 不是 JSON
8613906    empty                             # SUCCESS 但 data 为空
```

没有匹配的号码回 `{"errmsg":"NOT FOUND","data":null}`。其他接口：`GET /stats`（每个号码被查了几次，HTTP 和 DNS 分开）、`POST /reset`（清零）、`GET /health`。

### 2.3 测试服务器上的部署

| 项 | 值 |
|---|---|
| 目录 | `/usr/local/gw-enum-mock`（`classes/` 程序，`enum-mock.cfg` 规则，`log/enum-mock.log` 日志） |
| 地址 | HTTP `127.0.0.1:15200`，DNS `127.0.0.1:15353`（UDP），只监听本机 |
| 启动 | `cd /usr/local/gw-enum-mock && setsid nohup ./start.sh &` |
| 停止 | `pkill -f "[i]t.EnumMock"` |
| 网关使用 | 客户配置 `SMSOptionEnumQueryRecPrefix: 86`、`SMSOptionEnumRoutingURL: http://127.0.0.1:15200/mnp/realenumquery2`；IR21 再加 `SMSOptionIR21EnumQueryRecPrefix: 86`、`SMSOptionEnumIR21RoutingURL: 127.0.0.1 -p 15353`（Core 把这个值原样拼进 dig 命令，所以可以带端口参数） |
| 手工查 | `curl -X POST -d '{"number":"8613800000001"}' http://127.0.0.1:15200/mnp/realenumquery2` |

回归用例不依赖这个部署：RoutingExtraIT 自己在进程内另起一个 EnumMock（HTTP `127.0.0.1:15201`，DNS `127.0.0.1:15354`）。

## 3. 怎么跑、怎么判定

```
regression/run.sh RoutingExtraIT
cat regression/.work/results/RoutingExtraIT.txt
```

- 进程内启动真实的 Core（`MTStreamConsumer`，以及处理发送结果的 `ResultStreamConsumer`），不需要数据库。
- 发 MT、看结果的方式和《路由优先级测试（CoreRouteIT）》2.2 一样：看这条 MT 出现在哪个通道的出向流 `stream:mt:routed:{ssid}:*`。默认发送方 `18667991226`、内容 `hello`。
- 模拟发送失败：把这条 MT 作为状态 2500 的 submit_resp 写进结果流（同《路由规则测试（RoutingRuleIT）》2.2）。
- 整组跑完约 1 分 20 秒，其中超时的用例（EN-06、EN-06b、IR-04）各要等 2～5 秒。

## 4. 用例

### 4.0 公共配置

cm.cfg 块的公共部分（下面各表只列额外配置）：

```
[名称]
CCBSSID: ssid
A2PPlayerType: operator
Protocol: SMPP
```

| 块 | CCBSSID | 额外配置 |
|---|---|---|
| V931 | 931 | `SMSOptionSubmitRetryCode: 2500`、`SMSOptionMTRetryPolicy: 1S,1S`（OP-06 要让它发送失败后改道） |
| V932～V949、V951～V954 | 932～954 | — |
| V950 | 950 | `SMSOptionDeliveryStartTime`、`SMSOptionDeliveryEndTime`：当前时间 2～3 小时之后（GMT，4.9） |
| BH | 490 | `Role: blackhole` |

V999 不在 cm.cfg 里，用来测“通道不存在”。其他：

| 文件 | 内容 |
|---|---|
| `Gmms/GmmsConfig.properties` | 追加 `BlackholeSsid=490`（按比例黑洞需要，4.8）；启动后把 `mnpqueryurl` 设为 `http://127.0.0.1:15201/mnp/realenumquery2`（EN-03） |
| `conf/contentTemplate.cfg` | `[K7]` / `English: promo .*`（4.6） |
| `conf/vendorRoutingReplacement.cfg` | 开始时 `86:999:1:2`（和这组无关的一条），4.7 里会改 |
| 发送方黑白名单 | 不配（`senderactiondir` 是空目录，客户都是默认 NonPA，发往 86、852 不受限制） |

### 4.1 运营商层（ENUM 查询）

**配置**

客户 C971：

```
[C971]
CCBSSID: 971
A2PPlayerType: operator
Protocol: SMPP
SMSOptionEnumQueryRecPrefix: 86
SMSOptionEnumRoutingURL: http://127.0.0.1:15201/mnp/realenumquery2
```

`conf/routing/C971_routingInfo.cfg`（C970、C972～C974 用同样的规则，只是 CCBSSID 不同）：

```
CCBSSID: 971
A2POPRouting: (Priority=10,Opid=860001,Relay=V931);(Priority=10,Opid=860000,Relay=V932);(Priority=10,Opid=-2,Relay=V933);(Priority=10,Opid=860003,Relay=V999)
A2POPBackupRouting: (Priority=10,Opid=860003,Relay=V936);(Priority=10,Opid=860001,Relay=V939)
A2PMainRouting: (Priority=10,PhonePrefix=8613800,Relay=V934);(Priority=20,PhonePrefix=86,Relay=V935)
A2PBackupRouting: (Priority=10,PhonePrefix=86,Relay=V940)
```

| 层 | 分组 | 通道 |
|---|---|---|
| 运营商 | 860001（具体运营商） | V931；备份 1 级 V939 |
| 运营商 | 860003 | V999（不存在）；备份 1 级 V936 |
| 运营商 | 860000（中国所有运营商） | V932 |
| 运营商 | -2（所有运营商） | V933 |
| 普通 | 8613800 | V934 |
| 普通 | 86 | V935；备份 1 级 V940 |

EnumMock 的规则：

```
8613800 86,1,460,00,ChinaMobile
8613801 86,2,460,01,ChinaUnicom
8613803 86,3,460,03,ChinaTelecom
8613811 852,5,454,05,HK5
```

**用例**（客户 C971）

| ID | 目的 | 接收号码 | 预期 | 结果 |
|---|---|---|---|---|
| OP-01 | 具体运营商的规则；运营商层压过更长的前缀 | 8613800000001（查到 860001） | V931，不是普通层 8613800 的 V934 | 通过 |
| OP-02 | 运营商没有规则时用国家级 | 8613801000001（860002） | V932（860000） | 通过 |
| OP-03 | 国家级也没有时用 -2 | 8613811000001（8520005） | V933 | 通过 |
| OP-04 | 运营商规则的通道不存在：选路时用运营商层的备份 | 8613803000001（860003） | V936 | 通过 |
| OP-05 | 一条 MT 只查一次 | 同 OP-01 | EnumMock 收到这个号码 1 次 | 通过 |
| OP-00 | 运营商层路由的消息，从 Redis 读回来后还带着运营商 | 同 OP-01 | 出向流里这条 MT 的 roperator 是 860001（不是通道 931） | 通过（修复前是 931） |
| OP-06 | 运营商通道发送失败后改道，先看运营商层的备份 | 8613800000007（860001）→ V931，V931 失败（2500） | V939（运营商层备份 1 级） | 通过（修复前去了普通层备份 V940） |
| OP-07 | CDR 的运营商列 | OP-06 那条在 V931 的 OUT_SUBMIT CDR | 运营商列是 1（860001 % 10000） | 通过（修复前是 0） |
| OP-08 | 普通前缀层路由的消息不变 | 8613902000001（查询失败，按前缀到 V935） | roperator 是通道 935，和原来一样 | 通过 |

**roperator 和 rssid 的分工**（2026-11-27 起）：roperator 是选路用的运营商，rssid 是通道。
- 走运营商、号码、二次路由层选到通道时，roperator 保持选路前的值（ENUM 查到的运营商，例如 860001）；走普通前缀层时，roperator 等于通道 ssid，和原来一样。旧代码的运营商、号码、二次路由路由器就是这样做的。
- 用 roperator 的：运营商层选路和改道、CDR 的运营商列、选路过程中的路由白名单。
- 和通道有关的都按 rssid：Client 填 MCC/MNC、发送时间窗、`BlackholePercent` 的 `R_SSID`、`RequiredCharsets`、`A2PContentSignature` 的 `R_OP`、`SenderReplacement`、长短信重发时判断是否已路由。对普通前缀路由的消息，这些和原来完全一样（原来 roperator 就等于 rssid）。

### 4.2 ENUM 查询本身

**配置**

| 客户 | 额外配置 | 路由 |
|---|---|---|
| C970 | 无（不查 ENUM） | 同 4.1 |
| C972 | `SMSOptionEnumQueryRecPrefix: 852`、`SMSOptionEnumRoutingURL: http://127.0.0.1:15201/mnp/realenumquery2` | 同 4.1 |
| C973 | `SMSOptionEnumQueryRecPrefix: 86`（不配地址，用 GmmsConfig 的 `mnpqueryurl`） | 同 4.1 |
| C971 | 见 4.1 | 同 4.1 |

EnumMock 的规则（失败、超时和并发用）：

```
8613902 fail
8613903 86,1,460,00,ChinaMobile delay=35000
8613904 http500
8613905 badjson
8613906 empty
8613820 86,1,460,00,ChinaMobile delay=100
8613821 86,2,460,01,ChinaUnicom delay=100
```

**用例**

| ID | 目的 | 客户 / 接收号码 | 预期 | 结果 |
|---|---|---|---|---|
| EN-01 | 没配查询号段：不查 | C970 / 8613800000002 | V934（普通层）；EnumMock 没收到查询 | 通过 |
| EN-02 | 号码不在查询号段里：不查 | C972（852）/ 8613800000003 | V934；没有查询 | 通过 |
| EN-03 | 没配客户地址：用全局 `mnpqueryurl` | C973 / 8613800000004 | V931；查了 1 次 | 通过 |
| EN-04 | 查询失败：errmsg=FAIL | C971 / 8613902000001 | V935（按前缀），消息不丢 | 通过 |
| EN-04 | HTTP 500 | C971 / 8613904000001 | V935 | 通过 |
| EN-04 | 应答不是 JSON | C971 / 8613905000001 | V935 | 通过 |
| EN-04 | SUCCESS 但没有数据 | C971 / 8613906000001 | V935 | 通过 |
| EN-04 | 号码不存在 | C971 / 8613990000001 | V935 | 通过 |
| EN-05 | 并发查询不串号：所有 Core 线程共用一个查询客户端，连接存在实例字段里 | C971，一次写入 40 条，8613820…（860001）和 8613821…（860002）交替，每次查询延迟 100 毫秒 | 每条都按自己号码的运营商路由：8613820 → V931，8613821 → V932 | 通过（0 条错、0 条丢） |
| EN-06 | 查询超时 | C971 / 8613903000001（35 秒才回） | `EnumQueryTimeoutMs`（默认 5000）后按前缀路由到 V935 | 通过：5.09 秒后到 V935（改之前是写死的 30 秒） |
| EN-06b | 超时可以热加载 | 运行中把 `EnumQueryTimeoutMs` 设成 2000，C971 / 8613903000002 | 2 秒左右按前缀路由 | 通过：2.04 秒 |
| EN-07 | 一条查询卡住时，不影响其他 MT | 和 EN-06 同时发 8613800000005 | 很快路由到 V931 | 通过：106 毫秒 |

EN-06 说明：查询服务不响应时，这条 MT 要占着 Core 的一个处理线程，直到 `EnumQueryTimeoutMs`。原来写死 30 秒，查询服务整体变慢时 Core 的处理线程会被大量占用；2026-11-27 改成配置项，默认 5 秒（《配置项说明》）。

### 4.3 IR21

**配置**

```
[C974]
CCBSSID: 974
A2PPlayerType: operator
Protocol: SMPP
SMSOptionEnumQueryRecPrefix: 86
SMSOptionIR21EnumQueryRecPrefix: 86
SMSOptionEnumIR21RoutingURL: 127.0.0.1 -p 15354
```

路由同 4.1。

**用例**

| ID | 目的 | 做法 | 预期 | 结果 |
|---|---|---|---|---|
| IR-01 | 模拟程序按 IR21 服务器的格式回 NAPTR | 用 JNDI 的 DNS 客户端查 `1.0.0.0.0.0.0.0.8.3.1.6.8.e164.arpa.` 的 NAPTR | 应答里有 `@cc86.opid1.mnc00.mcc460` | 通过 |
| IR-02 | Core 用 IR21 查运营商 | C974 发 8613800000006 | V931（860001） | 通过（EnumMock 收到 1 次 DNS 查询） |
| IR-03 | IR21 查到的运营商没有专门规则 | C974 发 8613801000006（860002） | V932（国家级 860000） | 通过 |
| IR-04 | IR21 查询超时 | C974 发 8613903000003（35 秒才回） | dig 只试 1 次，`EnumQueryTimeoutMs` 后按前缀到 V935 | 通过：5.08 秒 |
| IR-05 | IR21 查不到（NXDOMAIN） | C974 发 8613990000006 | 按前缀到 V935 | 通过 |

**说明**：Core 的 IR21 查询是执行系统的 `dig` 命令，机器上必须有 `dig`（`bind-utils`）。测试服务器 11-27 已经安装；没有 `dig` 的机器上，IR-02 改为检查“IR21 失败、按前缀路由、消息不丢”，IR-03～05 跳过。`dig` 现在带 `+time=秒数 +tries=1`，按 `EnumQueryTimeoutMs` 只等一次（原来是 dig 默认的 5 秒 × 3 次）。

### 4.4 二次路由

**配置**

| 客户 | 额外配置 |
|---|---|
| C975 | `smsOptionNeedSupportSecondRouting: true`<br>`smsOptionNeedSupportSecondRoutingExpiredTime: 3`<br>`SMSOptionSecondRoutingRecPrefix: 86` |
| C976 | 同上，但 `SMSOptionSecondRoutingRecPrefix: all` |
| C977 | 无（不开二次路由） |

三个客户的路由文件内容相同（CCBSSID 各自的）：

```
A2PSecRouting: (Priority=10,PhonePrefix=86,Relay=V937)
A2PMainRouting: (Priority=10,PhonePrefix=86,Relay=V938);(Priority=10,PhonePrefix=852,Relay=V938)
```

**用例**

| ID | 目的 | 客户 / 接收号码 | 预期 | 结果 |
|---|---|---|---|---|
| SR-01 | 第一次发这个号码：主路由 | C975 / 8613700000001 | V938 | 通过 |
| SR-02 | 3 秒内再发：二次路由 | C975 / 8613700000001 | V937 | 通过 |
| SR-03 | 另一个号码第一次 | C975 / 8613700000002 | V938 | 通过 |
| SR-04 | 不在二次路由号段的号码，连发两次 | C975 / 85212345678 | 两次都是 V938 | 通过 |
| SR-05 | 超过 3 秒后再发 | C975 / 8613700000001（等 3.5 秒） | V938 | 通过 |
| SR-06 | 号段配 `all` | C976 / 8613700000003，连发两次 | 第二次 V937 | 通过（修复前两次都是 V938） |
| SR-07 | 没开二次路由时，有 `A2PSecRouting` 也不用 | C977 / 8613700000004，连发两次 | 两次都是 V938 | 通过 |
| SR-08a | `A2PSecRouting` 的分组：具体前缀先于 `all` | C984 / 8613700000005，连发两次 | 第一次主路由 V954；第二次 86 分组的 V953（不是 Priority 更小的 `all` 分组） | 通过 |
| SR-08b | 没有具体分组时用 `all` | C984 / 447700900001，连发两次 | 第一次 V954；第二次 `all` 分组的 V952 | 通过 |

C984：`SMSOptionSecondRoutingRecPrefix: all`，路由 `A2PSecRouting: (Priority=10,PhonePrefix=all,Relay=V952);(Priority=20,PhonePrefix=86,Relay=V953)`、`A2PMainRouting: (Priority=10,PhonePrefix=86;44,Relay=V954)`。

`SMSOptionSecondRoutingRecPrefix` 只决定号码能不能走二次路由（写了 `all` 就是所有号码都能），不影响 `A2PSecRouting` 里规则的先后。

**SR-06 的原因**：`isNeedSecondRoutingRecPrefix()` 里写的是 `"all".equalsIgnoreCase(recipient)`，拿接收号码和 `all` 比，已改成和配置的前缀比。

### 4.5 轮流选通道

**配置**：C978，路由 `A2PMainRouting: (Priority=10,PhonePrefix=86,Relay=V941,V942,V943)`。

| ID | 目的 | 输入 | 预期 | 结果 |
|---|---|---|---|---|
| RR-01 | `Relay=V941,V942,V943` 轮流 | 连续发 6 条 | 依次轮流，每个通道 2 次 | 通过：941, 942, 943, 941, 942, 943 |

### 4.6 排除关键字

**配置**：C979，路由 `A2PMainRouting: (Priority=10,PhonePrefix=86,ExcludeKeywordId=K7,Relay=V944);(Priority=20,PhonePrefix=86,Relay=V945)`；`contentTemplate.cfg` 的 K7 是 `promo .*`。

| ID | 目的 | 内容 | 预期 | 结果 |
|---|---|---|---|---|
| EX-01 | 内容不属于 K7 | `hello` | V944 | 通过 |
| EX-02 | 内容属于 K7：这条规则不命中 | `promo 50 off` | V945（下一条规则） | 通过 |

### 4.7 系统路由替换的重新加载

**配置**：C980，路由 `A2PMainRouting: (Priority=10,PhonePrefix=86,Relay=V946)`。每一步改写 `conf/vendorRoutingReplacement.cfg`（修改时间设成 5 秒前），再向 `channel:gmms:control` 发 `RELOAD_SYSTEM_REPLACE`，等 2 秒。

| ID | 替换文件 | 预期 | 结果 |
|---|---|---|---|
| SY-01 | `86:999:1:2`（和 946 无关） | V946 | 通过 |
| SY-02 | `86:980:946:947` 和 `86:-1:946:948` | V947（客户级优先） | 通过 |
| SY-03 | 只有 `86:-1:946:948` | V948 | 通过 |
| SY-04 | 改回 `86:999:1:2` | V946 | 通过 |

### 4.8 黑洞比例

**配置**

```
[C981]
CCBSSID: 981
A2PPlayerType: operator
Protocol: SMPP
SMSOptionEnumQueryRecPrefix: 86
SMSOptionEnumRoutingURL: http://127.0.0.1:15201/mnp/realenumquery2
BlackholePercent: (Percent=0);(R_SSID=947,Percent=100)

[C983]
CCBSSID: 983
A2PPlayerType: operator
Protocol: SMPP
SMSOptionEnumQueryRecPrefix: 86
SMSOptionEnumRoutingURL: http://127.0.0.1:15201/mnp/realenumquery2

[C982]
CCBSSID: 982
A2PPlayerType: operator
Protocol: SMPP
BlackholePercent: (Percent=30)
```

```
# C981_routingInfo.cfg
CCBSSID: 981
A2POPRouting: (Priority=10,Opid=860001,Relay=V947);(Priority=10,Opid=860000,Relay=V948)

# C982_routingInfo.cfg
CCBSSID: 982
A2PMainRouting: (Priority=10,PhonePrefix=86,Relay=V949)
```

GmmsConfig 要有 `BlackholeSsid=490`。`BlackholePercent` 里的 `R_SSID` 是**路由到的通道的 ssid**（按 rssid 判断）。要让某个运营商的消息进黑洞：用运营商路由把它路由到某个通道，再给这个通道设比例。

**用例**

| ID | 目的 | 客户 / 接收号码 | 预期 | 结果 |
|---|---|---|---|---|
| BH-01 | 计时窗口里的第一条不进黑洞 | C981 / 8613801004001（860002 → V948） | V948 | 通过 |
| BH-02 | 路由到 V947 的进黑洞（`R_SSID=947,Percent=100`） | C981 / 8613800004002（860001 → V947） | 不发出，IN_SUBMIT CDR 1 条，客户收到 DELIVERED | 通过 |
| BH-03 | 路由到 V948 的不进（客户默认 `Percent=0`） | C981 / 8613801004003 | V948 | 通过 |
| BH-04 | 再发一条到 V947 的 | C981 / 8613800004004 | 进黑洞 | 通过 |
| BH-05 | `Percent=30` 的比例 | C982，先发 1 条（窗口里第一条），再发 400 条 | 进黑洞的比例在 22%～38% | 通过：30.0%（发出 280 条） |

### 4.9 通道的发送时间窗

**配置**：V950 配 `SMSOptionDeliveryStartTime`、`SMSOptionDeliveryEndTime`，时间窗是“当前时间 2～3 小时之后”（按默认时区 GMT，用例运行时算出来），所以现在不在窗内；C983 查 ENUM，路由 `A2POPRouting: (Priority=10,Opid=860001,Relay=V950);(Priority=10,Opid=860000,Relay=V951)`。

| ID | 目的 | 接收号码 | 预期 | 结果 |
|---|---|---|---|---|
| DW-01 | 运营商层路由到 V950 的消息，按 **V950 的时间窗** | 8613800006001（860001 → V950） | 不发出，日志 `scheduleDeliveryTime update to next day`（推到时间窗开始时，写 SMQ） | 通过 |
| DW-02 | 没有时间窗的通道 | 8613801006002（860002 → V951） | V951 | 通过 |

Client 给 submit_sm 填 MCC/MNC 也改成了按通道，但只在原客户是透传模式时才执行，而且 ENUM 已经设了 `rMncMcc`，按通道取配置的那一步走不到，这里没有单独测。

## 5. 最近一次结果

| 时间 | 环境 | 结果 |
|---|---|---|
| 2026-11-27 | 测试服务器（修复前，提交 `79173c7` 的代码） | 36 项通过、2 项失败（OP-06、SR-06） |
| 2026-11-27 | 测试服务器（修复后） | 45 项全部通过 |
| 2026-11-27 | 测试服务器（`EnumQueryTimeoutMs`、装了 `dig`） | 49 项全部通过 |

## 6. 修复（2026-11-27）

**OP-06 和 CDR 运营商列：两处回归**

对比最早的代码（`f49bd6d`）：`setRSsID()` 一直会把 roperator 改成通道 ssid，但旧的运营商、运营商备份、号码、二次路由路由器选定通道后都会把它恢复成运营商（`//set opId to real op id`），所以 CDR 能写出运营商号，改道时也能找到运营商层的备份。现在失效有两个原因：

| # | 原因 | 修复 |
|---|---|---|
| 1 | 11-13 路由重构（`c9c19d3`）合并成 `RuleRouter` 时，漏了“恢复运营商”这一步 | `IOSMSChannelRouter`（选路）、`IOSMSRerouteDispatcher`（改道）：规则来自运营商、号码、二次路由层时，设置通道后恢复 roperator（`RoutingRule.Layer.keepsOperator()`） |
| 2 | 改成 Redis Stream 后每一跳都要反序列化：`SerializableHandler` 先读第 6 段 roperator，再用 `setRSsID` 读第 8 段通道，把 roperator 又覆盖了（旧架构走内部连接，不经过 Redis，所以以前没暴露） | 读第 8 段时保留第 6 段的 roperator |

roperator 保住以后，和通道有关的判断都改成按 rssid（见 4.1 下面的“分工”），所以这些通道配置对运营商路由的消息照样生效（DW-01、BH-02）。

**SR-06**：`isNeedSecondRoutingRecPrefix()` 改成用配置的前缀和 `all` 比较。

**后补的（2026-11-27）**：ENUM / IR21 超时改成 `EnumQueryTimeoutMs`（默认 5 秒，热加载）；测试服务器装了 `dig`；多个 Core 同时重新加载路由在测试服务器上测过（《路由重新加载测试（IncrementalRoutingIT）》第 7 节）。

**还没处理的**

| 问题 | 说明 |
|---|---|
| 透传模式的客户 | 消息进来时按 PDU 的 MCC/MNC 设了 roperator（运营商客户的 ssid），反序列化不再覆盖后会一直保留；这类客户如果配了运营商路由，会拿这个 ssid 去匹配 `Opid`。这和旧架构一致，没有单独测 |
