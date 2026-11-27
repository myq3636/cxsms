# 路由优先级测试（CoreRouteIT）

> 日期：2026-11-27。用例代码：`regression/test/it/CoreRouteIT.java`，套件 `CoreRouteIT`（`regression/suites.txt`，it 模式），21 项检查。
> 路由规则的完整说明见《路由配置说明》，功能清单见《Core模块功能与测试用例清单》第三节“路由”。同组的另外两份：《路由规则测试（RoutingRuleIT）》、《路由重新加载测试（IncrementalRoutingIT）》。

## 1. 测什么

一条新 MT 进 Core 后，选哪个通道：

- 前缀路由，以及最长前缀优先；
- 发送方路由、关键字路由与前缀路由的先后；
- 号码路由压过普通路由（包括发送方规则）；
- 主路由没有命中时走备份路由；
- 客户没有路由时走所属业务类型的默认路由；
- 按权重分流；
- 没有路由时回失败 DR；
- 黑洞通道和按比例进黑洞。

## 2. 怎么跑、怎么判定

### 2.1 运行

```
regression/run.sh CoreRouteIT                     # 单独跑
cat regression/.work/results/CoreRouteIT.txt      # 每项一行 PASS / FAIL
```

- 环境：测试服务器上的回归环境（JDK 17，本地 Redis 16379），不需要真实的客户、通道和数据库。
- 进程内启动真实的 Core：配置加载、`MTStreamConsumer`、路由、写出向流都是生产代码。
- 用例启动前，自己把第 3、4 节的配置写进回归用的 `a2p_home`（`regression/.work/home`）。

### 2.2 一条 MT 怎么发、怎么看结果

- **发送**：模拟 SMPP Server，把 MT 直接写进 `stream:mt:pending:{ossid}:{分片}`，类型 Submit，要 DR。
  - 默认发送方 `18667991226`，默认接收号码 `8613800000000`，默认内容 `hello`。
  - 用例表里的“输入”只列出和默认值不同的字段。
- **结果**：

| 结果 | 怎么判断 |
|---|---|
| 路由到通道 X | `stream:mt:routed:{X 的 ssid}:*` 里有这条 MT，并且客户的 DR 流里没有它的 DR |
| 路由失败 | 出向流里没有这条 MT，`stream:dr:pending:{ossid}:*` 里有给客户的失败 DR |
| 已确认 | 日志里有这条 MT 的 `[STREAM-ACK] flow=server->core` |
| CDR | 日志里的 `[CDR] type=...` 行，或者 CDR 文件里的条数 |

手工复查可以看 Core 的日志：路由成功打 `[MT-TO-CLIENT] rssid=...`，路由失败打 `[ROUTING]` 相关的 WARN。

## 3. 公共配置

### 3.1 cm.cfg 块的写法

用例里每个块都是下面的公共部分，再加上各小组表里列出的“额外配置”：

```
[名称]
CCBSSID: ssid
A2PPlayerType: operator
Protocol: SMPP
```

### 3.2 通道（各小组共用）

| 块名 | CCBSSID | 额外配置 | 说明 |
|---|---|---|---|
| V401 | 401 | — | 通道 |
| V402 | 402 | — | 通道 |
| V403 | 403 | — | 通道 |
| V404 | 404 | — | 通道 |
| V405 | 405 | — | 通道 |
| BH | 490 | `Role: blackhole` | 黑洞通道（4.6） |

通道不需要 OutgoingConnection：用例只看 Core 把 MT 写到了哪个通道的出向流，不真正发给供应商。

### 3.3 发送方黑白名单

这组用例**不配发送方名单**：回归环境的 `conf/senderactiondir` 是空目录。客户都是默认的 NonPA，NonPA 只在发往 65 时严格检查发送方，所以发往 86、852 不受发送方限制。

## 4. 用例

### 4.1 前缀、发送方、关键字

**配置**

cm.cfg：

```
[R500]
CCBSSID: 500
A2PPlayerType: operator
Protocol: SMPP
```

`conf/routing/R500_routingInfo.cfg`（4.2、4.3 也用这个文件）：

```
CCBSSID: 500
A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V401);(Priority=31,PhonePrefix=8613,Relay=V402);(Priority=10,PhonePrefix=86;8613,Sender=SENDA,Relay=V403);(Priority=20,PhonePrefix=86;8613,KeywordId=K1,Relay=V404)
A2PBackupRouting: (Priority=30,PhonePrefix=852,Relay=V405)
A2PNumberRouting: (Number=8613800000099,Relay=405,Priority=1)
```

`conf/contentTemplate.cfg`（关键字 K1：内容匹配 `your otp .*`）：

```
[K1]
English: your otp .*
```

R500 的规则按“层 / 分组 / 优先级”整理：

| 层 | 分组 | 规则（Priority 从小到大） |
|---|---|---|
| 号码层 | 8613800000099 | P1 → 405（V405） |
| 普通层 | 8613 | P10 发送方 SENDA → V403；P20 关键字 K1 → V404；P31 → V402 |
| 普通层 | 86 | P10 发送方 SENDA → V403；P20 关键字 K1 → V404；P30 → V401 |
| 普通层备份 1 级 | 852 | P30 → V405 |

**用例**（客户 R500）

| ID | 目的 | 输入 | 预期 | 依据 |
|---|---|---|---|---|
| RT-01a | 前缀路由 | 接收 `8618600000000`（不在 8613 段） | V401（86 分组 P30） | 分组由具体到宽泛 |
| RT-01b | 最长前缀优先 | 接收 `8613800000000` | V402（8613 分组 P31） | 优先级只在同一分组里比：8613 分组有规则命中，就不看 86 分组的 P30 |
| RT-02 | 发送方路由优先于前缀 | 发送方 `SENDA` | V403（8613 分组 P10） | 同一分组里 P10 < P31 |
| RT-03a | 关键字路由优先于前缀 | 内容 `your otp 1234` | V404（8613 分组 P20） | 内容命中模板 K1；P20 < P31 |
| RT-03b | 发送方路由优先于关键字路由 | 发送方 `SENDA`，内容 `your otp 1234` | V403 | 两条都满足，P10 < P20 |

### 4.2 备份路由

**配置**：同 4.1 的 R500，用到的是 `A2PBackupRouting: (Priority=30,PhonePrefix=852,Relay=V405)`；R500 的主路由里没有 852 的规则。

**用例**（客户 R500）

| ID | 目的 | 输入 | 预期 | 依据 |
|---|---|---|---|---|
| RT-06 | 主路由没有命中时走备份 | 接收 `85212345678` | V405（备份 1 级的 852） | 《路由配置说明》3.3：主路由一条规则都没命中，从备份 1 级找 |

### 4.3 号码路由

**配置**

cm.cfg：

```
[R502]
CCBSSID: 502
A2PPlayerType: operator
Protocol: SMPP

[R506]
CCBSSID: 506
A2PPlayerType: operator
Protocol: SMPP
```

`conf/routing/R506_routingInfo.cfg`：

```
CCBSSID: 506
A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V401)
A2PNumberRouting: (Number=8613800000090;8613800000091,Relay=405,Priority=1);(Number=8613800000092,Relay=403,Priority=2);(Number=8613800000092,Relay=402,Priority=1);(Number=8613800000093,Relay=999,Priority=1);(Number=8613800000094,Relay=402:50,403:50,Priority=1)
```

`conf/routing/R502_routingInfo.cfg`（R502 没有号码规则；4.4 也用这个文件）：

```
CCBSSID: 502
A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V401)
```

R500 用 4.1 的文件（号码 8613800000099 → 405）。R506 的号码规则：

| 号码 | 规则 | 说明 |
|---|---|---|
| 8613800000090、…091 | P1 → 405 | 一条规则写多个号码 |
| 8613800000092 | P1 → 402；P2 → 403 | 同一号码两条规则 |
| 8613800000093 | P1 → 999 | 999 不是任何通道的 ssid |
| 8613800000094 | P1 → 402:50,403:50 | 按权重分流，Relay 写 ssid |

**用例**

| ID | 目的 | 客户 | 输入 | 预期 | 依据 |
|---|---|---|---|---|---|
| RT-07 | 号码路由优先于前缀路由 | R500 | 接收 `8613800000099` | V405 | 号码层（第 2 层）在普通层（第 4 层）之前 |
| RT-07b | 号码路由优先于发送方路由 | R500 | 发送方 `SENDA`，接收 `8613800000099` | V405 | 发送方规则在普通层，号码层先命中 |
| RT-07c | 一条规则写多个号码（第 1 个） | R506 | 接收 `8613800000090` | V405 | `Number` 用 `;` 分隔 |
| RT-07d | 一条规则写多个号码（第 2 个） | R506 | 接收 `8613800000091` | V405 | 同上 |
| RT-07e | 同一号码两条规则 | R506 | 接收 `8613800000092` | V402（P1） | 同一分组 Priority 小的先 |
| RT-07f | 没列出的号码走前缀路由 | R506 | 接收 `8613800000089` | V401 | 号码层没命中，到普通层 |
| RT-07g | 号码路由按客户生效 | R502 | 接收 `8613800000090` | V401 | R502 没有号码规则，用自己的前缀路由 |
| RT-07h | 号码规则指向不存在的通道 | R506 | 接收 `8613800000093` | 路由失败，给客户回失败 DR | 《路由配置说明》3.3：通道不在 cm.cfg，从备份 1 级找，不再看其他主路由规则；R506 没有备份，所以路由失败 |
| RT-07i | 号码规则按权重分流 | R506 | 接收 `8613800000094`，发 30 条 | V402、V403 两个通道都用到 | `Relay=402:50,403:50` |

### 4.4 没有路由、默认路由

**配置**

cm.cfg：

```
[R503]
CCBSSID: 503
A2PPlayerType: operator
Protocol: SMPP
CustomerServiceType: SVC
```

- R502：见 4.3，只有 86 的主路由，没有备份。
- R503：**没有自己的路由文件**。
- `conf/routing/SVC_default_routingInfo.cfg`（业务类型 SVC 的默认路由，不写 CCBSSID）：

```
A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V403)
```

**用例**

| ID | 目的 | 客户 | 输入 | 预期 | 依据 |
|---|---|---|---|---|---|
| RT-13 | 没有路由 | R502 | 接收 `85212345678` | 不路由；给客户回失败 DR；MT 已确认，不会重复消费 | R502 没有 852 的规则，也没有备份 |
| RT-12 | 客户没有路由文件时用默认路由 | R503 | 默认 | V403 | R503 的 `CustomerServiceType: SVC` → `SVC_default_routingInfo.cfg` |

### 4.5 按权重分流

**配置**

cm.cfg：

```
[R501]
CCBSSID: 501
A2PPlayerType: operator
Protocol: SMPP
```

`conf/routing/R501_routingInfo.cfg`：

```
CCBSSID: 501
A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V401:20,V402:80)
```

**用例**（客户 R501）

| ID | 目的 | 输入 | 预期 | 依据 |
|---|---|---|---|---|
| RT-04 | 按比例分流 | 连续发 300 条（msgId 以 `501_` 开头） | 300 条都路由出去；V401 在 30～90 条之间（期望 60） | 按权重随机，允许统计波动 |

### 4.6 黑洞

**配置**

cm.cfg（BH 通道见 3.2，`Role: blackhole`）：

```
[R505]
CCBSSID: 505
A2PPlayerType: operator
Protocol: SMPP

[R504]
CCBSSID: 504
A2PPlayerType: operator
Protocol: SMPP
BlackholePercent: (Percent=100)
```

routing：

```
# conf/routing/R505_routingInfo.cfg
CCBSSID: 505
A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=BH)

# conf/routing/R504_routingInfo.cfg
CCBSSID: 504
A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V401)
```

GmmsConfig：`Gmms/GmmsConfig.properties` 末尾加 `BlackholeSsid=490`（和 BH 通道的 ssid 一致）。

两种黑洞需要的配置不一样：

| 黑洞方式 | 需要的配置 | 用例 |
|---|---|---|
| 路由到 `Role: blackhole` 的通道 | cm 里有 `Role: blackhole` 的通道，路由规则指向它即可，**不需要 `BlackholeSsid`** | RT-16a |
| 按比例进黑洞（`BlackholePercent`） | 客户的 `BlackholePercent`，**还要配 GmmsConfig 的 `BlackholeSsid`**（大于 0）。没配时代码不做按比例黑洞，消息照常路由 | RT-16b、RT-16c |

`BlackholeSsid` 还用于记录被 Core 直接拒绝的 MT 的 rssid（号码规则不符、非 ASCII 超过 140 字节、超过号码 24 小时上限），和这组用例无关。

**用例**

| ID | 目的 | 客户 | 输入 | 预期 | 依据 |
|---|---|---|---|---|---|
| RT-16a | 路由到黑洞通道 | R505 | 默认 | 不写任何出向流；日志有 `[CDR] type=OUT_SUBMIT status=0` 和 `[CDR] type=OUT_DELI_REP status=10000`（像发出并送达一样） | 黑洞通道不真正发出，按黑洞规则写 CDR、回 DR |
| RT-16b | `BlackholePercent=100`：计时窗口内的第一条照常发 | R504 | 默认 | V401 | 黑洞计时窗口（`BlackholeTimerInSec`，默认 60 秒）里的第 1 条不进黑洞 |
| RT-16c | 同一窗口里的下一条进黑洞 | R504 | 默认（紧接 RT-16b） | 不发出，IN_SUBMIT CDR 1 条 | 100% 进黑洞 |

RT-16b、RT-16c 依赖发送顺序和 60 秒的计时窗口，要连续执行。

## 5. 最近一次结果

| 时间 | 环境 | 结果 |
|---|---|---|
| 2026-11-26 | 测试服务器全量回归（提交 `79173c7` 的代码） | 21 项全部通过 |

## 6. 没有覆盖到的

| 内容 | 在哪里 |
|---|---|
| 运营商层、排除条件、外发失败后的备份逐级改道、重试码、级别用完后重试主路由、系统路由替换、路由文件的格式检查 | 《路由规则测试（RoutingRuleIT）》 |
| `RELOAD_ROUTING` 只加载改动的文件、单个文件错误不影响其他 | 《路由重新加载测试（IncrementalRoutingIT）》 |
| `ReplaceContentByTemplate` | CoreContentIT |
| 二次路由（RT-11）、`Relay=V1,V2` 轮流、ENUM / IR21 查运营商（RT-09、RT-10）、按比例黑洞的比例（RT-17） | 2026-11-27 补在《路由补充测试（RoutingExtraIT）》，其中发现二次路由号段配 `all` 不生效；IR21 因测试服务器没有 `dig` 只测了失败路径 |
