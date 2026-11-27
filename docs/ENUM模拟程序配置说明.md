# ENUM 模拟程序配置说明（EnumMock）

> 日期：2026-11-27。程序代码：`regression/test/it/EnumMock.java`。测试服务器上部署在 `/usr/local/gw-enum-mock`。
> 用它测试的用例见《测试用例说明/路由补充测试（RoutingExtraIT）》。

## 1. 用途

网关 Core 在路由前，可以按客户配置查询接收号码属于哪个运营商，再按运营商选通道（`A2POPRouting`）。查询有两种：

| 查询 | 网关怎么查 | 生产上对接 |
|---|---|---|
| ENUM / MNP | HTTP POST JSON | MNP 查询服务（GmmsConfig `mnpqueryurl`） |
| IR21 | 执行系统命令 `dig` 查 DNS 的 NAPTR 记录 | IR21 DNS 服务器（GmmsConfig `DNSAddress`） |

EnumMock 同时模拟这两种服务，按规则文件给出应答。用来在测试环境里验证运营商路由，以及查询失败、超时、错误应答时网关的处理，不需要真实的查询服务。

## 2. 部署（测试服务器）

### 2.1 目录

```
/usr/local/gw-enum-mock/
  classes/it/EnumMock.class、EnumMock$Rule.class   程序（回归构建的 test 类）
  enum-mock.cfg                                     规则文件
  start.sh                                          启动脚本
  log/enum-mock.log                                 日志
```

### 2.2 地址

| 服务 | 地址 | 说明 |
|---|---|---|
| HTTP（ENUM / MNP） | `127.0.0.1:15200` | 任何路径都按查询处理，习惯用 `/mnp/realenumquery2`（和生产一致） |
| DNS（IR21） | `127.0.0.1:15353`，UDP | 只回 `e164.arpa` 的 NAPTR 查询 |

只监听本机地址，外网访问不到。回归用例 RoutingExtraIT 不用这个部署，它在进程内另起一个 EnumMock（HTTP `15201`、DNS `15354`），两者互不影响。

### 2.3 启动、停止、更新

```
# 启动
cd /usr/local/gw-enum-mock && setsid nohup ./start.sh > /dev/null 2>&1 < /dev/null &

# 停止（[i] 防止 pkill 匹配到自己的 shell）
pkill -f "[i]t.EnumMock"

# 确认在运行
ss -ltn | grep ":15200 "; ss -lun | grep ":15353 "
curl -s http://127.0.0.1:15200/health          # 回 ok
```

`start.sh` 的内容：

```bash
#!/bin/bash
cd /usr/local/gw-enum-mock
exec java -Xmx64m -cp classes it.EnumMock --http=127.0.0.1:15200 --dns=127.0.0.1:15353 --config=enum-mock.cfg >> log/enum-mock.log 2>&1
```

启动参数：

| 参数 | 默认 | 说明 |
|---|---|---|
| `--http=地址:端口` | `127.0.0.1:15200` | HTTP 服务 |
| `--dns=地址:端口` | `127.0.0.1:15353` | DNS 服务；写空值（`--dns=`）不启动 DNS |
| `--config=文件` | `enum-mock.cfg` | 规则文件 |

**更新程序**：回归构建后，把 `regression/.work/testall/it/EnumMock*.class` 拷到 `classes/it/`，再停止、启动。

服务器重启后不会自动启动，需要手工执行上面的启动命令。

### 2.4 IR21 需要的 dig

网关的 IR21 查询是执行系统命令 `dig`，机器上必须有 `dig`（`bind-utils` 包）。测试服务器 2026-11-27 已经安装（`yum install -y bind-utils`，版本 9.11.4）。手工检查 DNS 应答：

```
dig @127.0.0.1 -p 15353 -t NAPTR 1.0.0.0.0.0.0.0.8.3.1.6.8.e164.arpa.
```

## 3. 规则文件

### 3.1 写法

一行一条规则，`#` 后面是注释，字段之间用空格或 tab 分隔：

```
号码或前缀   应答   [delay=毫秒]
```

- **按最长前缀匹配**：号码以哪条规则的前缀开头，就用哪条；多条都匹配时用最长的。写完整号码就是只对这一个号码生效。
- 号码前面的 `+` 会先去掉再匹配。
- 没有任何规则匹配时，按 `notfound` 回应。
- **改了就生效**：每次查询前检查文件的修改时间，变了就重新读，不用重启。日志会打一行 `rules loaded from ...: N`。

### 3.2 应答

| 应答 | HTTP 返回 | DNS 返回 | 网关的结果 |
|---|---|---|---|
| `国家码,运营商id,mcc,mnc[,名称]` | 成功（格式见 3.3） | NAPTR 记录（格式见 3.3） | 运营商 = 国家码 × 10000 + 运营商id |
| `fail` | `{"errmsg":"FAIL","data":null}` | NXDOMAIN | 没有运营商，按前缀路由 |
| `empty` | `{"errmsg":"SUCCESS","data":[]}` | NXDOMAIN | 同上 |
| `notfound` | `{"errmsg":"NOT FOUND","data":null}` | NXDOMAIN | 同上 |
| `http500` | HTTP 500，`{"errmsg":"internal error"}` | NXDOMAIN | 同上 |
| `badjson` | HTTP 200，`<html>not json</html>` | NXDOMAIN | 同上 |

`delay=毫秒` 可以加在任何应答后面：等这么久才回。网关等待查询的时间是 GmmsConfig 的 `EnumQueryTimeoutMs`（默认 5000 毫秒，热加载）：ENUM 的连接和读各等这么久，IR21 的 dig 只试 1 次、等这么久。所以 `delay` 大于它就是模拟超时，例如 `delay=35000`：网关等 5 秒后放弃，按前缀路由。

名称不写时用 `OP{运营商id}`。

### 3.3 成功应答的格式

HTTP（规则 `8613800 86,1,460,00,ChinaMobile`，查号码 `8613800000001`）：

```json
{"errmsg":"SUCCESS","data":[{"8613800000001":{"country_code":86,"operator_id":1,"mcc":"460","mnc":"00","operator":"ChinaMobile","type":"mobile"}}]}
```

DNS（同一条规则，查 `1.0.0.0.0.0.0.0.8.3.1.6.8.e164.arpa.`，也就是号码倒序加 `e164.arpa`）：

```
NAPTR 100 100 "u" "E2U+a2p" "!^.*$!a2p:8613800000001@cc86.opid1.mnc00.mcc460.gprs!" .
```

网关从这两种应答里取国家码和运营商 id，得到运营商 860001；HTTP 应答里的 mcc、mnc 还会记成消息的 `rMncMcc`（46000）。

### 3.4 测试服务器上现在的规则

```
# 前缀       应答                              [delay=毫秒]
8613800    86,1,460,00,ChinaMobile
8613801    86,2,460,01,ChinaUnicom
8613803    86,3,460,03,ChinaTelecom
8613811    852,5,454,05,HK5
8613902    fail
8613903    86,1,460,00,ChinaMobile  delay=35000
8613904    http500
8613905    badjson
8613906    empty
```

### 3.5 常用写法

| 想测什么 | 规则 |
|---|---|
| 某个号段属于某个运营商 | `8613800  86,1,460,00,ChinaMobile` |
| 只有一个号码特殊（例如携号转网） | `8613800000123  86,2,460,01,ChinaUnicom`（比号段长，优先） |
| 查到的运营商没有专门的路由，走国家级规则 | 配一个路由里没有的运营商 id，例如 `86,9,460,09` → 860009，匹配 `Opid=860000` |
| 外国运营商，走 `Opid=-2` | `852,5,454,05` |
| 查询失败 / 服务出错 / 应答格式错 | `fail`、`http500`、`badjson`、`empty` |
| 查询慢（不超时） | `86,1,460,00  delay=5000` |
| 查询超时 | `86,1,460,00  delay=35000` |

## 4. HTTP 接口

| 方法 | 路径 | 作用 |
|---|---|---|
| POST | 任何路径（例如 `/mnp/realenumquery2`） | 查询：请求体 `{"number":"..."}`，按规则应答 |
| GET | `/stats` | 每个号码被查了几次，HTTP 和 DNS 分开：`{"http":{"8613800000001":2},"dns":{...}}` |
| POST | `/reset` | 清空计数 |
| GET | `/health` | 回 `ok` |

请求体里没有 `number` 时回 HTTP 400。

手工查询：

```
curl -X POST -H "Content-Type: application/json" -d '{"number":"8613800000001"}' http://127.0.0.1:15200/mnp/realenumquery2
```

日志里每次查询一行，例如：

```
07:28:17.400 [ENUM-MOCK] HTTP 8613800000001 -> ok
07:28:17.438 [ENUM-MOCK] HTTP 8613902000001 -> fail
08:01:02.100 [ENUM-MOCK] DNS 1.0.0.0.0.0.0.0.8.3.1.6.8.e164.arpa type 35 -> NAPTR cc86 opid1
```

网关因为超时先断开时，日志里会有一行 `HTTP error: java.io.IOException: Broken pipe`，这是正常的。

## 5. 网关怎么配置使用它

### 5.1 客户（cm.cfg）

```
[客户块名]
...
SMSOptionEnumQueryRecPrefix: 86                                         # 哪些号段要查（逗号分隔，all 为全部）
SMSOptionEnumRoutingURL: http://127.0.0.1:15200/mnp/realenumquery2      # ENUM 地址；不配时用 GmmsConfig 的 mnpqueryurl
```

要走 IR21：号码要**同时**在 `SMSOptionEnumQueryRecPrefix` 和下面的 `SMSOptionIR21EnumQueryRecPrefix` 里，这时用 IR21，不再查 ENUM：

```
SMSOptionIR21EnumQueryRecPrefix: 86
SMSOptionEnumIR21RoutingURL: 127.0.0.1 -p 15353                         # dig 的服务器；不配时用 GmmsConfig 的 DNSAddress
```

网关把 `SMSOptionEnumIR21RoutingURL` 原样拼进命令 `dig @{值} +time={秒} +tries=1 -t NAPTR {号码倒序}.e164.arpa.`，所以可以带 `-p 端口`。生产上 DNS 服务器用 53 端口时，只写地址即可。

改了 cm.cfg 后执行 `RELOAD_CUSTOMER` 生效。

### 5.2 全局默认地址（GmmsConfig）

| 配置 | 作用 |
|---|---|
| `mnpqueryurl` | 客户没配 `SMSOptionEnumRoutingURL` 时用的 ENUM 地址 |
| `DNSAddress` | 客户没配 `SMSOptionEnumIR21RoutingURL` 时用的 IR21 DNS 服务器 |

测试服务器上这两项现在指向生产的查询服务（`mnpqueryurl=http://172.31.216.59:5200/mnp/realenumquery2`、`DNSAddress=172.17.183.143`），测试服务器访问不到。要让所有客户都用模拟程序，可以把它们改成 `http://127.0.0.1:15200/mnp/realenumquery2` 和 `127.0.0.1 -p 15353`；只想让个别客户用，按 5.1 在客户上配，不用改全局。

### 5.3 路由（conf/routing）

查到运营商后，按 `A2POPRouting` 的 `Opid` 选通道。`Opid` 有三种写法，由具体到宽泛：

```
A2POPRouting: (Priority=10,Opid=860001,Relay=V1);(Priority=10,Opid=860000,Relay=V2);(Priority=10,Opid=-2,Relay=V3)
A2POPBackupRouting: (Priority=10,Opid=860001,Relay=V4)
A2PMainRouting: (Priority=10,PhonePrefix=86,Relay=V5)
```

| 查到的运营商 | 选的通道 |
|---|---|
| 860001（具体运营商） | V1 |
| 860002（中国，但没有专门的规则） | V2（国家级 `国家码*10000`） |
| 8520005（别的国家） | V3（`-2` = 所有运营商） |
| 没查到（失败、超时、号码不存在） | V5（普通前缀路由） |
| 860001，V1 发送失败（失败码在 V1 的 `SMSOptionSubmitRetryCode` 里） | V4（运营商层的备份） |

只要查到了运营商，运营商层就压过普通前缀路由，哪怕普通路由的前缀更长。更多规则见《路由配置说明》。

### 5.4 怎么确认生效

| 看哪里 | 内容 |
|---|---|
| Core 日志 | `[MT-ROUTE] ossid=971 rssid=931 by op opid=860001 priority=10 file=C971`（运营商层）；`... by main prefix=86 ...`（普通层） |
| EnumMock | `curl http://127.0.0.1:15200/stats` 看这个号码有没有被查；`log/enum-mock.log` 看给了什么应答 |
| CDR | 走运营商层路由的消息，CDR 的运营商列是运营商号（860001 → 1）；走普通层的是 0 |

## 6. 排查

| 现象 | 原因和处理 |
|---|---|
| 网关没有查询（`/stats` 里没有这个号码） | 客户没配 `SMSOptionEnumQueryRecPrefix`，或者号码不在这个号段里；改了 cm.cfg 后没执行 `RELOAD_CUSTOMER` |
| 查了但还是按前缀路由 | 应答不是成功（看 EnumMock 日志里的 `->` 后面）；或者路由文件里没有匹配这个运营商的 `A2POPRouting` 规则 |
| IR21 一直失败 | 机器上没有 `dig`（`which dig`）；`SMSOptionEnumIR21RoutingURL` 的地址或端口不对，可以按 2.4 手工 dig 一下 |
| 一条消息过了几秒才路由 | 查询超过了 `EnumQueryTimeoutMs`（默认 5 秒）：规则里的 `delay` 太大，或者 EnumMock 没在运行、端口不通（连接也按这个时间超时） |
| `curl` 连不上 | EnumMock 没启动（2.3），或者端口被占 |

## 7. 限制

- 成功应答的 JSON 结构是固定的，只能改国家码、运营商 id、mcc、mnc、名称，`type` 固定是 `mobile`。不能原样返回一段自定义的 JSON，所以“缺字段”“字段类型不对”这类异常应答测不了。
- 同一个号码只有一种应答，不能按查询次数变化，例如第 1 次失败、第 2 次成功。
- DNS 只支持 `e164.arpa` 的 NAPTR 查询，其他类型都回 NXDOMAIN；只走 UDP，没有 TCP。
- 只用于测试环境，不要让生产网关指向它。
