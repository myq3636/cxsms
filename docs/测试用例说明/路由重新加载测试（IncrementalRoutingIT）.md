# 路由重新加载测试（IncrementalRoutingIT）

> 日期：2026-11-27。用例代码：`regression/test/it/IncrementalRoutingIT.java`，套件 `IncrementalRoutingIT`（`regression/suites.txt`，it 模式），28 项检查。
> 重新加载的规则见《配置项说明》2.2（`RELOAD_ROUTING`、`ConfigFileStableCheckMs`、`ReloadCustomerMaxRemovePercent`）和 6.1（路由文件）。同组：《路由优先级测试（CoreRouteIT）》《路由规则测试（RoutingRuleIT）》。

## 1. 测什么

改了路由文件后发 `RELOAD_ROUTING`：

- 只加载改动过的文件；没有改动时什么都不做；
- 一个文件写错只影响它自己，它保留原来的路由，其他文件的改动照常生效；
- 一次删掉太多文件、文件还在写，都要拒绝或推迟；
- `RELOAD_ROUTING:-f` 全部重新加载；
- Redis 里的“通道 → 客户”索引跟着更新；
- `RELOAD_CUSTOMER`（只改 cm.cfg）不动路由。

## 2. 怎么跑、怎么判定

### 2.1 运行

```
regression/run.sh IncrementalRoutingIT
cat regression/.work/results/IncrementalRoutingIT.txt
```

- 环境：测试服务器上的回归环境（JDK 17，本地 Redis 16379）。进程内启动 Core 的配置部分（`boot()`），**不消费 MT**，也不需要数据库。
- 用回归环境自带的 cm.cfg 和两个路由文件（第 3 节）；用例结束时把这两个文件和 cm.cfg 恢复原样，并删掉用例新建的 `CUSTX*`、`CUST9*` 文件。

### 2.2 怎么操作、怎么看结果

| 动作 | 做法 |
|---|---|
| 改路由文件 | 直接写 `conf/routing/{名称}_routingInfo.cfg`，并把文件的修改时间设成 5 秒前，这样 reload 时文件已经算“写完”（不用等 `ConfigFileStableCheckMs`） |
| 发 reload 命令 | 向 Redis 频道 `channel:gmms:control` 发 `RELOAD_ROUTING`、`RELOAD_ROUTING:-f` 或 `RELOAD_CUSTOMER`，等日志出现完成或拒绝的那一行 |
| 看加载结果 | Redis 哈希 `config:files:CoreEngine1:1` 的字段：`routing.result`（`ok` / `partial` / `failed`）、`routing.failed`（失败的文件和原因）、`routing.version`（内容版本）、`routing.failReason`（整次被拒绝的原因） |
| 看加载了几个文件 | 日志 `Reload routingInfo completed ... loaded=N, failed=N, removed=N, files=N, action=...`；没有改动时 `no routing file changed` |
| 看实际路由 | `mainRouteRssid(客户 ssid, 发往 8613800000000 的 MT)`：这个客户现在主路由选到的通道 ssid |
| 看通道索引 | `config:file:routing:{版本}:channel`：通道名 → 用它的客户列表（`customer`、`ssid`、`routes`）；`config:file:routing:{版本}:channelRoutes`：`通道名|客户名` → 路由明细（`type`、`PhonePrefix` 等） |

## 3. 配置

### 3.1 cm.cfg（回归环境自带，用例不改）

用到的块：

```
[CUST8888]
CCBSSID: 8888
A2PPlayerType: operator
Protocol: SMPP
SMSOptionRecipientMaxSendCountIn24H: 100

[VENDOR8]
CCBSSID: 8
A2PPlayerType: operator
Protocol: SMPP

[CUST7001]
CCBSSID: 7001
A2PPlayerType: operator
Protocol: SMPP

[VENDOR9]
CCBSSID: 9
A2PPlayerType: operator
Protocol: SMPP
SMSOptionCoreProcessorThrottlingNum: 2
CoreHoldMaxSeconds: 5

[VENDOR12]
CCBSSID: 12
A2PPlayerType: operator
Protocol: SMPP
```

`VENDOR31` **不在 cm.cfg 里**（4.6 用来看索引是否照样记录规则里写的通道名）。`CUSTX1`～`CUSTX8`、`CUST9001` 也没有 cm 块，只有路由文件。

### 3.2 路由文件（回归环境自带）

```
# conf/routing/CUST8888_routingInfo.cfg
CCBSSID: 8888
A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=VENDOR8)

# conf/routing/CUST7001_routingInfo.cfg
CCBSSID: 7001
A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=VENDOR9)
```

### 3.3 GmmsConfig 和发送方名单

| 配置 | 值 | 作用 |
|---|---|---|
| `ConfigFileStableCheckMs` | 默认 500 毫秒 | 文件修改时间距现在不到这个值，要等它不再变化才读（4.5） |
| `ReloadCustomerMaxRemovePercent` | 默认 20% | 一次删掉的路由文件超过当前文件数的这个比例，整次拒绝（4.4） |
| 发送方黑白名单 | 不涉及 | 这组只看路由文件的加载 |

## 4. 用例

用例按顺序执行，后一步基于前一步的文件状态。

### 4.1 启动和“没有改动”

| ID | 目的 | 操作 | 预期 |
|---|---|---|---|
| IR-01a | 启动时发布路由 | 启动 | `routing.version` 有值，`routing.failed` 为空 |
| IR-01b | 通道索引：客户 | — | `VENDOR8` 下有 `CUST8888`，ssid 8888 |
| IR-01c | 通道索引：路由明细 | — | `CUST8888` 在 `VENDOR8` 下有 1 条路由，`type=A2PMainRouting`，`PhonePrefix=86` |
| IR-01d | 通道索引：只记实际用到的通道 | — | `CUST7001` 只在 `VENDOR9` 下，不在 `VENDOR8` 下 |
| IR-02 | 没有改动 | `RELOAD_ROUTING` | 日志 `no routing file changed`；路由表还是同一个对象（没有重建）；版本不变 |

### 4.2 只加载改动的文件

**改动**：CUST8888 的 `Relay=VENDOR8` 改成 `Relay=VENDOR12`，然后 `RELOAD_ROUTING`。

| ID | 目的 | 预期 |
|---|---|---|
| IR-03a | 只读了一个文件 | 日志 `loaded=1,`、`failed=0` |
| IR-03b | 只有改了的客户变了 | CUST8888 的主路由是 12（VENDOR12）；CUST7001 仍是 9 |
| IR-03c | 路由表换成新的 | 路由表对象换了；版本号变了 |
| IR-03d | 通道索引跟着变 | `VENDOR12` 下有 CUST8888，`VENDOR8` 下没有了 |
| IR-03e | 旧版本的索引清掉 | `config:file:routing:{旧版本}:channel` 不存在了 |

### 4.3 写错的文件只影响自己

每一轮同时改两个文件：CUST8888 写成下面的错误内容；CUST7001 正常修改（第 1、3 轮改成 `Relay=VENDOR12`，第 2、4 轮改回 `Relay=VENDOR9`）。然后 `RELOAD_ROUTING`。

| ID | CUST8888 写成 | 预期 |
|---|---|---|
| IR-04a | 空文件 | `routing.result=partial`；`routing.failed` 里有 CUST8888；CUST8888 仍走 12（保留上次的路由）；CUST7001 的改动生效 |
| IR-04b | 没有 CCBSSID：`A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=VENDOR8)` | 同上 |
| IR-04c | CCBSSID 不是数字：`CCBSSID: abc` + 上一行 | 同上 |
| IR-04d | 属性名写错：`CCBSSID: 8888` + `A2PMainRouting: (Priority=30,PhonePrefix=86,Relayy=VENDOR8)` | 同上（等于没有 Relay） |

| ID | 操作 | 预期 |
|---|---|---|
| IR-05 | CUST7001 恢复原样，CUST8888 改回正确的 `Relay=VENDOR12`，`RELOAD_ROUTING` | `routing.result=ok`，`routing.failed` 清空；内容和 4.2 之后一样，所以版本号也和那时一样 |
| IR-06a | 新建一个错误的文件 `CUST9001_routingInfo.cfg`（内容 `CCBSSID: x`） | `partial`，`routing.failed` 里有 CUST9001；其他文件不变，版本不变 |
| IR-06b | 删掉 CUST9001 的文件 | `ok`，`routing.failed` 清空 |

### 4.4 新增和删除文件

新建 8 个文件 `CUSTX1`～`CUSTX8`，内容都是（ssid 从 7101 到 7108）：

```
CCBSSID: 710n
A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=VENDOR9)
```

| ID | 操作 | 预期 |
|---|---|---|
| IR-07a | 新建 8 个文件，`RELOAD_ROUTING` | `loaded=8,`；`VENDOR9` 下有 9 个客户（CUST7001 + 8 个新的） |
| IR-07b | 删掉 CUSTX8 | `removed=1`；VENDOR9 下没有 CUSTX8 了；`ok` |
| IR-07c | 一次删掉 CUSTX1～CUSTX5（9 个文件里删 5 个） | 整次被拒绝：`routing.result=failed`，`routing.failReason` 说明原因；版本不变，CUSTX1 还在（超过 `ReloadCustomerMaxRemovePercent` 的 20%） |

之后用例把 CUSTX1～CUSTX5 写回去。

### 4.5 文件还在写

| ID | 操作 | 预期 |
|---|---|---|
| IR-08a | 一个线程每 50 毫秒改写一次 CUSTX1（只改末尾的注释行），这时 `RELOAD_ROUTING` | `partial`，`routing.failed` 里有 CUSTX1（文件一直在变，这次算失败），其他文件照常 |
| IR-08b | 停止写，等 0.8 秒后再 `RELOAD_ROUTING` | `ok` |

### 4.6 通道索引：权重、备份、发送方规则

**改动**：`CUSTX2_routingInfo.cfg` 改成：

```
CCBSSID: 7102
A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=VENDOR9:20,VENDOR12:80);(Priority=10,PhonePrefix=852,Sender=ABC,Relay=VENDOR31)
A2PBackupRouting: (Priority=30,PhonePrefix=86,Relay=VENDOR8)
```

| ID | 目的 | 预期 |
|---|---|---|
| IR-09a | 按权重的两个通道 | CUSTX2 同时出现在 VENDOR9 和 VENDOR12 下 |
| IR-09b | 备份路由 | CUSTX2 在 VENDOR8 下，路由明细的 `type=A2PBackupRouting` |
| IR-09c | 发送方规则 | CUSTX2 在 VENDOR31 下（VENDOR31 不在 cm.cfg，索引照样按规则里写的通道名记） |

### 4.7 全部重新加载、升级、RELOAD_CUSTOMER

| ID | 目的 | 操作 | 预期 |
|---|---|---|---|
| IR-10 | 强制全部重新加载 | `RELOAD_ROUTING:-f` | 日志 `action=-f`，`loaded` 等于 `files`（每个文件都读了） |
| IR-12 | 旧版本没有索引（升级前写的） | 删掉当前版本的 `:channel`、`:channelRoutes`，再 `RELOAD_ROUTING:-f` | 版本号不变（内容没变），索引重新写出来，CUSTX2 在 VENDOR9 下 |
| IR-11 | `RELOAD_CUSTOMER` 不动路由 | 把 CUST8888 的路由文件恢复成 `Relay=VENDOR8`（不发 `RELOAD_ROUTING`），发 `RELOAD_CUSTOMER` | 版本不变；日志没有新的 `Start reload routingInfo`；CUST8888 仍走 12（路由只由 `RELOAD_ROUTING` 重建） |

## 5. 最近一次结果

| 时间 | 环境 | 结果 |
|---|---|---|
| 2026-11-26 | 测试服务器全量回归（提交 `79173c7` 的代码） | 28 项全部通过 |

## 6. 没有覆盖到的

| 内容 | 说明 |
|---|---|
| 多个 Core 节点同时 reload | 这组只看一个进程内的节点（`CoreEngine1:1`）；多个 Core 进程见第 7 节（在测试服务器上测） |
| 通道改名后路由跟着走 | 在 RenameReloadIT（路由指向已删除或改名的通道，按未路由失败 2120） |
| `smsui routingInfo -a`（管理命令转成 `RELOAD_ROUTING:-a`） | 这里直接发 Redis 命令，没有经过 smsui |
| reload 之后正在路由的 MT 用新表还是旧表 | 没有覆盖（路由表是建好后一次替换，设计上不会出现一半新一半旧） |
| `{业务类型}_default` 文件的增量加载 | 没有单独测 |

## 7. 多个 Core 同时重新加载（测试服务器）

> 检查程序：`regression/test/it/MultiCoreRoutingReloadCheck.java`。不在 `suites.txt` 里：它操作测试服务器上**正在运行**的网关（路由目录、GmmsConfig、Redis 控制频道），要手工执行。

### 7.1 做法

- 测试服务器上有 3 个 Core（CoreEngine1～3，节点号 0）。路由目录 `/usr/local/a2p/conf/routing` 平时只有 `RTSVCS_default_routingInfo.cfg`。
- 检查程序加一个测试路由文件 `ZZMCTEST_routingInfo.cfg`（cm.cfg 里**没有**的假客户，CCBSSID 99901，不影响任何现有客户），写文件用“先写临时文件再改名”的方式（《配置项说明》推荐的做法），然后向 Redis 控制频道 `channel:gmms:control` 发命令。
- 每一步都对比 3 个 Core：
  - `config:files:CoreEngine{n}:0` 的 `routing.version`（内容版本）、`routing.result`、`routing.failed`、`routing.failReason`、`routing.loadedAt`；
  - 每个 Core 实际生效的规则 `routingInfoCoreEngine{n}.Rules:ZZMCTEST`（字段 `main:0:86:10` 的 `Relay`）。
- 开始前备份 `GmmsConfig.properties` 为 `GmmsConfig.properties.bak.multicore`，结束时无论成败都还原，删掉测试文件。

运行（在测试服务器的回归目录，用回归构建出的 class）：

```
cd /tmp/newsms_new
export REDISCLI_AUTH=$(grep "^Redis_password" /usr/local/a2p/Gmms/GmmsConfig.properties | cut -d= -f2- | tr -d "\r ")
java -cp "regression/.work/testall:jar/*" -Dredis=172.19.4.38:8386 -Da2p=/usr/local/a2p/ \
     -Dcores=CoreEngine1:0,CoreEngine2:0,CoreEngine3:0 -Dch1=RTCH01:9621 -Dch2=RTCH02:9622 it.MultiCoreRoutingReloadCheck
```

`ch1`、`ch2` 是 cm.cfg 里现有的两个通道（块名:ssid），测试路由指向它们。

### 7.2 用例与结果（2026-11-27，13 项全部通过）

| ID | 操作 | 预期 | 结果 |
|---|---|---|---|
| MC-00a/b | 开始前 | 没有测试文件；3 个 Core 版本一致（`c21d29c0…`），都是 ok | 通过 |
| MC-01a | 新增 `ZZMCTEST`（`A2PMainRouting: (Priority=10,PhonePrefix=86,Relay=RTCH01)`），`RELOAD_ROUTING` | 3 个 Core 换成同一个新版本，都是 ok | 通过：约 0.7 秒全部完成；3 个 Core 的 `loadedAt` 相差 1 毫秒 |
| MC-01b | 看每个 Core 的规则 | 都是 `main:0:86:10` → 9621 | 通过 |
| MC-02 | 改成 `Relay=RTCH02` | 3 个 Core 都换成新版本，规则 → 9622 | 通过 |
| MC-03 | 改成错误内容（`CCBSSID: abc`） | 3 个 Core 都是 partial，`routing.failed` 都以 `ZZMCTEST:` 开头（“CCBSSID 必须是整数”）；版本不变；规则保留 9622 | 通过 |
| MC-04 | 改回 MC-02 的内容 | 3 个 Core 都是 ok，版本回到 MC-02 的值（内容相同，版本相同） | 通过 |
| MC-05 | `RELOAD_ROUTING:-f` | 3 个 Core 都重新读所有文件，ok，版本不变 | 通过 |
| MC-06 | 删除测试文件 | 2 个文件删 1 个（50%），超过 `ReloadCustomerMaxRemovePercent`（20%）：3 个 Core 都拒绝（`failed`，原因 `1 of 2 routing files would be removed, limit 20%`），规则保留 | 通过 |
| MC-07 | GmmsConfig 加 `ReloadCustomerMaxRemovePercent=100`，`RELOAD_GMMS_CONFIG`，再 `RELOAD_ROUTING` | 3 个 Core 都删掉测试路由，版本回到开始前的 `c21d29c0…` | 通过（也验证了 GmmsConfig 热加载对 3 个 Core 都生效） |
| MC-08 | 收尾 | GmmsConfig 和测试前一样 | 通过（md5 不变） |

### 7.3 没有覆盖到的

| 内容 | 说明 |
|---|---|
| 重新加载时正在路由的 MT | 测试服务器上没有流量；路由表是建好后一次替换，设计上不会出现一半新一半旧 |
| 某个 Core 没收到命令（例如重启中） | 它重启后会读最新的文件；没有测 |

