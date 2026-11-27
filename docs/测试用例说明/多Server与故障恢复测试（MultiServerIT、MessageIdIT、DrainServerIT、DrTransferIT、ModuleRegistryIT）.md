# 多 Server 与故障恢复测试（MultiServerIT、MessageIdIT、DrainServerIT、DrTransferIT、ModuleRegistryIT、MessageIdCheck）

> 日期：2026-11-27。用例代码在 `regression/test/it/`（MessageIdCheck 在 `regression/test/com/king/message/gmms/`，unit 模式），都在 `regression/suites.txt`：
>
> | 套件 | 模式 | 项数 | 内容 |
> |---|---|---|---|
> | MultiServerIT | it-multi | 47 | 两个 Server 进程加载不同的客户（`SmppServerModules`）、换 Server、kill 后重启 |
> | MessageIdIT | it-multi | 24 | `MessageIdFormat=v2`：两个 Server 进程的消息 ID，kill -9 后不重复 |
> | MessageIdCheck | unit | 57 | 消息 ID 的生成公式（v1、v2、十六进制） |
> | DrainServerIT | it-multi | 23 | `a2pctl drain / remove / cancel` 下线一个 Server |
> | DrTransferIT | it-multi | 19 | `a2pctl drain-dr`：Server 被 kill -9 后把它的 DR 流移走 |
> | ModuleRegistryIT | it-multi | 46 | 模块登记：`a2pctl init`、状态、心跳、模块号冲突 |
>
> Core 选 DR 发往哪个 Server（心跳检查）在《DR流程测试》的 DrLivenessIT 部分。

## 1. 测什么

生产上有多个 Server 进程（MultiSmppServer1、MultiSmppServer2），客户可以用 `SmppServerModules` 限定只能连哪些 Server。客户的 DR 要回到它绑定着的 Server：Core 按会话登记（`smpp:server:*`）和 Server 心跳选目标，写进 `stream:dr:pending:{客户}:{Server模块}:{分片}`。

| 场景 | 要保证的 |
|---|---|
| 客户只能连某些 Server | 不在列表里的 Server 拒绝绑定（0x0D） |
| DR 回到哪 | 消息从哪个 Server 进来就回哪；客户换了 Server，DR 跟着走 |
| `RELOAD_CUSTOMER` 改 `SmppServerModules` | 客户从旧 Server 断开、在新 Server 上绑定；别的客户不受影响 |
| Server 被 kill | 只能连它的客户等它重启；不受限的客户连到另一个 Server，DR 跟过去 |
| 消息 ID | 两个 Server 同时发 ID 不重复；kill -9 重启后不重用 |
| 下线一个 Server | 新连接拒绝，逐个关闭会话，没发完的 DR 退回 Core 重发（11005） |
| Server 崩溃后的 DR | 工具把死掉的 Server 的 DR 流移给在线的 Server，每条只送一次 |
| 模块登记 | 一个模块名同时只能在一个节点上跑；模块号不冲突 |

## 2. 怎么跑、怎么判定

```
regression/run.sh MultiServerIT     # 其余套件同样
cat regression/.work/results/MultiServerIT.txt
```

- 环境：测试服务器上的回归环境（JDK 17，本地 Redis 16379）。
- 另起两个**真实的 Server 进程**（MultiSmppServer1、MultiSmppServer2，共用 Redis）；本程序扮演 Core，**用产品代码**选 DR 的目标 Server 并写 DR；用原始 SMPP 客户端绑定、提交、收 DR（同《Server功能测试》）。
- DrainServerIT、DrTransferIT、ModuleRegistryIT 运行真实的 `A2pCtl` 工具；ModuleRegistryIT 用生产的启动器（`GmmsLauncher`）和生产格式的模块列表起进程。

## 3. 公共配置

### 3.1 MultiServerIT 的客户

```
[MA]
CCBSSID: 120
A2PPlayerType: operator
Protocol: SMPP
ConnectionType: MC
WindowSize: 20
SmppServerModules: MultiSmppServer1
IncomingConnection: (ConnName=MA_c1,userName=ma,password=pw120,URL=127.0.0.1,SessionNumber=20,MinReceiverNumber=1,MaxReceiverNumber=5,BindMode=TRX,Version=V34)
```

| 块 | CCBSSID | `SmppServerModules` |
|---|---|---|
| MA | 120 | MultiSmppServer1 |
| MC | 121 | MultiSmppServer2 |
| MB | 122 | MultiSmppServer1,MultiSmppServer2 |
| MN | 123 | 不配（两个都能连） |
| MD | 124 | MultiSmppServer1（MS-12 改成两个，再改回来） |
| ME | 125 | MultiSmppServer2（MS-13 由重新加载加进来） |

### 3.2 DrainServerIT、DrTransferIT 的客户

| 套件 | 客户 | 绑定 / 限定 |
|---|---|---|
| DrainServerIT | X（130） | 只绑定在 Server 1 |
| DrainServerIT | Y（131） | 两个 Server 都绑定 |
| DrainServerIT | SP（132） | `SmppServerModules` 只有 Server 1 |
| DrTransferIT | X | 只绑定在 Server 1（被 kill 的那个），不受限 |
| DrTransferIT | Y | 两个都绑定 |
| DrTransferIT | Z | `SmppServerModules` 只有 Server 1 |

### 3.3 GmmsConfig

| 配置 | 值 | 套件 |
|---|---|---|
| `ModuleStatusHeartbeatSeconds` | 1 | 全部 |
| `ModuleStatusTTLSeconds` | 5（ModuleRegistryIT 是 3） | 全部 |
| `ModuleDrainMaxSeconds` | 30 | DrainServerIT |
| `ModuleRegistryEnable` | true（子进程） | DrainServerIT、ModuleRegistryIT |
| `MessageIdFormat` | 不配（默认 v2）；ID-01e 在一个模块上配 `{模块}.MessageIdFormat=v1` | MessageIdIT |

`conf/a2p_modules.conf`：两个 Server 和用例需要的 Core、Client（ModuleRegistryIT 用一份 10 个模块的生产格式列表，其中一行被注释掉）。

## 4. 用例

### 4.1 客户限定在某些 Server（MultiServerIT）

| ID | 情况 | 预期 |
|---|---|---|
| MS-01a/b | MA（只有 Server 1） | Server 1 能绑定；Server 2 拒绝（0x0D） |
| MS-01c/d | MC（只有 Server 2） | 反过来 |
| MS-01e/f | MB（两个都列）、MN（不配） | 两个都能绑定 |
| MS-02 | 会话登记 | 每个客户登记在自己的 Server 下 |
| MS-03a/b | 提交 | 消息记下是从哪个 Server 进来的 |

### 4.2 DR 回到哪（MultiServerIT）

| ID | 情况 | 预期 |
|---|---|---|
| MS-04a/b | MA、MC 的 DR | 分别从 Server 1、Server 2 发出 |
| MS-05a/b | MB 两个都连着 | 消息从哪个进来，DR 就从哪个发出 |
| MS-06 | MB 现在只连着 Server 2，消息是从 Server 1 进来的 | DR 跟到 Server 2 |
| MS-07 | 客户从 Server 1 重连到 Server 2 | DR 跟过去 |

### 4.3 重新加载改 Server（MultiServerIT）

| ID | 情况 | 预期 |
|---|---|---|
| MS-10a～c | MA 改成只有 Server 2 | 在 Server 2 能绑定，Server 1 拒绝 |
| MS-10d～f | — | 它在 Server 1 上的会话被关掉；两个 Server 上的其他客户不受影响 |
| MS-10g～j | — | 客户重连到新 Server；Server 1 收的消息的 DR 送到 Server 2 上的客户；在新 Server 上提交正常；Core 找不到 MA 在 Server 1 上的活跃会话了 |
| MS-11a/b | 改回 Server 1 | Server 1 能绑定，Server 2 拒绝 |
| MS-12a～d | MD 放宽到两个，再收窄到 Server 1 | 放宽时会话重建、客户重连，Server 2 也能绑定；收窄后 Server 2 拒绝，Server 1 照常 |
| MS-13a～d | 重新加载加一个只在 Server 2 的客户 ME；再删掉 | 在 Server 2 能绑定、Server 1 拒绝；删掉后拒绝（0x0F），会话关掉 |
| MS-14a/b | 改密码 | 旧密码被拒（0x0E），新密码接受 |

### 4.4 Server 被 kill（MultiServerIT）

| ID | 情况 | 预期 |
|---|---|---|
| MS-20a/b | kill Server 1 | 它的客户断开；Server 2 上的客户不受影响 |
| MS-21a | MA（只有 Server 1） | 不能自己换到 Server 2（0x0D） |
| MS-21b/c | MN（不受限） | 重连到 Server 2；死掉的 Server 收的消息的 DR 送到 Server 2 上的 MN |
| MS-22a | 只能连死掉的 Server 的客户 | Core 找不到 DR 流（走 DR 重试策略） |
| MS-22b～d | 重启 Server 1 | Core 又找到它的 DR 流；MA 重新绑定；之后的 DR 送到 |

### 4.5 消息 ID（MessageIdIT、MessageIdCheck）

**两个 Server 进程（MessageIdIT）**：

| ID | 情况 | 预期 / 实测 |
|---|---|---|
| ID-02d/e | 模块号 | MultiSmppServer2 是 2 号；没配 `MessageIdFormat` 时默认 v2，第一条之前就生效 |
| ID-04a～d | 两个 Server 各 1500 条 | 每条都有回应；10 位数字，第一位是模块号（`1694010000`、`2694010000`）；不重复；同一模块内递增 |
| ID-14j～n | 十六进制 ID 的客户，两个 Server | 8 位十六进制，最高位是模块号（`14bf3670`、`24bf3e40`）；不重复；DR 里和以前一样按十进制写；位置存在 Redis；kill -9 重启后不重用 |
| ID-11 | 真实的 deliver_sm | 带着真实 submit_sm_resp 的 ID |
| ID-06b～d | K=1，300 个 ID | 位置几秒内就保存；kill -9 重启后有标记，不重用 |
| ID-08 | 没有保存的位置 | 照常启动并告警 |
| ID-06z | 对照：没有标记 | 会重用 |
| ID-01e | 某个模块 `MessageIdFormat=v1` | 这个模块回到旧格式（`5662170001`） |
| ID-09a/b | 同类模块里第 10 个 | 拒绝启动（exit 255）；第 9 个能通过这项检查 |
| ID-16a～d | 模块号怎么来 | 取名字末尾的数字；列表里 Server1 被注释掉时 Server3 是 3 号（不是位置 2）；列表里只有本模块时还是 2 号；名字里没有数字时按在列表里的位置 |

**生成公式（MessageIdCheck）**，节选：

| ID | 情况 | 预期 |
|---|---|---|
| ID-01a～f | v1（旧格式） | 和以前完全一样：时间加模块的公式、`generateCommonStringID`、`generateCommonMsgID`、SMPP 的 msgId、`generateHexID` |
| ID-14z | 对照：v1 的 4 号模块 | 放不进 2 位模块号，跑进时间里（v1 的问题） |
| ID-12z | 对照：v1 两个 HTTP 模块同一秒、同一序号 | 同一个 ID（v1 的问题） |
| ID-02a～c | v2 | 第一位是模块号，后面是计数器；10 位十进制；Client1 错开半个周期 |
| ID-13 | `SMSOptionSupportMaxInMsgID` | v2 下也是 13 位 |
| ID-14a/b | v2 十六进制，模块 1～9 | 8 位十六进制，模块号在最高 4 位；DR 里按十进制最多 10 位 |

### 4.6 下线一个 Server（DrainServerIT）

| ID | 情况 | 预期 |
|---|---|---|
| DS-00a～c | 初始化；X 只在 Server 1，Y 两个都连 | 对照：Y 从 Server 1 进来的消息的 DR 回 Server 1 |
| DS-01a | drain Server 1，有客户 SP 只能用 Server 1 | 拒绝，列出 SP（132） |
| DS-01b | 改配置后再 drain | DRAINING，提示处理负载均衡 |
| DS-01c | 再 drain 最后一个在线的 Server | 拒绝 |
| DS-02a～c | — | Core 知道它在下线；Y 的 DR 改发 Server 2；X 只连着 Server 1，它还连着时 DR 仍发 Server 1 |
| DS-03 | 新连接连 Server 1 | 关闭 |
| DS-04a～c | — | 阶段 `CLOSE_SESSIONS`，一个一个关掉 Server 1 上的 2 个会话；Server 2 上的会话不动 |
| DS-05a | 会话关闭时发出了但没回应的 DR | 以 11005 退回 Core 重发 |
| DS-05b | 留给 Server 1 但没有会话的 DR | 以 11005 退回 Core |
| DS-05c | 什么都没剩 | `DONE` |
| DS-06a～c | X 绑定到 Server 2 | X 的 DR 发 Server 2 并送到 |
| DS-07a/b | cancel | ACTIVE，Server 1 又接受连接 |
| DS-08a/b | remove，再 cancel | DRAINING、下线完、提示停止；cancel 后 ACTIVE，没有残留的操作 |

### 4.7 Server 崩溃后移 DR（DrTransferIT）

| ID | 情况 | 预期 |
|---|---|---|
| DT-00 | X、Z 在 Server 1，Y 两个都连 | — |
| DT-01a/b | Y 的 5 条 DR 从 Server 1 发出、一直没回应，然后 kill -9 Server 1 | Server 1 的流里剩下：5 条待确认、5 条没读、X 的 3 条、Z 的 2 条、1 条 MO，共 16 条 |
| DT-02 | 心跳还在时运行工具 | 拒绝，提示用 drain |
| DT-03 | `--dry-run`（心跳过期后） | 14 条可以移到 Server 2，Z 的 2 条没有 Server 可去，什么都不改 |
| DT-04a～e | 运行 | 14 条移到 Server 2，报告 Z 的 2 条留下；Y 在 Server 2 上收到原来待确认的 5 条和没读的 5 条，每条一次（加 MO 共 11 次）；Server 1 的流里只剩 Z 的 2 条；清空的流和门铃标记删掉 |
| DT-04f/g | remove Server 1 | 还有 Z 的 2 条时拒绝，`--force` 也拒绝 |
| DT-05 | X 的 3 条 | 照 Core 的做法发到 Server 2，X 不在那里，以 11005 退回 Core 重试 |
| DT-06a～f | Z 也允许 Server 2 并绑定上，再运行一次 | Z 的 2 条移过去，每条收到一次；Server 1 的流和门铃都没了；第三次运行什么都找不到；可以 remove |

### 4.8 模块登记（ModuleRegistryIT）

| ID | 情况 | 预期 |
|---|---|---|
| RG-01a/b | 默认关 | Redis 里什么都没有；用文件里的列表 |
| RG-02a～f | `a2pctl init` | 之前 status 提示没初始化；导入生产列表 10 个模块（注释的行不导入），带角色、类型、节点、状态；再 init 不变；status 列出 |
| RG-03a/b | 列表里模块号重复、超过 9 | 拒绝，什么都不写 |
| RG-04a/b | 模块加载 | 节点被纠正，版本号；和文件列表一样 |
| RG-05a～e | 一个模块 DRAINING，`RELOAD_MODULES` | 从列表里去掉，选通道时不选它；它的连接信息和号码保留；回到 ACTIVE 又在列表里；下线中的模块自己还看得到自己 |
| RG-06a～d | 不发 `RELOAD_MODULES` | 下一次心跳就拿到变化；别的机器上的模块（不在本机文件里）也知道，3 号；按集群里活跃的模块数分连接（6/3、4/3 向上取整）；去掉一个后 6/2 |
| RG-07 | — | 模块报告它在用的版本 |
| RG-08 | 一个模块离开列表 | 别的模块的 v1 模块号不变 |
| RG-09a～d | 旧版本；Redis 里的登记丢了 | 不用旧版本；模块用手上的继续；`init --from-file` 恢复，模块拿到新版本 |
| RG-10a～d | 直接启动一个新模块 | 能启动，自己登记为 ACTIVE，别的模块看得到，消息 ID 是 3 号 |
| RG-11a～e | 心跳；kill（SIGTERM） | 心跳马上没了，可以马上在另一台机器（节点 1）启动，登记里换成新节点 |
| RG-12a～e | 同一个名字在另一个节点运行 | 拒绝（exit 255）；kill -9 后心跳还在时也拒绝；心跳 TTL 到期后能启动；同一节点重启不冲突 |
| RG-13a～c | 另一个名字用同一个号码；号码超过 9 | 拒绝，不登记 |
| RG-14a～c | 刚删掉的模块 | 号码保留；再启动是 ACTIVE；下线中的模块重启后仍是下线中 |

## 5. 最近一次结果

| 时间 | 环境 | 结果 |
|---|---|---|
| 2026-11-27 | 测试服务器全量回归（提交 `32c4d33` 的代码） | 6 组共 216 项，全部通过 |

## 6. 没有覆盖到的

| 内容 | 说明 |
|---|---|
| 负载均衡（客户前面的 LB） | DS-01b 只提示，没有真实的 LB |
| 两个 Server 在两台机器上 | 都在同一台机器 |
| HTTP Server（CommonHttpServer）的多进程 | 只测了 SMPP Server |
