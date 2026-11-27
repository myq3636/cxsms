# 配置重新加载测试（ConfigReloadIT、GmmsReloadIT、ConfigFileIT、RenameReloadIT、RapidReloadIT、SmsUiCommandCheck）

> 日期：2026-11-27。用例代码在 `regression/test/it/`（SmsUiCommandCheck 在 `regression/test/com/king/...`，unit 模式），都在 `regression/suites.txt`：
>
> | 套件 | 模式 | 项数 | 内容 |
> |---|---|---|---|
> | ConfigReloadIT | it | 66 | GmmsConfig 新加的配置项能热加载 |
> | GmmsReloadIT | it | 23 | GmmsConfig 重新加载：按类型校验、分片严格校验、删掉的项回默认、写进 Redis 给 Portal |
> | ConfigFileIT | it | 15 | 配置文件内容写进 Redis 给 Portal |
> | RenameReloadCore / Server / Client | it-mod:CoreEngine1 / MultiSmppServer1 / MultiSmppClient1 | 30 / 30 / 10 | cm.cfg 里改了 shortName、CCBSSID 后重新加载 |
> | RapidReloadCore / Server / Client | 同上 | 21 / 6 / 4 | 有流量时连续发重新加载命令，以及半写的、空的配置文件 |
> | SmsUiCommandCheck | unit | 25 | smsui 命令翻译成控制通道上的命令 |
>
> 路由文件的重新加载在《路由重新加载测试（IncrementalRoutingIT）》，3 个 Core 同时加载路由见那份文档第 7 节。

## 1. 测什么

模块订阅 state Redis 上的控制通道 `channel:gmms:control`，收到 `RELOAD_*` 命令就重新读对应的文件。加载状态写在 Redis `config:files:{模块}:{节点}`（版本、结果、失败原因、加载时间），GmmsConfig 生效的值写在 `config:effective:{模块}:{节点}`，给 Portal 看。

| 部分 | 内容 |
|---|---|
| GmmsConfig | 热加载白名单里的项改了就生效；值不对时整次加载拒绝、保留原值；从文件删掉的项回默认值；要重启才生效的项只提示；分片值严格校验 |
| 给 Portal 的内容 | 每个配置文件按块写进 Redis，密码打码，带版本和 TTL；没有模块用的旧版本删掉 |
| cm.cfg 改名、改 ssid | 只动改了的那个客户：Core 的查找、重试策略跟着走；Server、Client 只断开改了的那个客户/通道的连接 |
| 连续重新加载 | 有流量时连续几百次命令，消息不丢、不出错；最后的文件生效 |
| 防护 | 半写的、空的 cm.cfg 拒绝；一次删掉的客户超过 `ReloadCustomerMaxRemovePercent`（默认 20%）拒绝；文件还在写时等写完 |
| smsui | 命令翻译成控制通道的命令（带目标模块、参数） |

## 2. 怎么跑、怎么判定

```
regression/run.sh GmmsReloadIT     # 其余套件同样
cat regression/.work/results/GmmsReloadIT.txt
```

- 环境：测试服务器上的回归环境（JDK 17，本地 Redis 16379）。
- 进程内启动真实的模块（Core、Server 或 Client，按 `it-mod:` 指定的模块身份），改 `conf/cm.cfg`、`Gmms/GmmsConfig.properties` 等文件，在控制通道上发命令（或直接调用 `reloadGmmsConfig("-a")`），再看 Redis 里的加载状态、内存里的对象、消息的去向。
- 每个套件结束时把改过的文件恢复原样。

## 3. 用到的配置

- 回归环境基础的 cm.cfg（34 个客户块，其中 CUST8888、VENDOR8、VENDOR9 等见《长短信测试》《Client发送速率与发送过程测试》），客户 SMPPCUST31～33（Server 用，见《Server功能测试》3.2）。
- RenameReloadCore：VENDOR8 加了 `SMSOptionMTRetryPolicy: 5M*3`，用来检查改名后重试策略还在。
- RenameReloadClient：追加两个通道 CLIV41（CCBSSID 141）、CLIV43（143），连假 SMSC。
- RapidReloadCore：给 CUST8888 发 MT 时每条换一个接收号码（CUST8888 配了按号码的发送上限）。

## 4. 用例

### 4.1 新配置项能热加载（ConfigReloadIT）

把下面这些项写进 GmmsConfig，`reloadGmmsConfig("-a")` 后检查：

```
CoreDrInlineDispatch=false        CoreMtIdempotency=false         ServerDrStreamShardFilter=true
CoreMtLeaseMs=30000               CoreMtMarkerTtlSeconds=86400    CoreOutboundUnreadStallMs=90000
CoreOutboundRespTimeoutMs=240000  CoreHoldReleaseTickMs=200       CoreHoldRecoverIntervalMs=20000
CoreHoldRecoverIdleMs=45000       CoreOutboundOutboxRecheckMs=30000  RedisStreamMaxDeliveries=5
CoreStreamTaskMaxBatches=4        CoreStreamTaskMaxMs=1500        CoreResultStreamSharded=false
CoreDrResultIdempotency=false     CoreDbFallbackInline=false      CoreCsmExpirySweep=false
CoreCsmExpiryIntervalSec=30       CoreCsmExpiryBatch=100          CoreCsmExpiryLeaseMs=120000
ClientSendIdempotency=false       ClientSendLeaseMs=30000         ClientStreamTaskMaxBatches=5
ClientStreamTaskMaxMs=1000        SmppPduLogEnable=false          SmppServerSessionKeeperShared=false
SmppServerSendQueueMax=5000       SmppServerSendQueueHighWater=500  ServerDrAckOnResp=false
```

| ID | 情况 | 预期 |
|---|---|---|
| CFG setup | 加载前（30 项） | 每项都不是上面的值 |
| CFG-01 | 重新加载 | 成功；30 项都生效（每项一个检查） |
| CFG-02 | 代码里读到的 | `ServerDrStreamShardFilter` 是 true，`CoreDbFallbackInline`、`ClientSendIdempotency` 是 false |
| CFG-03 | 再加一行 `CoreMtLeaseMs=abc` | 整次加载拒绝，保留原来的 30000 |
| CFG-04 | 文件恢复原样 | 删掉的项回到默认值 |

### 4.2 GmmsConfig 的校验（GmmsReloadIT）

| ID | 改动 | 预期 |
|---|---|---|
| R-01a | `Metrics.LogLevel=INFO`（文字） | 生效（以前整次加载失败） |
| R-01b | `Metrics.MinTPS=0.5`（小数） | 生效 |
| R-01c | `DB.PoolStats.Enable=true`、`MessageIdIncludeNodeId=true` 和别的项一起 | 生效 |
| R-01d | `CoreMtIdempotency=ture`（拼错） | 拒绝（以前悄悄当成 false） |
| R-01e | `RedisStreamMaxDeliveries=7x` | 拒绝，还是 10 |
| R-02a | `MyShards=0-3x` | 拒绝，分片不变 [0, 1]（以前接受了，变成 [0]） |
| R-02b | `MyShards` 超过 TotalShards-1 | 拒绝 |
| R-02c | `MyShards=` 空 | 拒绝 |
| R-02d | `TotalShards` 改小到比现有的 MyShards / CoreShard 小 | 拒绝 |
| R-03a/b | 设 `RedisStreamMaxDeliveries=7`、`MessageExpireTime=100`，再从文件删掉 | 回到默认 10、1380（以前保留 7、100） |
| R-04 | `MaxCPUUsage=70`（只在启动时读） | 日志 `Restart required`，不是 `Applied` |
| R-05 | `Redis_master_port=16379x` | ERROR “will fail at next restart”，但不拒绝这次加载 |
| R-06 | `CDRFileMaxSize=200` | CDR 写入器用上了（204800 字节） |
| R-07a/b | Redis 里的生效值 | 模块列在 `config:effective:modules`；`config:effective:CoreEngine1:1` 有生效的值（如 TotalShards=2）和 `_meta.loadedAt` |
| R-07c | — | 密码打码 |
| R-07d/d2 | 改了要重启才生效的项 | `_meta.pendingRestart` 列出 `MaxCPUUsage,Redis_master_port`；文件改回运行中的值后变空 |
| R-07e～g | 加载成功、失败、删掉一项 | 新值写进 Redis；失败时 Redis 里的值不变、`_meta.reloadResult=failed`；删掉的项从 Redis 去掉 |
| R-07h | `GmmsConfigPublishEnable=false` | 不写 Redis |

### 4.3 配置文件写进 Redis（ConfigFileIT）

| ID | 情况 | 预期 |
|---|---|---|
| F-01a～e | 启动时的 cm.cfg | `config:files:CoreEngine1:1` 里有 `cm.version`；`config:file:cm:{版本}` 每个客户块一项（34/34），格式 blocks；`IncomingConnection` 里的密码打码；`cm.result=ok`，内容 TTL 7 天，模块列出 |
| F-02a/b | 其他文件 | 路由写进去（2 个块）；senderReplacement、vendorTemplate、vendorRoutingReplacement、OperatorMode 都有记录（空文件 0 项） |
| F-03a～c | cm.cfg 重新加载，加一个好客户和一个坏客户 BADCUST96 | 新版本；好的进去，坏的没进（`cm.result=partial`，`cm.failed=BADCUST96`）；没有模块用的旧版本删掉 |
| F-04/04b | 另一个模块还在用某个版本 | 不删；内容改回原样，版本号和启动时一样 |
| F-05 | 改 OperatorMode.properties | 写进去，格式 properties |
| F-06 | recipientRuleFile.cfg | 按行写，如 `000002 = {"cc":"86","lens":[11]}` |
| F-07 | `ConfigFilePublishEnable=false` | 重新加载不写 |

### 4.4 cm.cfg 改名、改 ssid：Core（RenameReloadCore）

| ID | 改动 | 预期 |
|---|---|---|
| RC-00 | — | 基准：CUST8888 的 MT 路由到 8 |
| RC-01a～d | VENDOR8 改名 VENDOR8N（ssid 不变） | ssid 8 对应新名字，旧名字没了；ssid 8 的重试策略还在（以前丢了）；别的客户还是同一个对象（没重建）；路由表里还是 8，MT 照常路由 |
| RC-02 | 再 `RELOAD_ROUTING:-f`（路由文件里还写着 `Relay=VENDOR8`） | 通道没了：不路由，确认，不重试；OUT_SUBMIT CDR 状态 2120；给客户回失败 DR |
| RC-03 | 名字改回来，`RELOAD_ROUTING` | 又能路由 |
| RC-04a | VENDOR8 的 ssid 8 改成 108 | 查找和重试策略都移到 108 |
| RC-05 | 不重新加载路由（路由表里还是 8） | 通道没了：不路由，2120，失败 DR |
| RC-06 | 已经带着 rssid 8 的重试 MT | 同上 |
| RC-06b | `RELOAD_ROUTING`（增量，路由文件没变） | 同上 |
| RC-07 | 碰一下客户的路由文件再 `RELOAD_ROUTING` | MT 到 108 |
| RC-08a～c | 改名到一个已经在用的 ssid（VENDOR8N 用 9） | 保留原来的 VENDOR8（以前通道丢了）；`cm.result=partial`、`cm.failed=VENDOR8N`、`cm.kept=VENDOR8`；MT 照样走保留的通道 |
| RC-08d | 把 ssid 改对 | 正常改名，没有保留 |
| RC-08e | 真的删掉（没有失败的块） | 客户删掉 |
| RC-09 | 两个通道对换 ssid | 两边的查找和重试策略都对 |
| RC-10a/b | 改路由文件、senderReplacement 后只重新加载 cm.cfg | Redis 里的路由、号段版本不变，MT 还按旧表 |
| RC-11a/b | 再 `RELOAD_ROUTING`、`RELOAD_PHONE_PREFIX` | 版本变了，MT 按新表（到 12）；新的号段内容生效并写进 Redis |

### 4.5 cm.cfg 改名、改 ssid：Server、Client（RenameReloadServer、RenameReloadClient）

**Server**（客户 SMPPCUST31，c33 一直不改作对照）：

| ID | 改动 | 预期 |
|---|---|---|
| RS-00 | — | c33 一直绑定着 |
| RS-01 | 只改名（SMPPCUST31N） | 改了的客户旧连接被关；重新绑定能接受（以前改名后拒绝）；提交写进 `stream:mt:pending:31`；DR 在新连接上送到；c33 的连接没断，提交和 DR 照常 |
| RS-02 | 改名回来 | 同上 |
| RS-03 | 只改 ssid（131） | 同上，提交写进 `stream:mt:pending:131` |
| RS-04 | 名字和 ssid 都改 | 同上 |
| RS-05 | 和 SMPPCUST32 对换 ssid | 两个客户都能用（提交进 `stream:mt:pending:32`） |
| RS-06 | 恢复原样 | 同上 |
| RS-07 | — | 对换回来后两个都能绑定 |

**Client**（通道 CLIV41，CLIV43 不改作对照）：

| ID | 改动 | 预期 |
|---|---|---|
| RK-00/00b | — | 两个通道启动时各绑定一次，MT 都能到 |
| RK-01a～c | CLIV41 改名 | 旧连接关闭，重新绑定一次，MT 能到；CLIV43 不重连，照常 |
| RK-02 | 改回来 | 照常 |
| RK-03/03b | ssid 141 改成 241 | 重新绑定，发往 241 的 MT 能到；CLIV43 不动 |
| RK-04/04b | 名字和 ssid 都改 | 同上 |

### 4.6 连续重新加载和防护（RapidReloadCore、Server、Client）

**Core**：

| ID | 情况 | 预期 / 2026-11-27 实测 |
|---|---|---|
| RA | 文件不变，连续 `RELOAD_CUSTOMER`，期间 600 条 MT | 600 条都路由（确认 600、路由 600、因没路由失败 0、nack 0） |
| RR-01b | — | 文件没变：VENDOR8 还是同一个对象 |
| RB | 连续 `RELOAD_ROUTING` | 600 条都路由 |
| RC | 各种命令混着发（`RELOAD_ANTISPAM`、黑白名单、模板、号段、`RELOAD_SYSTEM_REPLACE`、`RELOAD_VENDOR_REPLACE`、`RELOAD_GMMS_CONFIG` 等） | 600 条都路由 |
| RD | 命令之间改文件（改别的客户），VENDOR8 的流量不停 | 600 条都路由 |
| RR-04b | — | VENDOR8 不在改动里：77 次文件变化后还是同一个对象 |
| RR-05a/b | 最后 | 最后的文件生效（ssid 13 是 VENDOR13N）；命令照常（没有卡住的线程） |
| RR-07 | 写了一半的 cm.cfg（大部分客户没了） | 拒绝：`cm.failReason=32 of 34 customers would be removed, limit 20%`；没人被删或重建，`cm.version` 不变 |
| RR-08 | 空的 cm.cfg | 拒绝：`file empty or unreadable`；什么都不变 |
| RR-09 | 文件恢复 | `cm.result=ok`，没人被重建 |
| RR-10/10b | 文件一直在变 | 拒绝（`file still being written`）；写完后加载成功 |
| RR-11 | 分块写文件时重新加载 | 等写完，读整个文件，只有新客户变了 |
| RR-12 | `ReloadCustomerMaxRemovePercent=100` | 允许大量删除 |
| RR-13/13b | 空的路由文件 | 各自保留原来的路由（`CUST7001:empty routing file;CUST8888:empty routing file`）；文件恢复后加载成功 |
| RR-14 | 路由在两个通道间来回切 330 次，期间查找 | 2.13 亿次查找没有一次落空，没有异常（以前每 2100 万次 26 次） |
| RR-06 | 整个过程 | MT 处理没有空指针，没有丢弃的快照，没有失败的加载 |

**Server**：

| ID | 情况 | 预期 |
|---|---|---|
| RS-00/01 | 40 次重新加载另一个客户 | c33 一直没断 |
| RS-02 | — | c33 的 796 条提交都写进流 |
| RS-03～05 | 之后 | c31 能绑定；最后的文件生效，`cm.result=ok`；没有加载或会话错误 |

**Client**：

| ID | 情况 | 预期 |
|---|---|---|
| RK-00 | — | 两个通道都绑定 |
| RK-01 | 连续改一个通道 | 之后正好 1 个连接（不泄漏不丢）：绑定 1、关闭 0 |
| RK-02/03 | — | 没改的通道从没重连；MT 两个都能到 |

### 4.7 smsui 命令（SmsUiCommandCheck）

| smsui 命令 | 控制通道上的命令 |
|---|---|
| `shutdown -a` | `SHUTDOWN` |
| `shutdown CoreEngine1` | `SHUTDOWN@CoreEngine1`（以前发给所有模块） |
| `shutdown` 两个模块 | `SHUTDOWN@MultiSmppClient1,MultiSmppClient2` |
| `shutdown -ha CoreEngine2` | `SHUTDOWN@CoreEngine2` |
| `shutdown`（没有目标） | 拒绝 |
| `switchdns slave` | `SWITCH_DNS:slave`（以前少了哪一边） |
| `switchdns` 别的值 | 拒绝 |
| `switchredis master` | `SWITCH_REDIS:master` |
| `routinginfo -r` | `RELOAD_ROUTING:-r` |
| `customer -a` | `RELOAD_CUSTOMER:-a` |
| `routinginfo -g`、`help` | 只在本地（下载文件、帮助） |
| `drError -a` | `RELOAD_DRERROR:-a`；不接受模块；`-r` 拒绝 |
| `coreShards -a`、`coreShards -a CoreEngine2` | `RELOAD_CORE_SHARDS:-a`、`RELOAD_CORE_SHARDS@CoreEngine2:-a` |
| `clientShards` 两个模块 | `RELOAD_CLIENT_SHARDS@MultiSmppClient1,MultiSmppClient2:-a` |
| `modules -a` | `RELOAD_MODULES:-a` |
| 名单的定时命令 | `RELOAD_WHITELIST`、`RELOAD_SENDER_WHITELIST`、`RELOAD_SENDER_BLACKLIST`、`RELOAD_CONTENT_WHITELIST`、`RELOAD_CONTENT_BLACKLIST`、`RELOAD_RECIPIENT_BLACKLIST`，都带 `:-a` |

## 5. 最近一次结果

| 时间 | 环境 | 结果 |
|---|---|---|
| 2026-11-27 | 测试服务器全量回归（提交 `32c4d33` 的代码） | 全部通过 |
| 2026-11-27 | 测试服务器重跑（取上面的实测输出） | 10 组共 230 项，全部通过 |

## 6. 没有覆盖到的

| 内容 | 说明 |
|---|---|
| 多个模块同时收到同一个命令 | 这里每次只起一个模块；3 个 Core 同时加载路由见 MultiCoreRoutingReloadCheck |
| Portal 读 `config:files`、`config:effective` 的页面 | 不在回归里 |
| `SWITCH_DNS` 真的切换 | 只测了命令翻译 |
