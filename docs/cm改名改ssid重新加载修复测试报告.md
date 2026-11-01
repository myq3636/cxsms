# cm.cfg 改 shortName、改 ssid 的 reload 修复：测试用例与执行报告

> 设计见《cm改名改ssid重新加载修复设计》。同时对此前所有批次做了全量回归。

## 1. 测试环境

本地 Redis 7.2.5，JDK 17。测试进程内启动真实模块，通过 Redis 控制频道发送真实的 `RELOAD_CUSTOMER`、`RELOAD_ROUTING`、`RELOAD_PHONE_PREFIX`，结束后恢复配置文件：

- **Core**（`CoreEngine1`）：真实的 MT、结果消费者和路由。
- **Server**（`MultiSmppServer1`）：真实的 SMPP Server，测试代码作为 SMPP 客户端连接，走真实的 bind、submit 和 DR。
- **Client**（`MultiSmppClient1`）：真实的 SMPP Client 和外发消费者，连接测试里的假供应商。假供应商按 system_id 统计 bind、断开和收到的 submit。

这个套件使用 `conf/a2p_modules.conf` 指定模块类型。没有它时，reload 不走断开和重建连接的分支，修复前第一次实测得到的部分 bind 失败（status=13）就是这样造成的，不是真实问题。

## 2. 用例（`RenameReloadIT`）

### 2.1 Core（27 项）

| ID | 场景 | 预期 / 实测 | 级别 |
|---|---|---|---|
| RC-01 | VENDOR8 改名 VENDOR8N，ssid 不变 | ssid 8 查到新名字，旧名字查不到；重试策略保留（修复前丢失）；VENDOR9、CUST8888 仍是**同一个对象**，没有重建；路由表不变，MT 仍发往 rssid 8 | P0 |
| RC-02 | 接着 `RELOAD_ROUTING`，路由文件还写旧名字 | MT 未路由、已确认消费、不重试；OUT_SUBMIT CDR 状态 2120；写出给客户的失败 DR（修复前 NPE、反复重试） | P0 |
| RC-03 | 名字改回并 `RELOAD_ROUTING` | MT 恢复路由 | P1 |
| RC-04 | VENDOR8 的 ssid 8 改 108 | 查询和重试策略都换到 108 | P0 |
| RC-05 | 没有 `RELOAD_ROUTING`，路由表还指向 8 | 同 RC-02：按 2120 失败，回失败 DR | P0 |
| RC-06 | 消息本身已带 rssid=8（重试的 MT） | 同上 | P0 |
| RC-07 | `RELOAD_ROUTING` | MT 发往 rssid 108 | P0 |
| RC-08a~c | 改名同时新 ssid 被占用 | 旧 VENDOR8 保留（修复前通道没了）；`cm.result=partial`、`cm.failed=VENDOR8N`、`cm.kept=VENDOR8`；MT 仍能走旧通道 | P0 |
| RC-08d | 修正 ssid 后 reload | 正常改名，`cm.kept` 为空 | P1 |
| RC-08e | 真的删除客户（没有失败的块） | 客户被删除 | P1 |
| RC-09 | VENDOR8、VENDOR9 互换 ssid | 查询正确，两边的重试策略都在 | P0 |
| RC-10 | 路由文件和 senderReplacement 都改了，只执行 `RELOAD_CUSTOMER` | Redis 里两者的版本不变；MT 仍走旧路由 | P0 |
| RC-11 | 再执行 `RELOAD_ROUTING`、`RELOAD_PHONE_PREFIX` | 两者的版本都更新，MT 走新路由（修复前 senderReplacement 的新内容不生效） | P0 |

### 2.2 Server（30 项）

客户 c33 全程保持连接、不做任何修改，用来验证不受影响。

| ID | 场景 | 预期 / 实测 | 级别 |
|---|---|---|---|
| RS-01 | SMPPCUST31 改名，ssid 不变 | 旧连接被断开（修复前没断开）；重新 bind 成功（修复前被拒绝）；submit 写入 stream；DR 在新连接上收到；**c33 连接没断，submit 和 DR 正常** | P0 |
| RS-02 | 名字改回 | 同上（修复前 bind 被拒绝） | P0 |
| RS-03 | ssid 31 改 131 | 旧连接断开，重新 bind，submit 写入 `stream:mt:pending:131`，DR 收到；c33 不受影响 | P0 |
| RS-04 | 名字和 ssid 都改 | 同上 | P0 |
| RS-05、06 | SMPPCUST31、SMPPCUST32 互换 ssid | 两个客户的旧连接都断开，都能重新 bind（修复前一个被拒绝），submit 写入各自的新 ssid，DR 收到；c33 不受影响 | P0 |
| RS-07 | 换回来 | 两个客户都能 bind | P1 |

### 2.3 Client（10 项）

通道 CLIV43（u143）不做任何修改。

| ID | 场景 | 预期 / 实测 | 级别 |
|---|---|---|---|
| RK-00 | 启动 | 两个通道各 bind 一次，MT 都能到达 | P0 |
| RK-01 | CLIV41 改名，ssid 不变 | 旧连接断开，只有一次新 bind；MT 到达；**u143 没有重连（断开 0 次、bind 1 次），MT 正常** | P0 |
| RK-02 | 名字改回 | 重新 bind，MT 到达 | P1 |
| RK-03 | ssid 141 改 241 | 重新 bind，发往 rssid 241 的 MT 到达；u143 没有重连 | P0 |
| RK-04 | 名字和 ssid 都改 | 同上 | P0 |

## 3. 测试中发现并修复的问题

| 问题 | 处理 |
|---|---|
| RC-11b：senderReplacement 改了之后，先 `RELOAD_CUSTOMER` 再 `RELOAD_PHONE_PREFIX`，新内容不生效。cm.cfg reload 把内存里的表换成新文件内容，之后比较时发现没有变化 | `RELOAD_CUSTOMER` 不再重读 senderReplacement |
| RC-11a 第一版把路由改到配了限速的 VENDOR9，消息先进暂存队列，测试没在 routed 里找到 | 测试问题，改用不限速的 VENDOR12 |
| 修复过程中，`ChannelRouter` 的修改第一次没有生效：它的 `routebySsid` 和两个 Backup 路由类写法不同，补丁没匹配上 | 分别修改；由 RC-02、RC-05、RC-06 覆盖 |

## 4. 没有覆盖的范围

| ID | 项 | 建议做法 |
|---|---|---|
| X-N1 | 单连接客户（不写 `ConnectionType`）和多节点客户（`MN`）在 Server、Client 上改名 | Server、Client 的连接用例用的都是 `ConnectionType: MC`。上线前在测试环境各改一个 |
| X-N2 | SSLSMPP、Peering、HTTP 通道和 HTTP 客户改名 | 同样走先清理后新建，但没有实测。在测试环境确认 |
| X-N3 | Client 模块互换 ssid | Client 只测了改名和改 ssid，互换在 Core、Server 上测过 |
| X-N4 | 改 ssid 时已在 stream 里、带旧 ssid 的消息，以及等待重试的 MT 和 DR | 没有专门验证（RC-06 只覆盖 Core 上的 MT）。建议在流量低、积压少时修改 ssid |
| X-N5 | 一次改很多客户时，重建连接的耗时 | 本地每次只改一到两个客户。批量修改前先在测试环境确认 |

## 5. 回归结果

执行时间 2026-11-01 21:34（AEDT）；`run-all.sh` 从头全量编译后执行 28 个套件，全部通过。

| 套件 | 检查数 | 通过 | 失败 |
|---|---|---|---|
| Batch1Check | 10 | 10 | 0 |
| InlineDrCheck | 7 | 7 | 0 |
| DrShardCheck | 10 | 10 | 0 |
| CoreDrFlowIT | 26 | 26 | 0 |
| Batch3IT | 32 | 32 | 0 |
| CoreE2EIT | 23 | 23 | 0 |
| ChannelRateIT | 40 | 40 | 0 |
| StartupIT | 2 | 2 | 0 |
| DoorbellRaceIT | 3 | 3 | 0 |
| ConfigReloadIT | 68 | 68 | 0 |
| Batch4IT | 25 | 25 | 0 |
| Batch5IT | 19 | 19 | 0 |
| Batch6IT | 22 | 22 | 0 |
| Batch7IT | 39 | 39 | 0 |
| ClientBatch1IT | 19 | 19 | 0 |
| ClientBatch2IT | 12 | 12 | 0 |
| ServerBatch1IT | 16 | 16 | 0 |
| ServerBatch2IT | 21 | 21 | 0 |
| ServerBatch3IT | 10 | 10 | 0 |
| ShardGroupIT | 8 | 8 | 0 |
| LogChainCoreIT | 12 | 12 | 0 |
| LogChainClientIT | 11 | 11 | 0 |
| DrLivenessIT | 9 | 9 | 0 |
| GmmsReloadIT | 23 | 23 | 0 |
| ConfigFileIT | 15 | 15 | 0 |
| **RenameReloadIT（Core，新增）** | 27 | 27 | 0 |
| **RenameReloadIT（Server，新增）** | 30 | 30 | 0 |
| **RenameReloadIT（Client，新增）** | 10 | 10 | 0 |
| **合计** | **549** | **549** | **0** |
