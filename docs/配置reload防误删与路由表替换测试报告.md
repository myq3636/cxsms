# 配置 reload 防误删与路由表替换：测试用例与执行报告

> 设计见《配置reload防误删与路由表替换设计》。同时对此前所有批次做了全量回归。

## 1. 测试环境

本地 Redis 7.2.5，JDK 17。在进程内启动真实的 Core、Server、Client 模块，通过 Redis 控制频道连续发送真实的 reload 命令，同时跑 MT 流量，模块类型来自 `conf/a2p_modules.conf`。本地 cm.cfg 有 34 个客户块，路由文件只有 2 个，文件比线上小很多。

## 2. 用例（`RapidReloadIT`）

### 2.1 Core（21 项）

| ID | 场景 | 预期 / 实测 | 级别 |
|---|---|---|---|
| RA | 文件不变，连续 `RELOAD_CUSTOMER`，600 条 MT | 全部路由，客户对象不变 | P0 |
| RB | 连续 `RELOAD_ROUTING`（数百次），600 条 MT | 全部路由，0 条按 2120 失败（修复前有 1 条失败） | P0 |
| RC | 16 种 reload 命令混合连发，600 条 MT | 全部路由 | P0 |
| RD | cm.cfg 反复改动，连续 reload，600 条 MT | 全部路由；没改的 VENDOR8 对象不变 | P0 |
| RR-05 | 连发结束后 | 最后一版文件生效，命令仍正常 | P0 |
| RR-07 | 半截 cm.cfg（34 个客户缺 32 个） | 拒绝，`cm.failReason=32 of 34 customers would be removed, limit 20%`；客户对象不变，`cm.version` 不变（修复前通道被删除、结果为 ok） | P0 |
| RR-08 | 空 cm.cfg | 拒绝，`cm.failReason=file empty or unreadable`；客户不变 | P0 |
| RR-09 | 完整文件放回 | `cm.result=ok`，`cm.failReason` 清空，没有客户被重建 | P0 |
| RR-10 | 文件持续被改写时 reload | 拒绝，`file still being written`，客户不变；写完再发命令，成功 | P0 |
| RR-11 | 文件分块写入途中 reload | 等写完才读，只有新增的客户变化，其他客户对象不变 | P0 |
| RR-12 | `ReloadCustomerMaxRemovePercent=100` | 允许批量删除 | P1 |
| RR-13 | 路由文件为空 | 拒绝，`routing.failReason=empty routing file: CUST7001`，路由不变；放回后成功 | P0 |
| RR-14 | 4 个线程不停查路由，300 次 `RELOAD_ROUTING`，再 30 次在两个通道之间改路由并 reload | 2519 万次查询：0 次查不到、0 次异常（修复前 2100 万次中有 26 次） | P0 |
| RR-06 | 全程 | 没有 NPE，没有丢弃快照，没有 reload 失败 | P0 |

### 2.2 Server（6 项）、Client（4 项）

| ID | 场景 | 预期 / 实测 | 级别 |
|---|---|---|---|
| RS-01、02 | 40 次连续改 c31，c33 持续提交 | c33 没断开，提交全部写入 stream | P0 |
| RS-03~05 | 结束后 | c31 能 bind，最后一版文件生效，没有 reload 或会话错误 | P0 |
| RK-01 | 40 次连续改 u141 | 最后只有 1 条连接 | P0 |
| RK-02 | 同上 | 没改的 u143 没有重连（修复前 4 轮中出现 1 次全部客户断开重连） | P0 |
| RK-03 | 结束后 | MT 能到达两个通道 | P0 |

说明：这组 Client 用例每 25 毫秒改一次文件，文件一直在变，40 次 reload 都没有真正加载（`binds=1`）。这正是防护的效果，但也意味着 RK 没有覆盖"连续真实重建连接"。单次改名后重建连接由 `RenameReloadIT` 覆盖。

## 3. 测试中发现并修复的问题

| 问题 | 处理 |
|---|---|
| 空 cm.cfg 的 md5 是 null，走"不需要加载"分支，安全但没有记录失败 | 单独判断，记为 `failed` |
| 空的路由文件解析后仍算"有块"，路由表被清空，MT 无路由 | 有空文件时拒绝，路由保持不变 |
| 第一版每次都等 `ConfigFileStableCheckMs`，`RELOAD_ROUTING` 不合并，连发会严重积压 | 文件修改时间已超过这个间隔时不等待 |
| 测试问题：`RR-12` 用了旧的配置对象（`RELOAD_GMMS_CONFIG` 会替换它）；`RR-14` 第一版的 15 万次"查不到"来自 RR-13 留下的空路由表 | 修正测试；修复 RR-13 后为 0 |

## 4. 没有覆盖的范围

| ID | 项 | 建议做法 |
|---|---|---|
| X-G1 | 线上大小的 cm.cfg 和路由文件 | 本地文件很小。确认 reload 耗时，以及路由 reload 期间新旧两份表的内存占用 |
| X-G2 | 写入中途暂停超过 `ConfigFileStableCheckMs` | 检查发现不了；如果删除比例也没超过阈值，仍会按写到一半的文件加载。线上请用"临时文件 + `mv`" |
| X-G3 | 半截文件保护在 Server、Client 上的表现 | 判断逻辑在所有模块共用的 `A2PCustomerManager` 里，只在 Core 上实测。可在测试环境 Server 上确认连接不断 |
| X-G4 | 线上运维脚本 | 被拒绝的 reload 不会自动重试；批量改名或删除超过 20% 时要先调大 `ReloadCustomerMaxRemovePercent`。确认脚本会看 `cm.result` |

## 5. 回归结果

执行时间 2026-11-02 10:26（AEDT）；`run-all.sh` 从头全量编译后执行 31 个套件，全部通过。

| 套件 | 检查数 | 通过 | 失败 |
|---|---|---|---|
| 此前的 25 个套件（Batch*、Core*、Client*、Server*、Config*、LogChain*、DrLiveness、GmmsReload、ShardGroup 等） | 482 | 482 | 0 |
| RenameReloadIT（Core / Server / Client） | 27 / 30 / 10 | 67 | 0 |
| **RapidReloadIT（Core / Server / Client，新增）** | 21 / 6 / 4 | 31 | 0 |
| **合计** | **580** | **580** | **0** |
