# Core 通道限速放行与空指针修复：测试用例与执行报告

> 范围：
> - 通道限速放行（`ChannelReleaseDispatcher`、暂存队列 `stream:core:hold:{rssid}`、`CoreOutboundTps` / `CoreHoldMaxSeconds`）
> - 两处空指针修复：DR 改道时间窗判断；`senderReplacement.cfg` 缺失时启动失败
> - 第 1 到 3 批的全量回归（用例见《Core优化测试用例与执行报告》）
>
> 设计见《Core通道限速放行设计》。第 1 到 5 节是用例设计，第 6 节是本地执行结果。

## 1. 测试环境

与《Core优化测试用例与执行报告》第 1 节相同（本地 Redis 7.2.5、JDK 17、真实的 Core 初始化、无数据库、测试程序扮演 Client 和 Server），另外增加了以下配置：

| 项 | 配置 |
|---|---|
| 限速通道 VENDOR9（rssid 9） | `CoreOutboundTps: 2`、`CoreHoldMaxSeconds: 5`，所以积压上限 = 2 × 5 = 10 条 |
| 客户 7001 的路由 | `conf/routing/CUST7001_routingInfo.cfg`：`A2PRoutingRelay: (PhonePrefix=86,Relay=VENDOR9)` |
| 不限速通道 VENDOR8（rssid 8） | 不配置两个新参数，使用默认值 0 和 21600 |
| 测试中的热加载 | 改写 cm.cfg 里 VENDOR9 的两个值，再调用真实的 `A2PCustomerManager.loadCustomers()` |
| 无 senderReplacement.cfg 的环境 | 复制一份 `a2p_home`，删掉 `conf/senderReplacement.cfg` |
| 测试用的缩短参数 | `CoreHoldRecoverIntervalMs=1000`、`CoreHoldRecoverIdleMs=1500`（线上默认 30 秒和 60 秒） |

新增的测试套件：

| 套件 | 内容 |
|---|---|
| `ChannelRateIT` | 通道限速放行的全部场景，以及空指针修复 NPE-02 |
| `StartupIT` | 在不含 senderReplacement.cfg 的环境里启动 Core（NPE-03） |
| `CoreDrFlowIT`（更新） | 3b 改用"让会话的消息存储暂时不可用"来制造失败（原来靠的那个空指针已经修掉了）；新增 NPE-01 |

## 2. 通道限速放行用例

| ID | 场景 | 步骤 / 前置 | 预期 | 套件：检查 | 级别 |
|---|---|---|---|---|---|
| CR-01 | 配置解析 | 加载 cm.cfg | VENDOR9 为 2 / 5；VENDOR8 为默认值 0 / 21600 | ChannelRateIT：`CR-01` | P0 |
| CR-02 | 不限速通道不受影响 | 外发到 rssid 8 | 直接写入 `stream:mt:routed:8:*`，不产生 hold stream | ChannelRateIT：`CR-02` | P0 |
| CR-03 | 限速通道进入暂存队列 | 外发 10 条到 rssid 9（放行线程未启动） | 全部写入 `stream:core:hold:9`；routed 为空；hold 的门铃已设置 | ChannelRateIT：`CR-03`（共 11 项） | P0 |
| CR-04 | 积压达到上限后拒收 | 第 11 条 | 不进入暂存队列；OUT_SUBMIT CDR 状态为 Throttled（2600）；给客户回失败 DR | ChannelRateIT：`CR-04a` 到 `CR-04c` | P0 |
| CR-05 | 按 TPS 放行 | 把最长暂存时间热加载为 60 秒，启动放行线程 | 2 秒内放出 3 到 6 条；10 条全部放完用时 ≥ 3.5 秒；hold 清空，门铃删除；放行时写入 outbox；放出的消息体就是原消息 | ChannelRateIT：`CR-05a` 到 `CR-05f` | P0 |
| CR-06 | 暂存超过 `CoreHoldMaxSeconds` | 热加载回 5 秒；暂停放行，写 3 条，等 5.5 秒后再启动 | 不放到 routed；OUT_SUBMIT CDR 状态为 EXPIRED（10200）；每条都给客户回失败 DR | ChannelRateIT：`CR-06a` 到 `CR-06d` | P0 |
| CR-07 | 轮到时消息本身已过期 | `expiryDate` 早于当前 GMT 时间 | 放行时判失败，不发出 | ChannelRateIT：`CR-07` | P1 |
| CR-08 | 同一次 MT 处理中，写入暂存队列也去重 | 在一次 MtAttempt 里同一段外发两次 | hold 里只有 1 条 | ChannelRateIT：`CR-08` | P0 |
| CR-09 | 两个 Core 放行同一条 | 同一个 hold 条目调用两次 `releaseHeld`（模拟另一个 Core 通过自动重投拿到了同一条） | 第一次成功；第二次判定为已被处理（GONE）；routed 只有 1 条 | ChannelRateIT：`CR-09` | P0 |
| CR-10 | 读走后挂掉的 Core | 用一个"已挂掉"的消费者读走一条，不放行 | 空闲超过阈值后被接管并放行 | ChannelRateIT：`CR-10a`、`CR-10b` | P0 |
| CR-11 | 热加载提高 TPS | 2 → 10 | 10 条在 2.5 秒内放完（每秒 2 条至少需要 4.5 秒） | ChannelRateIT：`CR-11a` 到 `CR-11c` | P1 |
| CR-12 | 热加载取消限速 | 改成 0 | 新消息直接写 routed；暂存队列里剩下的消息被放完 | ChannelRateIT：`CR-12a`、`CR-12b` | P1 |
| CR-13 | Core 全链路 | `MTStreamConsumer` 处理客户 7001 的 MT | 路由到 VENDOR9 → 进入暂存队列 → 被放行到 routed | ChannelRateIT：`CR-13a`、`CR-13b` | P0 |

## 3. 空指针修复用例

| ID | 场景 | 步骤 / 前置 | 预期 | 套件：检查 | 级别 |
|---|---|---|---|---|---|
| NPE-01 | 原消息没有 `timeStamp` 的 DR | 端到端：submit_resp（不带 timeStamp）→ 供应商 DR | DR 正常转发，反查缓存被删除。修复前会抛空指针，DR 一直被重投 | CoreDrFlowIT：`NPE-01a`、`NPE-01b` | P0 |
| NPE-02 | 供应商客户不存在、也没有 timeStamp，并且 DR 状态不是成功 | 直接调用 `handleOutDeliveryReportResForInnerAck` | 不抛异常，不改道，DR 正常转发 | ChannelRateIT：`NPE-02a`、`NPE-02b` | P0 |
| NPE-03 | 缺少 `senderReplacement.cfg` 时启动 | 在删掉该文件的环境里执行完整的 Core 初始化 | 正常启动，cm.cfg 加载成功 | StartupIT：`NPE-03a`、`NPE-03b` | P1 |

## 3A. 门铃竞态修复用例（测试中发现的问题 T-3）

| ID | 场景 | 步骤 / 前置 | 预期 | 套件：检查 | 级别 |
|---|---|---|---|---|---|
| RACE-01 | 压测：一边排空一边写入 | 真实的 `MTStreamConsumer` 运行中，往同一个 stream 逐条写入 300 条 MT，每条间隔随机 0 到 15ms | 全部被消费，stream 剩余 0 条 | DoorbellRaceIT：`RACE-01` | P1 |
| RACE-02 | 竞态的交错顺序（确定性复现） | stream 为空并留有旧门铃 → 分发线程读到 XLEN=0 → 生产者 XADD 并重新写门铃 → 分发线程执行删除门铃 | 门铃保留（旧代码无条件 ZREM，会把新消息的门铃删掉） | DoorbellRaceIT：`RACE-02` | P0 |
| RACE-03 | stream 确实为空 | 空 stream 加旧门铃 | 门铃被删除 | DoorbellRaceIT：`RACE-03` | P1 |

## 3B. 配置热加载用例（问题 T-4）

| ID | 场景 | 步骤 / 前置 | 预期 | 套件：检查 | 级别 |
|---|---|---|---|---|---|
| CFG-01 | 新增配置支持热加载 | 往 `GmmsConfig.properties` 写入 11 个新配置 → `reloadGmmsConfig("-a")` | 返回成功；`getCommonProperty` 读到的都是新值 | ConfigReloadIT：`CFG-01`（12 项） | P0 |
| CFG-02 | 代码按新值执行 | `ServerDrStreamShardFilter=true` | `isServerDrShardFilterEnabled()` 返回 true | ConfigReloadIT：`CFG-02` | P1 |
| CFG-03 | 配置值写错 | `CoreMtLeaseMs=abc` | 整次热加载被拒绝，原值保留 | ConfigReloadIT：`CFG-03` | P1 |
| CFG-04 | 从文件删除某个 key | 恢复原文件后热加载 | 保留上一次的值，不恢复默认值（已在《配置项说明》中写明） | ConfigReloadIT：`CFG-04` | P2 |

## 4. 回归

| 套件 | 内容 | 检查数 |
|---|---|---|
| Batch1Check | 第 1 批：分片公式、DR 反查缓存的失败与重试 | 10 |
| InlineDrCheck | 第 2 批：同步转发开关的作用范围 | 7 |
| DrShardCheck | Server DR 分片 | 10 |
| CoreDrFlowIT | 第 1、2 批的集成测试（3b 已改用新的失败制造方式）+ NPE-01 | 26 |
| Batch3IT | 第 3 批：幂等、outbox、四种超时 | 32 |
| CoreE2EIT | 第 3 批 Core 全链路 | 23 |
| DoorbellRaceIT | 门铃竞态（见 3A） | 3 |
| ConfigReloadIT | 配置热加载（见 3B） | 26 |

## 5. 不在本地范围的项

| ID | 项 | 建议做法 |
|---|---|---|
| X-11 | 多个 Core 节点同时放行：总速率 = TPS × 节点数 | 测试环境部署 2 个 Core，给同一个通道配 `CoreOutboundTps: 5`，观察 routed 的写入速率约为每秒 10 条 |
| X-12 | 失败后的重试策略：供应商的重试码里包含 2600 时，被拒的消息会写入 RMQ 后重新外发 | 需要数据库；测试环境里给供应商配上 `SMSOptionSubmitRetryCode` 后验证 |
| X-13 | HTTP 通道限速：放行到 `stream:http:submit:*` | 本地没有 HTTP 通道配置；测试环境配置一个 HTTP 供应商再验证 |
| X-14 | 大量积压时 Redis 的内存与放行性能 | 压测：例如每秒 5000 条进、每秒 1 条出，观察 hold 长度停在上限、拒收的条数与预期一致、Redis 内存 |
| X-15 | 原有的 X-01 到 X-07 | 见《Core优化测试用例与执行报告》第 6 节 |

## 6. 执行结果

最终一轮：2026-10-28 17:16（AEDT），Redis 7.2.5，OpenJDK 17.0.21 (Temurin)；代码为 `main@92e4e6b` 加上本次未提交的改动；`run-all.sh` 从头全量编译后依次执行 9 个套件。

### 6.1 各套件汇总

| 套件 | 类型 | 检查数 | 通过 | 失败 |
|---|---|---|---|---|
| Batch1Check | 单元 | 10 | 10 | 0 |
| InlineDrCheck | 单元 | 7 | 7 | 0 |
| DrShardCheck | 单元 | 10 | 10 | 0 |
| CoreDrFlowIT | 集成 | 26 | 26 | 0 |
| Batch3IT | 集成 | 32 | 32 | 0 |
| CoreE2EIT | 集成 | 23 | 23 | 0 |
| ChannelRateIT | 集成 | 40 | 40 | 0 |
| StartupIT | 集成（无 senderReplacement.cfg） | 2 | 2 | 0 |
| DoorbellRaceIT | 集成 | 3 | 3 | 0 |
| **合计** | | **153** | **153** | **0** |

### 6.2 新用例结果

| 用例 | 结果 | 实测数据 |
|---|---|---|
| CR-01 到 CR-04 | ✅ | 第 11 条按 Throttled 拒收，写 CDR 并回 DR |
| CR-05 | ✅ | 每秒 2 条：2 秒内放出 4 条，10 条用时 4.66 秒 |
| CR-06、CR-07 | ✅ | 暂存超时、消息过期都按 EXPIRED 失败，不发出 |
| CR-08、CR-09 | ✅ | 写入暂存队列去重；两个 Core 放行同一条，只有一个成功 |
| CR-10 | ✅ | 死掉的消费者留下的条目被接管并放行 |
| CR-11、CR-12 | ✅ | 热加载改成每秒 10 条后，10 条用时 1.44 秒；改成 0 后新消息直接外发，剩余的被放完 |
| CR-13 | ✅ | MT 消费者 → 路由 → 暂存 → 放行，全链路通过 |
| NPE-01 到 NPE-03 | ✅ | |
| RACE-01 到 RACE-03 | ✅ | |
| 回归（第 1 到 3 批，108 项） | ✅ | |

### 6.3 执行过程

| 轮次 | 结果 | 说明 |
|---|---|---|
| 第 1 轮 ChannelRateIT | 6 项失败 | 发现 T-1（产品问题，已修复）和 T-2（测试设计问题，已调整） |
| 第 2 轮全量 | 150/150 通过 | 修复 T-1 之后 |
| 第 3 轮全量 | CoreE2EIT 卡住 | E2E-01 中 MT 写入后一直没被消费，定位为 T-3（产品问题，已修复）；断言失败后后台线程没有退出，导致进程不结束，所以给每个套件加了 300 秒超时 |
| 第 4 轮全量 | 150/150 通过 | T-3 修复之前的代码，这次没有触发竞态。该竞态是偶发的 |
| 第 5 轮全量 | 153/153 通过 | 包含 T-3 修复和新增的 DoorbellRaceIT |
| 第 6 轮全量（最终，17:28） | **179/179 通过** | 包含 T-4 修复和新增的 ConfigReloadIT（10 个套件） |

## 7. 测试中发现并修复的问题

| # | 问题 | 发现方式 | 处理 |
|---|---|---|---|
| T-1 | **放行速率只有配置值的一半，并且接管挂掉 Core 的消息很慢**：每轮恢复时拿到的令牌，在没有可恢复条目时就浪费了；如果恢复那一轮恰好没有令牌，恢复就被跳过，而恢复计时照样重置 | 第一次执行时 CR-05a（每秒 2 条只放出 1 条）、CR-10b（恢复用了 8.6 秒）、CR-11b 失败；日志里放行间隔正好 1 秒 | 恢复只负责把挂掉消费者的条目转给自己，不消耗令牌；放行时先取本节点名下的待处理条目，再用剩余令牌读新条目。修复后 CR-05 中 10 条用时 4.66 秒，CR-10 及时恢复，CR-11 用时 1.44 秒 |
| T-3 | **门铃竞态导致消息可能一直不被消费（已有问题，影响线上）**：`popResponsibleDoorbell` 发现 stream 为空时会删除它的门铃，但"查长度"和"删门铃"是两次独立的 Redis 调用。如果恰好在这两步之间写入一条新消息，新消息的门铃就会被删掉：这条消息没被读过，不在待确认列表里，自动重投也领不到，只能等同一个客户、同一个分片的下一条消息到来才会被处理。所有 stream 消费者（MT、入向 DR、结果、Server DR、Client 外发）共用这个函数 | 第 3 轮 CoreE2EIT 卡住：消费者启动后 10ms 写入的 MT，恰好赶上上一个场景留下的旧门铃正在被清理 | 改为一段 Lua：`XLEN == 0` 时才执行 `ZREM`（`StreamQueueManager.removeDoorbellIfEmpty`）。随机压测（RACE-01）没能复现这个毫秒级的窗口，所以另外用确定性的交错顺序（RACE-02）验证 |
| T-4 | **新增的 GmmsConfig 配置不能热加载**：热加载只会生效白名单里的 key，这几批新增的 10 个 key 都不在白名单里，改了要重启才生效（之前说"热加载即可生效"是错的）；另外默认值表里 `CoreOutboundOutboxRecheckMs` 仍是旧值 300000 | 整理《配置项说明》时核对 `reloadGmmsConfig` 的代码发现 | 加入白名单，同时补上类型校验和默认值；新增 ConfigReloadIT 验证 |
| T-2 | CR-05 的测试设计问题：按每秒 2 条放 10 条，正好需要 5 秒，等于最长暂存时间，最后几条按设计被判超时 | 同上 | 测试改为先热加载把最长暂存时间设为 60 秒，再测速率。产品行为是正确的 |
