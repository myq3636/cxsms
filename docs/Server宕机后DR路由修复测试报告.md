# Server 宕机后 DR 路由修复：测试用例与执行报告

> 设计见《Server宕机后DR路由修复设计》。同时对此前所有批次做了全量回归。

## 1. 测试环境

本地 Redis 7.2.5，JDK 17。测试按两台 Server 的写法直接写入 bind 登记（`smpp:server:session:*`、`smpp:server:active:*`、`smpp:server:module:*`）和模块心跳（`module:status:server:{模块}:{节点}`、`system:server:nodes`），调用真实的 `SmppServerDrRouteResolver`。

## 2. 用例（`DrLivenessIT`）

| ID | 场景 | 预期 / 实测 | 级别 |
|---|---|---|---|
| LV-01 | 客户在 Server1、Server2 都有登记，两个节点都有心跳；DR 原始模块为 Server1 | 发往 MultiSmppServer1 | P0 |
| LV-02 | 删除 Server1 的心跳（模拟 kill），bind 登记仍在 | 发往 MultiSmppServer2 | P0 |
| LV-03 | 同上 | 一行 INFO `[DR-ROUTE] server module=MultiSmppServer1 node=1 has no heartbeat` | P1 |
| LV-04 | 同上 | Server1 的登记只跳过，不删除 | P1 |
| LV-05 | Server1 心跳恢复 | 又发往 MultiSmppServer1 | P0 |
| LV-06 | `SmppServerDrLivenessCheck=false` | 原来的行为：发往 MultiSmppServer1 | P1 |
| LV-07 | 没有任何 bind 登记，原始模块已离线 | 不发往 MultiSmppServer1（返回空，Core 按 DR 发送失败进入重试） | P0 |
| LV-08 | 1000 条 DR 的 Redis 读次数 | 共 1003 次 GET：每条 DR 读 1 次登记（原来就有），心跳每秒最多 1 次 | P1 |
| LV-09 | 节点从没注册过心跳（不在 `system:server:nodes`，模拟 `ModuleStatusReporter` 启动失败） | 仍按原来的行为使用这个节点的登记 | P0 |

## 3. 测试中发现并修复的问题

第一次全量回归中，`CoreDrFlowIT` 有 5 项失败：测试环境里没有任何 Server 心跳，第一版把"没有心跳"一律当作离线，DR 全部找不到目标模块。

这在线上同样可能发生：如果某个 Server 的 `ModuleStatusReporter` 启动失败（Server 仍在正常服务，只打一行 warn），它就再也收不到 DR。

**修复**：只把"注册过（在 `system:server:nodes` 里）、现在没有心跳"的节点当作离线。被 kill 的节点来不及从集合里移除，所以仍能识别；从没注册过的节点按原来的行为处理。新增 LV-09 覆盖这个场景。修复后 `CoreDrFlowIT` 26 项、`DrLivenessIT` 9 项全部通过。

## 4. 不在本地范围的项

| ID | 项 | 建议做法 |
|---|---|---|
| X-D1 | 真实的 kill 场景 | 测试环境两台 Server，客户同时连在两台上。kill -9 Server1 后持续下发，30 秒后的 DR 应全部写进 `stream:dr:pending:{ossid}:MultiSmppServer2:*`，Core 日志有 `[DR-ROUTE] ... has no heartbeat` 和 `SMPP DR target server module fallback` |
| X-D2 | kill 后心跳还没过期的约 30 秒内写入 Server1 stream 的 DR | 本次不处理，要等 Server1 重启后读出、回失败结果、经 DR 重试再发出 |

## 5. 回归结果

执行时间 2026-10-30 16:10（AEDT）；`run-all.sh` 从头全量编译后执行 23 个套件。

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
| **DrLivenessIT（新增）** | 9 | 9 | 0 |
| **合计** | **444** | **444** | **0** |
