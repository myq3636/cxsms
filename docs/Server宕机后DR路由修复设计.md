# Server 宕机后 DR 路由修复设计

## 1. 现象

客户通过 Server1 下发 submit，随后 Server1 被 kill，客户的连接全部转到 Server2。这条消息的 DR 到达 Core 后（约 90 秒后），仍被写进 `stream:dr:pending:9999:MultiSmppServer1:14`，日志里没有任何兜底或告警。

## 2. 原因

Core 选 DR 的目标模块（`SmppServerDrRouteResolver.resolveTargetModule`）：

1. 取原始模块：MT 从 Server1 进来，即 `MultiSmppServer1`。
2. 查 bind 会话登记 `hasActiveSessionInModule(ossid, MultiSmppServer1)`：只要该模块名下还有未过期的会话登记，就直接返回原始模块，不打日志。

Server 每 30 秒刷新一次会话登记，有效期为 `SmppServerSessionRegistryTtlSeconds`，默认 `MaxSilentTime` + 60 = **660 秒**。正常停机会删除登记，进程被 kill 时来不及删除，只能等过期。所以 kill 后约 11 分钟内，Core 一直认为 Server1 上还有这个客户的连接。

后果：`stream:dr:pending:{ossid}:MultiSmppServer1:*` 只有模块名为 `MultiSmppServer1` 的进程会读，Server2 不读；`PELMonitor` 只认领已读未确认的条目，也接手不了。这期间到达的 DR 一直留在 stream 里，要等 Server1 重启后读到，发现没有会话再给 Core 回失败结果，经 DR 重试才可能发到 Server2。

## 3. 方案

Server 模块本来就有心跳：`module:status:server:{模块}:{节点}`，每 10 秒写一次，有效期 `ModuleStatusTTLSeconds`（默认 30 秒）。进程被 kill 后，心跳最多 30 秒就消失。

- **bind 登记加心跳判断**（`SmppServerSessionRegistry.hasActiveSessionInModule`）：会话登记里记录了 nodeId；该节点没有心跳时，这条登记不算有效会话。
  - 只是跳过，**不删除**登记：心跳恢复（进程重启，或只是 Redis 短暂抖动）后立即重新生效；登记本身仍按原 TTL 过期。
  - **只把"注册过、现在没心跳"的节点当作离线**：节点启动时把自己加入 `system:server:nodes`，被 kill 时来不及移除，所以"在集合里、但没有心跳"就是进程已经不在了。不在集合里的节点（例如 `ModuleStatusReporter` 启动失败，Server 仍在正常服务）按在线处理，否则这个 Server 再也收不到 DR。
  - 登记里没有 nodeId（旧数据），或读心跳时 Redis 出错：按在线处理，与原来一致。
  - 心跳结果在本地缓存 1 秒：每个节点每秒最多多 1 次 Redis GET，与 DR 数量无关。
- **原始模块离线后的选择**，沿用已有逻辑：
  - 客户在其他模块有会话：选那个模块（`fallback-active`，限频 INFO）。
  - 都没有：原来"没有登记数据就退回原始模块"的分支，现在要求原始模块不是"已知离线"（`system:server:nodes` 不为空，且没有任何节点报告该模块 ONLINE）；已知离线时继续往下，选在线的已配置 Server 模块；仍然没有就返回空。返回空时 Core 按 DR 发送失败处理（`FAIL_SENDOUT_DELIVERYREPORT`，进入 DR 重试），不再写进没人读的 stream。
- **日志**：发现某节点没有心跳时一行 INFO `[DR-ROUTE] server module=... node=... has no heartbeat ...`（每个节点每 10 分钟最多一行，附带次数）。随后的改发由已有的 `SMPP DR target server module fallback ... fallbackModule=...` 记录。

## 4. 新增配置

| 配置项 | 默认 | 生效 | 作用 |
|---|---|---|---|
| `SmppServerDrLivenessCheck` | `true` | 热加载 | 是否检查节点心跳。false 恢复原来的行为 |

已加入 `GmmsUtility` 热加载白名单（布尔校验、默认值）。

## 5. 不在本次范围

已经写进离线模块 stream 的 DR（kill 后心跳还没过期的约 30 秒内写入的），仍要等原模块重启后处理。由 Server 接手离线模块 DR stream 的方案，这次不做。
