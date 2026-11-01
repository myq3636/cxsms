# cm.cfg 改 shortName、改 ssid 的 reload 修复设计

## 1. 问题

实测（Core、Server 模块，真实 reload 命令和 SMPP 连接）发现，直接修改 cm.cfg 再 reload，配置都显示加载成功（`cm.result=ok`），但运行有问题：

| 改动 | 现象 |
|---|---|
| 只改 shortName，ssid 不变 | 新 bind 被拒绝；旧连接没断开，能提交，但 DR 报 `session not found` 发不出去；通道的重试策略丢失 |
| 两个客户互换 ssid | 其中一个客户 bind 被拒绝；重试策略可能丢失（取决于处理顺序） |
| 通道改 ssid 或改名后，路由还指向不存在的通道 | Core 抛 NPE，只打一行 INFO，消息退回 pending 反复重试，不回失败 |
| 改名，同时新 ssid 已被占用 | 旧通道被删除，新通道没加载，通道没了 |
| senderReplacement 改了，先执行 `RELOAD_CUSTOMER` 再执行 `RELOAD_PHONE_PREFIX` | 新内容永远不生效 |

**原因**：改名被当成"新增一个 + 删除一个"，而 `d927c98`（快照重构）把删除放在变更列表最后，先建新客户、再按 ssid 清理旧客户。ssid 相同时，新客户刚建好的登录信息、连接管理器和重试策略被一起清掉；连接管理器已被新的替换，旧连接也没被断开。重构前的代码是先删除后新建，从代码看没有这个问题。

## 2. 方案

### 2.1 先清理、后新建（`applyCustomerChanges`）

变更列表处理两轮：第一轮清理所有改动过的旧客户，第二轮新建所有改动过的新客户。

- **副本替换不变**：解析 → 只为改动过的客户建新对象 → 建好完整的新快照 → 一次原子替换引用，这些都没有改。只调整替换之后"断开和重建连接"的顺序。
- **没改的客户不受影响**：内容没变的客户复用原对象，不进变更列表，连接不断开。
- **改动过的客户**：和原来一样断开重连。一次改多个客户时，先被清理的客户要等其他客户清理完才重建，这段时间 bind 会被拒绝，客户端需要重试。

### 2.2 路由只由 `RELOAD_ROUTING` 生效

按用户决定，cm.cfg reload 不重建路由表。配套修改：

- **MT 路由到不存在的通道时按未路由失败**（`ChannelRouter.routebySsid` 和 6 个路由类）：状态 RECIPIENT_ADDR_ERROR（2120），写 OUT_SUBMIT CDR，确认消费，按重试策略处理或给客户回失败 DR。日志 WARN `[MT-ROUTE] routing points to rssid=... but there is no such channel`，同一通道每 10 分钟最多一行，附带次数。
- **Redis 内容与生效的表一致**：路由只在启动和 `RELOAD_ROUTING` 时写，senderReplacement 只在启动和 `RELOAD_PHONE_PREFIX` 时写。
- **`RELOAD_CUSTOMER` 不再重读 senderReplacement**：否则 `RELOAD_PHONE_PREFIX` 比较时发现没有变化，新内容不生效。

`RELOAD_CUSTOMER` 仍然会重读路由文件（`routingInfoMap`），改动过的客户的 ALLOW 策略从中读取，这一点没有改。路由表（`A2PRoutingRelay` 等）不受影响。

### 2.3 新块加载失败时不丢通道（`buildSnapshot`）

无法从文件判断"新块是不是旧客户改名而来"。规则：**有新名字的块加载失败时，文件里已经没有的旧客户先保留**，ssid 没被占用才保留。

- `cm.result=partial`，`cm.failed` 是失败的新块，`cm.kept` 是保留的旧客户，日志 ERROR。
- 快照标记为不完整，下一次 reload 重新判断。修正文件后，按新文件处理。
- 如果同一次 reload 既真的删除了客户，又有无关的新块写错，被删除的客户也会暂时保留，修正错误后才删除。选择宁可晚删，不丢通道。

## 3. 使用方式

改 shortName 或 ssid 的步骤：

1. 修改 cm.cfg。如果是通道，同时修改路由文件的 `Relay=`；如果是客户改 ssid，同时修改路由文件的 `CCBSSID`。
2. 同时执行 `RELOAD_CUSTOMER` 和 `RELOAD_ROUTING`。

修改 ssid 时已经在 stream 里、带旧 ssid 的消息（`stream:mt:pending:{旧 ssid}`、`stream:mt:routed:{旧 ssid}`、等待重试的 MT 和 DR）这次没有专门验证，建议在流量低、积压少的时候修改 ssid。只改 shortName 时 ssid 不变，不涉及这个问题。

## 4. 配置项

没有新增配置项。`config:files:{模块}:{NodeID}` 新增字段 `cm.kept`。
