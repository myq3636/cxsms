# GmmsConfig 热加载优化设计

## 1. 背景

审查 `GmmsUtility.reloadGmmsConfig` 时发现下列问题，前两个已在本地用真实热加载代码复现：

| # | 问题 | 后果 |
|---|---|---|
| 1 | 白名单里的文本、小数、部分布尔项（`Metrics.LogLevel`、`Metrics.MinTPS`、`DB.StartupCheck`、`DB.StartupCheckSql`、`DB.PoolStats.Enable`、`MessageIdIncludeNodeId`）校验时被当成整数 | 改了这些项，**整次热加载失败**，同时改的其他项也不生效 |
| 2 | `MyShards`、`CoreShard.*`、`ClientShard.*` 写错时，解析函数吞掉异常、返回 `{0}`，校验永远通过 | 热加载显示成功，节点悄悄只负责 0 号分片：原分片积压，可能和别的节点重叠 |
| 3 | 校验分片时临时改写线上正在用的 `totalShards`，finally 里再改回 | 这段时间内计算分片的线程会用到新的分片总数 |
| 4 | 比较差异只遍历新文件里的 key | 从文件里删掉一项，热加载不会恢复默认值 |
| 5 | 约 20 项在白名单里，但代码只在启动时读一次 | 日志显示 `Applied`，实际要重启 |
| 6 | 需要重启的项写错时，热加载不检查 | 下次重启时 `initUtility` 解析失败，执行 `System.exit(-1)`，模块起不来 |

另外，运维需要知道各模块**当前实际生效的配置**，而不是磁盘上的文件内容（两者在"改了文件但需要重启"时会不同）。

## 2. 方案

### 2.1 按类型校验（问题 1）

- 新增 `isStringRuntimeKey`（文本项）和 `isDoubleRuntimeKey`（`Metrics.MinTPS`），这两类不再进入整数校验。
- `DB.StartupCheck`、`DB.PoolStats.Enable`、`MessageIdIncludeNodeId` 加入布尔列表。
- 布尔值严格校验：只接受 `true`/`false`（不区分大小写）。原来 `Boolean.parseBoolean` 永远不报错，`ture` 会被悄悄当成 false。

### 2.2 分片配置严格校验（问题 2、3）

新增 `validateShardSet(value, totalShards, key)`：数字格式错误、区间反向、分片号超出 `0..TotalShards-1`、结果为空，都抛出异常，整次热加载被拒绝。

- 分片总数作为参数传入，**不再改写共享的 `totalShards` 字段**。
- 同时改了 `TotalShards` 时，按新的总数校验。
- 启动时的解析行为不变（仍是报错后回退到 0 号分片，避免启动失败）。

### 2.3 删除的配置项恢复默认值（问题 4）

比较差异时取新旧文件 key 的并集。

- **白名单里的项被删除**：从生效配置里移除，读取的地方自然回到代码默认值；有缓存字段的项（`MessageExpireTime` 等）按默认值刷新。日志 `Applied GmmsConfig: key old -> (removed, default)`。
- **需要重启的项被删除**：照常记为 `Restart required`，内存里保持原值，重启后按默认值。

### 2.4 白名单与实际读取一致（问题 5）

- **改成每次读取，真正支持热加载**：`CDRFileSwitchInterval`、`CDRFileMaxSize`、`ExceptionMsgFileSwitchInterval`、`ExceptionMsgFileMaxSize`、`PmqFileSwitchInterval`、`PmqFileMaxSize`、`SdqFileSwitchInterval`、`SdqFileMaxSize`。文件大小在下一次写入时按新值判断；切换间隔在当前这一轮等待结束后按新值。
- **移出白名单**（热加载时日志如实显示 `Restart required`）：`MaxCPUUsage`、`SystemStatusCheckInterval`、`RuninOverloadStatusPeriod`、`ExpiredMessageQueueSize`、`DNSPort`、`DNSMaxFailedLimit`、`DNSTestPeriod`、`DNSTestNumber`、`DNSBufferCapacity`、`DNSBufferTimeout`、`NMGAddress`、`NMGPort`、`DefaultRetryPolicy`、`AntiSpam.Characters2Escape`、`DefaultSuffix`、`LocalSuffix`、`SMSQueryDRHttpModule`、`{模块}.Asynchronous`、`{模块}.MaxCDRAsynQueueSize`、`{模块}.MaxSDQAsynQueueSize`、`SmppServerInboundWorkerMax`、`SmppServerInboundWorkerQueue`、`SmppServerInboundDispatcherThreads`，以及长连接共享队列的 `SenderWorkerThreads`、`RespWorkerThreads`、`DispatcherThreads`、`WorkerQueueSize`。
- `DNSAddress` 保留在白名单：MNP 查询每次读取；DNS 健康检查仍按启动时的值。

### 2.5 需要重启的项也检查格式（问题 6）

对改了但不能热加载的项：原值是整数、新值不是整数，或原值是 `true`/`false`、新值不是布尔值时，打 ERROR：

```
GmmsConfig restart-required key has an invalid value, the module will fail at next restart: key=ThreadPool.MaxPoolSize old=5 new=5a
```

**不拒绝本次热加载**（这些项本来就不会被应用），只是提前提醒。

### 2.6 生效配置写入 Redis（新增）

启动连上 Redis 后，以及每次热加载完成后（不论是否有变化），把本模块当前生效的 GmmsConfig 写到 state Redis：

| key | 类型 | 内容 |
|---|---|---|
| `config:effective:{模块名}:{NodeID}` | Hash | 生效配置的每一项（`key → value`）；密码类的值（key 含 password、passwd、pwd）写成 `******` |
| 同上，`_meta.*` 字段 | | `_meta.module`、`_meta.nodeId`、`_meta.file`（配置文件路径）、`_meta.loadedAt`（写入时间）、`_meta.source`（`startup` 或 `reload`）、`_meta.reloadResult`（`ok`、`failed` 或 `unchanged`）、`_meta.pendingRestart`（文件里已修改、但要重启才生效的 key，逗号分隔） |
| `config:effective:modules` | Set | 写过生效配置的 `{模块名}:{NodeID}`，便于列出所有模块 |

- **写入是原子的**：一段 Lua 先删除旧 Hash 再写入，不会读到写了一半的内容。
- **没有过期时间**：模块下线后 key 会保留，可以按 `_meta.loadedAt` 判断是否过期，或手工删除。
- **文件里没写的项不出现在 Hash 里**，表示使用代码默认值（默认值见《配置项说明》）。
- Redis 不可用时只打 WARN，不影响启动和热加载。
- 新增开关 `GmmsConfigPublishEnable`（默认 true，热加载）：关闭后不再写入。

查看方式：

```
redis-cli SMEMBERS config:effective:modules
redis-cli HGETALL config:effective:CoreEngine1:1
redis-cli HGET config:effective:CoreEngine1:1 _meta.pendingRestart
```

## 3. 不在本次范围

- 把六个手写分类列表合并为一张配置登记表（审查报告第 7 项），单独一批做。
- cm.cfg 的生效配置写入 Redis。cm.cfg 有几千个客户块，写入方式需要另外设计。
