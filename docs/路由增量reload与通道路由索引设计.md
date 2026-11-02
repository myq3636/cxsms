# 路由增量 reload 与通道路由索引设计

## 1. 目标

线上约 3000 个路由文件。原来每次 `RELOAD_ROUTING` 都重新解析全部文件：

- 改一个文件也要全部重来，速度慢。
- 空文件会让整次 reload 被拒绝，一个文件有错影响所有路由。
- portal 只能按客户查路由，不能查"某个通道被哪些客户使用"。

## 2. 增量 reload

### 2.1 做法

每个路由文件单独加载成一份小表（`RoutingFile`：文件状态、块、小表），内存里按文件名保存。reload 时：

1. 列出目录，比较每个文件的大小和修改时间，找出新增、改动、删除的文件。**没变的文件不读取、不解析。**
2. 改动的文件刚写过时，等 `ConfigFileStableCheckMs` 再比较一次，还在变化的文件这次算失败。
3. 只解析改动的文件，生成新的小表。
4. 合并所有小表为新版本，通过一个引用替换（上一批的一次性替换不变）。

### 2.2 一个文件有错，其他文件不受影响

以下情况只有这个文件失败，它**保留旧版本**（新文件则没有路由），其他文件照常生效：

- 文件为空或读不出。
- `CCBSSID` 不是数字；或者缺失（只有 `{业务类型}_default` 默认路由文件可以不写，其他文件没有 ssid，路由永远查不到）。
- 路由项格式错误，或缺少 `Relay`。
- 文件正在写。

结果记录在 `config:files:{模块}:{NodeID}`：`routing.result=partial`，`routing.failed` 列出文件和原因，日志 ERROR。失败的文件下次 reload 会重试。

整次拒绝只剩两种：目录读不出或一个路由文件都没有；要删除的文件超过当前文件数的 `ReloadCustomerMaxRemovePercent`。

### 2.3 通道 ssid 变化

路由文件里写的是通道名，加载时转换成 ssid。按用户决定，增量不检查通道 ssid 变化：**通道 ssid 变了，要修改或 `touch` 引用它的路由文件，再 `RELOAD_ROUTING`。** 只要文件的修改时间变了就重新加载，内容相同也一样。

需要一次全部重新加载时：`RELOAD_ROUTING:-f`。`-a`（默认）和 `-r` 都按增量处理。

### 2.4 配套调整

- `RELOAD_CUSTOMER` 不再重读路由文件（原来每次 reload 客户都要解析全部文件）。改动过的客户的 ALLOW 策略读取最近一次加载的路由块。
- 启动时用同一套代码加载路由表（每个文件一份小表），不再有两套加载逻辑。
- 文件没有任何变化时，不重建路由表，也不重写 Redis。
- 生成 Redis 内容（序列化全部块、计算版本、生成通道索引）放到发布线程执行，不占用 reload 线程。路由表在此之前已经生效；有多次待发布时只执行最新一次。
- 已知限制：`routingInfo{模块}.*` 这组旧 key 仍按原逻辑在路由模块上全量重写（源码里没有读取方，保留是因为可能有外部使用）。

### 2.5 耗时实测

3002 个路由文件，每个文件 50 条路由，本地 Redis。这里的耗时是 reload 线程的 `costMs`，只含路由表生效，不含后台写 Redis：

| 场景 | 耗时 |
|---|---|
| 没有文件变化 | 约 200ms 内（测试按 200ms 间隔观察日志，未单独记录 `costMs`） |
| 改一个文件 | 128 到 181ms |
| `RELOAD_ROUTING:-f` 全部重新加载 | 1172ms |

后台写 Redis 没有单独计时。放到后台之前，同样 3002 个文件时这一步在 reload 线程上占 1.0 到 1.8 秒。本地 IT 模块没有写 `routingInfo*`（当时 `redisMs` 为 0 到 1），路由模块上还要加上这部分时间，需要在测试环境确认（见测试报告）。

## 3. 通道路由索引

随路由内容按版本保存两个 Hash，和其他内容一样清理和续期：

| key | field | 值 |
|---|---|---|
| `config:file:routing:{版本}:channel` | 通道名 | 使用这个通道的客户：`[{"customer":"CUST8888","ssid":"8888","routes":1}]`，`routes` 是条数 |
| `config:file:routing:{版本}:channelRoutes` | `通道名\|客户名` | 这个客户走这个通道的路由明细 |

```
HGET config:files:CoreEngine1:1 routing.version
HKEYS config:file:routing:{版本}:channel                       # 所有被使用的通道
HGET  config:file:routing:{版本}:channel VENDOR8               # 哪些客户使用这个通道
HGET  config:file:routing:{版本}:channelRoutes "VENDOR8|CUST8888"   # 明细
```

明细示例：

```json
[{"type":"A2PRoutingRelay","PhonePrefix":"86","Relay":"VENDOR8"},
 {"type":"A2PBackupRoutingRelay","PhonePrefix":"852","Relay":"VENDOR8:50,VENDOR9:50"}]
```

**明细单独存放的原因**：第一版把明细放在 `:channel` 的值里，实测 3000 个客户、每个 50 条路由都走同一通道时，这一个值达到 10MB，portal 读取和 Redis 都受影响。拆开后同样场景这个值约 155KB，明细按需读取。

- 覆盖所有名字含 `RoutingRelay` 的路由项，包括备用路由、二次路由、发送方、内容关键字、运营商、号码路由。
- 按比例分配或主备写法（`a:20,b:30`、`a;b`）里出现的每个通道都算使用。
- 内容来自实际在用的路由块：文件失败时用它的旧版本。
- 通道名是路由加载时解析用的名字（`Relay` 值去掉最后一个 `_` 之后的部分，和路由查通道的规则相同）。要 ssid，用 `config:file:cm:{版本}` 查这个客户块的 `CCBSSID`。
- 升级时如果当前版本已经存在但还没有索引，下一次发布（启动或 reload 有文件变化）会补写。

## 4. 配置项

没有新增配置项，沿用 `ConfigFileStableCheckMs`、`ReloadCustomerMaxRemovePercent`、`ConfigFilePublishEnable`。
