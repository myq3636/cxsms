# 路由增量 reload 与通道路由索引：测试用例与执行报告

> 设计见《路由增量reload与通道路由索引设计》。

## 1. 测试环境

本地 Redis 7.2.5，JDK 17。测试进程内完成真实初始化，reload 通过 Redis 控制频道发送真实命令，临时改写 `conf/routing/` 和 cm.cfg，结束后恢复。

## 2. 用例（`IncrementalRoutingIT`，模块 `CoreEngine1`）

| ID | 场景 | 预期 / 实测 |
|---|---|---|
| IR-01a～d | 启动 | 路由写入，`routing.failed` 为空；`:channel` 中 VENDOR8 → CUST8888，`:channelRoutes` 有类型和号段；CUST7001 只出现在 VENDOR9 下 |
| IR-02 | 没有文件变化，`RELOAD_ROUTING` | 路由表还是同一个对象，版本不变 |
| IR-03a～e | 改一个文件 | `loaded=1`；这个客户改走新通道，另一个客户不变；生成新版本，索引跟随，旧版本的索引被删除 |
| IR-04a～d | 一个文件空、缺 `CCBSSID`、`CCBSSID` 非数字、缺 `Relay`，同时改另一个文件 | `routing.result=partial`，`routing.failed` 写明文件和原因；坏文件保留旧路由，另一个文件的修改生效 |
| IR-05 | 文件修好 | `result=ok`，`failed` 清空，内容版本回到之前 |
| IR-06a～b | 新增坏文件，再删除 | 不加载、其他不变；删除后 `failed` 清空 |
| IR-07a～c | 新增 8 个文件；删 1 个；再删 5/9 | 8 个加载并进索引；删 1 个后路由消失；删 5/9 超过 20%，整次拒绝，路由不变 |
| IR-08a～b | 文件持续写入时 reload | 只有这个文件失败（`file still being written`）；写完后下次 reload 正常 |
| IR-09a～c | 按比例分配、备用路由、发送方路由 | `VENDOR9:20,VENDOR12:80` 在两个通道下都能查到；备用、发送方路由都进索引并带类型 |
| IR-10 | `RELOAD_ROUTING:-f` | 所有文件重新加载 |
| IR-11 | 路由文件已改，只执行 `RELOAD_CUSTOMER` | 路由和 Redis 版本都不变，也没有触发路由加载 |
| IR-12 | 版本已存在但没有索引（模拟升级） | 下一次发布补写索引 |

调整的已有用例：

| 用例 | 调整 |
|---|---|
| `RenameReloadIT` RC-02、RC-03 | 路由文件没变，改用 `RELOAD_ROUTING:-f` |
| `RenameReloadIT` RC-06b（新增）、RC-07 | 通道 ssid 从 8 改为 108 后，普通 `RELOAD_ROUTING` 不生效（仍按未路由失败）；`touch` 路由文件再 reload，MT 走 108 |
| `RapidReloadIT` RR-13 | 两个文件都清空：原来整次拒绝（`failed`），现在每个文件各自失败（`partial`），路由不变 |
| `ConfigFileIT` F-02 | 增加等待，见第 3 节 |

## 3. 测试中发现并修复的问题

| 问题 | 处理 |
|---|---|
| 第一版改一个文件也要 1.1 到 3 秒，和全量差不多。分段计时后，路由表约 100ms 就生效，其余时间都在 reload 线程上生成 Redis 内容 | 放到发布线程，有多次待发布时只执行最新一次。改一个文件降到 128 到 181ms |
| 第一版把路由明细放在 `:channel` 的值里，3000 个客户、每个 50 条路由走同一通道时，这一个值达到 10MB | 明细拆到 `:channelRoutes`（`通道名\|客户名`），同样场景 `:channel` 的值约 155KB |
| 升级后内容版本可能不变（已有 `:meta` 就只续期），索引永远写不进去 | 版本已存在但没有索引时补写（IR-12） |
| 新增的坏文件被删除后，所有文件都"没有变化"，`routing.failed` 一直留着旧内容 | 上次有失败时重新发布（IR-06b） |
| 启动时所有文件都失败，`routingInfoMap` 仍是启动解析的块，Redis 会写入没有生效的内容 | 启动时总是用实际加载结果替换（代码修改，没有专门用例） |
| 非 `_default` 文件缺 `CCBSSID` 时，路由表 key 是客户名，永远查不到，原来只打一行日志 | 判这个文件失败（IR-04b） |
| 全量回归时 `ConfigFileIT` F-02a、F-02b 失败：用例在 cm 写入后立即检查其他文件，而写入按顺序进行，约晚 100ms | 日志确认内容都已写入，属于用例时序问题，用例加等待。修改后单独运行 2 次都通过 |

## 4. 3000 个文件的耗时

3002 个路由文件，每个 50 条路由，本地 Redis。下表是 reload 线程耗时（日志 `costMs`）。

| 场景 | 修改前（2000 个文件） | 现在（3002 个文件） |
|---|---|---|
| 没有文件变化 | 约 2.2 秒（全量） | 约 200ms 内（测试按 200ms 间隔观察日志，未记录 `costMs`） |
| 改一个文件 | 约 2.2 秒（全量） | 128、151、181ms |
| 全量（`-f`） | 约 2.2 秒 | 1172ms（不含后台写 Redis） |

修改前的数字是 2000 个文件时测的，文件数不同；全量的两个数字统计范围也不同（现在不含 Redis 内容生成），不能直接比较。

## 5. 不在本地范围的项

| ID | 项 | 建议做法 |
|---|---|---|
| X-R1 | 路由模块上的 `routingInfo{模块}.*` 旧 key | 本地测试模块没有写这组 key，它们仍在 reload 线程上全量重写。线上看路由模块的 `costMs`；如果明显偏大，确认没有外部使用后可以去掉或改为后台写 |
| X-R2 | 后台写 Redis 的耗时和内存 | 本地未单独计时。上线后看 `Config file content written to Redis` 日志和 `MEMORY USAGE`；每次路由变化都会写一个新版本（旧版本无模块使用后删除） |
| X-R3 | 真实路由文件 | 本地用生成的文件，见下面的上线前检查 |
| X-R4 | 多进程 | 用例是单进程。各模块 reload 后 `routing.version` 应相同 |

**上线前检查（X-R3）**：现在的校验规则是，名字含 `RoutingRelay` 的项必须能解析、每组必须有 `Relay`，非 `_default` 文件必须有数字 `CCBSSID`。原来这类错误只跳过出错的项，其余项照常加载；现在整个文件失败。reload 时它保留旧路由，但**启动时没有旧路由可保留，这个客户就没有路由**。建议上线前先在测试环境用线上路由文件启动一次，检查 `routing.failed` 是否为空。

## 6. 回归结果

`run-all.sh` 从头全量编译后执行 32 个套件。`ConfigFileIT` 在全量回归中 2 项失败（原因见第 3 节），修复用例后单独重跑通过，下表是修复后的结果；产品代码在回归之后没有再修改，没有再跑一次全量。

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
| **IncrementalRoutingIT（新增）** | 28 | 28 | 0 |
| RenameReloadCore | 30 | 30 | 0 |
| RenameReloadServer | 30 | 30 | 0 |
| RenameReloadClient | 10 | 10 | 0 |
| RapidReloadCore | 21 | 21 | 0 |
| RapidReloadServer | 6 | 6 | 0 |
| RapidReloadClient | 4 | 4 | 0 |
| **合计** | **611** | **611** | **0** |
