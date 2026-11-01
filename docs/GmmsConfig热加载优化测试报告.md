# GmmsConfig 热加载优化：测试用例与执行报告

> 设计见《GmmsConfig热加载优化设计》。同时对此前所有批次做了全量回归。

## 1. 测试环境

本地 Redis 7.2.5，JDK 17。测试在进程内完成 GmmsUtility 的真实初始化（模块 `CoreEngine1`，NodeID 1），每个用例改写本地的 `Gmms/GmmsConfig.properties`，调用真实的 `reloadGmmsConfig`，检查返回值、代码读到的值、日志和 Redis。测试结束后恢复原文件。

本地配置文件中 `TotalShards=2`、`MyShards=0-1`。

## 2. 用例（`GmmsReloadIT`）

| ID | 场景 | 预期 / 实测 | 级别 |
|---|---|---|---|
| R-01a | `Metrics.LogLevel=INFO` | 热加载成功，读到 INFO。修改前：整次热加载失败 | P0 |
| R-01b | `Metrics.MinTPS=0.5` | 成功 | P0 |
| R-01c | `DB.PoolStats.Enable=true`、`MessageIdIncludeNodeId=true`，同时改 `RedisStreamMaxDeliveries=7` | 三项都生效。修改前：三项都不生效 | P0 |
| R-01d | `CoreMtIdempotency=ture` | 拒绝。修改前：悄悄按 false 生效 | P0 |
| R-01e | `RedisStreamMaxDeliveries=7x` | 拒绝，仍为默认值 10 | P1 |
| R-02a | `MyShards=0-3x` | 拒绝，分片保持 `[0, 1]`。修改前：显示成功，分片变成 `[0]` | P0 |
| R-02b | `MyShards=0-2`（超出 `TotalShards-1`） | 拒绝 | P0 |
| R-02c | `MyShards=`（空） | 拒绝 | P1 |
| R-02d | `TotalShards=1`（现有 `MyShards=0-1` 超出新范围） | 拒绝，`TotalShards` 仍为 2 | P0 |
| R-03a/b | 先设 `RedisStreamMaxDeliveries=7`、`MessageExpireTime=100`，再从文件删除这两项 | 恢复默认值 10 和 1380（后者是缓存字段）。修改前：保持 7 和 100 | P1 |
| R-04 | `MaxCPUUsage=70`（只在启动时读取） | 日志 `Restart required GmmsConfig: MaxCPUUsage`。修改前：显示 `Applied`，实际不生效 | P1 |
| R-05 | `Redis_master_port=16379x`（需要重启的项写错） | 热加载不被拒绝，打 ERROR `... will fail at next restart: key=Redis_master_port` | P1 |
| R-06 | `CDRFileMaxSize=200` | CDR 写入按 204800 字节切换文件。修改前：要重启 | P2 |
| R-07a | 启动、热加载后 | `config:effective:modules` 里有 `CoreEngine1:1` | P0 |
| R-07b | 同上 | `config:effective:CoreEngine1:1` 中的值与正在使用的一致，带 `_meta.loadedAt` | P0 |
| R-07c | 同上 | 密码类的值写成 `******` | P0 |
| R-07d | 文件里改了 `MaxCPUUsage`、`Redis_master_port`（都要重启） | `_meta.pendingRestart` = `MaxCPUUsage,Redis_master_port` | P1 |
| R-07d2 | 文件恢复成正在使用的值，再热加载 | `_meta.pendingRestart` 变回空 | P1 |
| R-07e | 热加载改为 8 | Redis 里变为 8，`_meta.reloadResult=ok` | P0 |
| R-07f | 热加载写错值、被拒绝 | Redis 里仍为 8，`_meta.reloadResult=failed` | P1 |
| R-07g | 从文件删掉这一项 | Redis 里不再有这一项（使用默认值） | P1 |
| R-07h | `GmmsConfigPublishEnable=false` | 之后的热加载不再写入 | P2 |

## 3. 测试中发现并修复的问题

| 问题 | 处理 |
|---|---|
| R-04 第一版：移出白名单的项在日志里显示为 `Unknown`，而不是 `Restart required` | 这些项以及 `AsyncSql*`、`DbHa.*`、`ModuleStatus*`、`DataControl_*`、`SystemManager.*`、`MGM.*`、`Mail*` 归入"需要重启" |
| R-05 第一版用 `ThreadPool.MaxPoolSize`，但本地配置文件里没有这一项，无法判断原值的类型 | 改用文件里已有的 `Redis_master_port`。没有原值的项无法判断类型，这是已知限制 |
| R-07d2 第一版：文件改回原值后，`_meta.pendingRestart` 仍保留旧记录 | 改为每次热加载都按"文件里的值 ≠ 正在使用的值"重新计算；文件没有变化时也会清空 |
| `ConfigReloadIT` 的 CFG-04 按旧行为断言"删掉的项保持原值" | 按新行为改为"恢复默认值" |

## 4. 不在本地范围的项

| ID | 项 | 建议做法 |
|---|---|---|
| X-R1 | 线上各模块的生效配置 | 上线后 `SMEMBERS config:effective:modules` 应列出所有模块；抽查 `HGETALL`，与各节点的配置文件对比 |
| X-R2 | 移出白名单的项 | 上线前检查运维脚本，看是否依赖"热加载后这些项生效"（实际原来就不生效） |
| X-R3 | 改小 `TotalShards` | 现在要求现有分片配置在新范围内，否则拒绝热加载；需要先改 `MyShards`/`CoreShard.*`，再改 `TotalShards`，或同时修改 |

## 5. 回归结果

执行时间 2026-11-01 18:39（AEDT）；`run-all.sh` 从头全量编译后执行 24 个套件。第一次执行中 `ConfigReloadIT` 的 CFG-04 按旧行为断言失败，修改测试后单独重跑 `ConfigReloadIT` 68 项全部通过。

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
| ConfigReloadIT | 68 | 68 | 0（重跑） |
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
| **GmmsReloadIT（新增）** | 23 | 23 | 0 |
| **合计** | **467** | **467** | **0** |
