# 配置文件生效内容写入 Redis：测试用例与执行报告

> 设计见《配置文件生效内容写入Redis设计》。同时对此前所有批次做了全量回归。

## 1. 测试环境

本地 Redis 7.2.5，JDK 17。测试进程内完成真实初始化（模块 `CoreEngine1`，NodeID 1）：客户、路由等在 Redis 连上之前加载，用来验证"Redis 还没连上时先排队、连上后写入"。reload 通过 Redis 控制频道 `channel:gmms:control` 发送真实的命令（`RELOAD_CUSTOMER`、`RELOAD_RECIPIENT_RULE`）。测试中临时改写 cm.cfg、`OperatorMode.properties`、`recipientRuleFile.cfg`，结束后恢复。

本地 cm.cfg 有 34 个客户块，`conf/routing/` 有 2 个块；`senderReplacement.cfg` 等文件为空。

## 2. 用例（`ConfigFileIT`）

| ID | 场景 | 预期 / 实测 | 级别 |
|---|---|---|---|
| F-01a | 启动 | 客户在 Redis 连上之前已加载；连上后写入，`config:files:CoreEngine1:1` 有 `cm.version` | P0 |
| F-01b | 同上 | `HGET config:file:cm:{版本} VENDOR9` 得到这个客户的 JSON，key 为文件里的写法（`CCBSSID`、`SMSOptionCoreProcessorThrottlingNum` 等）；`config:file:cm:{版本}:ssid` 中 9 → VENDOR9 | P0 |
| F-01c | 同上 | 每个客户块一项（34 / 34），`:meta` 的 `format=blocks`、`entries=34` | P0 |
| F-01d | 同上 | 连接定义里的密码打码：`IncomingConnection` 中为 `password=******`，原密码不出现 | P0 |
| F-01e | 同上 | `cm.result=ok`；内容的 TTL 为 7 天；模块在 `config:effective:modules` 中 | P1 |
| F-02a | 启动 | 路由写入，2 个块 | P0 |
| F-02b | 启动 | `phonePrefix`、`vendorTemplate`、`systemVendorReplace`、`operatorMode` 都有记录（空文件为 0 项） | P1 |
| F-03a | cm.cfg 新增客户 NEWCUST95 和写错的 BADCUST96（CCBSSID 非数字），`RELOAD_CUSTOMER` | 生成新版本 | P0 |
| F-03b | 同上 | 新版本里有 NEWCUST95、没有 BADCUST96；`cm.result=partial`，`cm.failed=BADCUST96` | P0 |
| F-03c | 同上 | 旧版本已经没有模块在用，被删除 | P0 |
| F-04 | 另一个模块记录还在用当前版本时，cm.cfg 改回原样并 reload | 当前版本保留不删 | P1 |
| F-04b | 同上 | 内容与启动时相同，版本号也与启动时相同 | P1 |
| F-05 | 修改 `OperatorMode.properties` | 监控线程发现修改后写入：`StoreMode=8888`，`format=properties` | P1 |
| F-06 | 修改 `recipientRuleFile.cfg`，`RELOAD_RECIPIENT_RULE` | 按行写入，`000002` 为文件第 2 行 | P1 |
| F-07 | `ConfigFilePublishEnable=false` 后 reload | 不再写入 | P2 |

## 3. 测试中发现并修复的问题

| 问题 | 处理 |
|---|---|
| F-01b 第一版：写入的 key 全部是小写（`ccbssid`、`smsoptioncoreprocessorthrottlingnum`）。原因是 `A2PCustomerConfig` 解析时把 key 转成小写存储 | `A2PCustomerConfig` 另外记录每个 key 在文件里的写法（新增 `getLinesWithOriginalNames()`），写 Redis 时使用。按 key 查找仍不区分大小写，原有逻辑不变 |
| F-06 第一版用块格式写测试文件，加载失败（`recipientRule.result=failed`） | `recipientRuleFile.cfg` 实际是 JSON，测试改用 JSON 内容。也说明加载失败时只记录 `failed`、内容保持不变是正确的 |

## 4. 不在本地范围的项

| ID | 项 | 建议做法 |
|---|---|---|
| X-F1 | 线上 cm.cfg 的大小 | 上线后看 `config:file:cm:{版本}` 的内存占用（`MEMORY USAGE`），确认在 state Redis 的容量范围内 |
| X-F2 | 多模块共用版本 | 各模块 reload 完成后，`config:files:*` 里的 `cm.version` 应相同，`config:file:cm:*` 只保留一个版本 |
| X-F3 | 内容模板、`antiSpam.cfg`、`MQMTaskConfig.properties` | 本地没有这些文件的内容，需在测试环境确认写入（`contentTemplate`、`antiSpam`、`mqmTask`） |

## 5. 回归结果

执行时间 2026-11-01 19:05（AEDT）；`run-all.sh` 从头全量编译后执行 25 个套件，全部通过。

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
| **ConfigFileIT（新增）** | 15 | 15 | 0 |
| **合计** | **482** | **482** | **0** |

## 6. Redis 中的显示示例

以下是本地实际写入 Redis 的内容（用 `redis-cli` 读取；文件路径中的本地目录写作 `$A2P_HOME`）。senderReplacement、OperatorMode、recipientRule 使用测试时临时写入的示例内容。**所有块文件存的都是文件里的原值**，配置项名字保持文件里的写法；文件里没写的项不出现。

### 6.1 模块在用的各个文件

```
> HGETALL config:files:CoreEngine1:1
cm.entries = 34
cm.failed =
cm.file = $A2P_HOME/conf/cm.cfg
cm.fileMd5 = b4eeba39803cdabed241cbe0da73e89a
cm.loadedAt = 2026-11-01 19:13:53.506+1100
cm.result = ok
cm.version = ae249b6c2a41bf6e295e45862b5a788a
phonePrefix.entries = 2
phonePrefix.file = $A2P_HOME/conf/senderReplacement.cfg
phonePrefix.result = ok
phonePrefix.version = 5e0f05dbf8dbedbc392c61c6529a90a7
routing.entries = 2
routing.file = $A2P_HOME/conf/routing
routing.version = fd0e8b006921cde87c755ea02f0f9220
operatorMode.version = 8754d43dbfc470de761b7daccec12a32
recipientRule.version = e75d1e5754c2a17169d1dc760fc09d4a
vendorTemplate.entries = 0
vendorTemplate.version = d41d8cd98f00b204e9800998ecf8427e
...（每个文件都有 version、loadedAt、result、file、entries，这里省略部分）
```

`cm.fileMd5` 是 cm.cfg 文件本身的 md5；`cm.version` 是实际在用内容的 md5，有客户加载失败时两者对不上。空文件的版本是空内容的 md5（`d41d8cd9...`）。

### 6.2 cm.cfg：按客户查

```
> HGET config:file:cm:ae249b6c2a41bf6e295e45862b5a788a VENDOR9
{"A2PPlayerType":"operator","CCBSSID":"9","CoreHoldMaxSeconds":"5","Protocol":"SMPP",
 "SMSOptionCoreProcessorThrottlingNum":"2","ShortName":"VENDOR9"}

> HGET config:file:cm:ae249b6c2a41bf6e295e45862b5a788a SMPPCUST31
{"A2PPlayerType":"operator","BufferTimeout":"3","CCBSSID":"31","ConnectionType":"MC",
 "IncomingConnection":"(ConnName=SMPPCUST31_c1,userName=c31,password=******,URL=127.0.0.1,SessionNumber=200,MinReceiverNumber=1,MaxReceiverNumber=5,BindMode=TRX,Version=V34)",
 "Protocol":"SMPP","ShortName":"SMPPCUST31","WindowSize":"20"}

> HGET config:file:cm:ae249b6c2a41bf6e295e45862b5a788a:ssid 31
SMPPCUST31

> HGETALL config:file:cm:ae249b6c2a41bf6e295e45862b5a788a:meta
entries = 34
file = $A2P_HOME/conf/cm.cfg
format = blocks
name = cm
version = ae249b6c2a41bf6e295e45862b5a788a
writer = CoreEngine1:1
writtenAt = 2026-11-01 19:13:53.504+1100
```

`ShortName` 是解析时自动加上的块名。连接定义里的密码显示为 `password=******`。

### 6.3 senderReplacement.cfg

文件内容：

```
[VENDOR8]
CCBSSID: 8
SenderReplacement: (PhonePrefix=86,ReplaceTo=10690000,SenderAddress=TESTSENDER,Ossid=8888,Rssid=8)

[VENDOR9]
CCBSSID: 9
SenderReplacement: (PhonePrefix=852,ReplaceTo=HKBRAND,SenderAddress=all,Ossid=all,Rssid=9)
```

Redis 中：

```
> HGETALL config:file:phonePrefix:5e0f05dbf8dbedbc392c61c6529a90a7
VENDOR8 = {"CCBSSID":"8","SenderReplacement":"(PhonePrefix=86,ReplaceTo=10690000,SenderAddress=TESTSENDER,Ossid=8888,Rssid=8)","ShortName":"VENDOR8"}
VENDOR9 = {"CCBSSID":"9","SenderReplacement":"(PhonePrefix=852,ReplaceTo=HKBRAND,SenderAddress=all,Ossid=all,Rssid=9)","ShortName":"VENDOR9"}
```

### 6.4 路由（conf/routing/）

块名取自文件名（`CUST8888_routingInfo.cfg` → `CUST8888`）：

```
> HGETALL config:file:routing:fd0e8b006921cde87c755ea02f0f9220
CUST7001 = {"A2PRoutingRelay":"(PhonePrefix=86,Relay=VENDOR9)","CCBSSID":"7001"}
CUST8888 = {"A2PRoutingRelay":"(PhonePrefix=86,Relay=VENDOR8)","CCBSSID":"8888"}
```

### 6.5 OperatorMode.properties（Properties 格式：key → value）

```
> HGETALL config:file:operatorMode:8754d43dbfc470de761b7daccec12a32
DRStoreMode = 9
ROP-FailedMode =
StoreMode = 8888
```

### 6.6 recipientRuleFile.cfg（文本格式：行号 → 这一行）

```
> HGETALL config:file:recipientRule:e75d1e5754c2a17169d1dc760fc09d4a
000001 = [
000002 = {"cc":"86","lens":[11],"allowPrefixList":["13","15","18"]}
000003 = ]
```
