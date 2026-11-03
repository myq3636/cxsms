# Core 模块功能与测试用例清单

> 范围：Core（`CoreEngine`，`DeliveryRouter`）当前 `main` 的代码。“回归现状”依据 `regression/` 现有用例。第一批（黑白名单、号码检查、路由优先级）、第二批（重试、MQM 回写）、第三批（多进程故障、假 DR 和延迟 DR、DR 改道）和第四批（内容处理、反垃圾、有效期和定时、部分热加载）已实现，结果见第 8～11 节；标“待补”的用例还没有实测。

## 1. 总览

Core 负责消费 MT、做检查和路由、外发到出向 stream，并处理结果和 DR。

- **已支持功能：** 45 组，其中回归已覆盖 30 组，部分覆盖 10 组，没有覆盖 5 组。
- **测试用例：** 120 条，其中已覆盖 95 条，部分覆盖 8 条，待补 17 条。
- **主要缺口：** 可靠性机制（去重、outbox、限速、长短信、分片）测得比较全，**部分业务功能还没有回归用例**：运营商和 ENUM 路由、第二路由、DR 映射和查询、部分热加载。

## 2. 已支持的功能

现状说明：已覆盖 = 回归有用例验证行为；部分 = 只验证了其中一段；待补 = 回归没有用例，功能在代码和配置中存在，但本文档没有实测。

### MT 接收与处理

| 功能 | 主要配置 | 回归现状 |
|---|---|---|
| 从 `stream:mt:pending:{ossid}:{分片}` 消费 MT，门铃调度，大客户让出 worker | `RedisStream*`、`TotalShards`、`CoreShard.*` | 已覆盖 |
| 多 Core 分片消费，启动时校验分片分配 | `CoreShard.{模块}` | 已覆盖 |
| 发送方、内容黑白名单（全局和客户级），拒绝时写 CDR 并回拒绝 DR | `conf/blacklist.cfg`、`whitelist.cfg`、`SMSOptionSupportBlackList`、`CustomerTypeBySender` | 已覆盖 |
| 号码前缀增删、号码和长度校验 | `SMSOptionAddOriPrefix*`、`SMSOptionAllowOriPrefixList`、`SMSOption*LenList`、`SMSOptionIsRecipitLenCheck` | 部分 |
| `recipientRuleFile.cfg` 接收号规则，非 ASCII 超长拒绝，同号 24 小时上限 | `recipientRuleFile.cfg`、`SMSOptionRecipientMaxSendCountIn24H`、`BlackholeSsid` | 已覆盖 |
| 重复消息检查 | `DuplicateMessagePeriod`、`DuplicateMessageServiceTypeIDList`、`CheckDuplicateMessageContent` | 已覆盖 |
| 反垃圾过滤 | `needAntiSpam`、`SMSOptionSupport*AntiSpam`、`conf/antiSpam.cfg` | 已覆盖 |
| service type id 校验 | `MaxServiceTypeID`、`Smpp.ServiceTypeIDTag` | 已覆盖 |
| 有效期计算、定时发送 | `ExpireTime`、`FinalExpireTime`、`MessageExpireTime`、`Min/MaxMessageExpireTimeFromCust`、`SMSOptionParseValidityPeriod` | 部分 |
| MT 处理去重（重投不重复写 CDR、DR、外发） | `CoreMtIdempotency`、`CoreMtLeaseMs`、`CoreMtMarkerTtlSeconds` | 已覆盖 |
| 超过投递次数进死信队列 | `stream:dlq:*` | 已覆盖 |
| DB 兜底（路由失败、无 rssid、RA2P 无效等） | `CoreDbFallbackInline` | 已覆盖 |

### 路由与选通道

| 功能 | 主要配置 | 回归现状 |
|---|---|---|
| 路由优先级：第二路由 → 号码路由 → 运营商路由 → 主路由 → 备用路由 | `conf/routing/`、`smsOptionNeedSupportSecondRouting` | 部分 |
| 按前缀、发送方、内容关键字选主路由 | `A2PRoutingRelay`、`A2PSenderRoutingRelay`、`A2PContentKeywordRoutingRelay` | 已覆盖 |
| 按比例分流 | `Relay=a:20,b:80` | 已覆盖 |
| 默认路由文件（`{业务类型}_default`） | `DEFAULT_ROUTING_SUFFER` | 已覆盖 |
| ENUM、IR21 查询后按运营商路由 | `SMSOptionEnumQueryRecPrefix`、`SMSOptionIR21EnumQueryRecPrefix`、`mnpqueryurl` | 待补 |
| 路由增量 reload、通道路由索引 | `RELOAD_ROUTING[:-f]` | 已覆盖 |
| 黑洞通道和按比例黑洞 | `BlackholeSsid`、`BlackholePercent`、`BlackholeDrStatusCode` | 部分 |

### 外发

| 功能 | 主要配置 | 回归现状 |
|---|---|---|
| 写入 `stream:mt:routed`、`stream:http:submit` | — | 已覆盖 |
| 内容处理：拆分、编码转换、UDH、后缀、内容替换、模板 | `SupportMsgSplit`、`MsgSplitLen`、`SMSOption*GSM7bit`、`SmsOptionIsSupportUDH`、`SMSOptionContentReplacementPolicy`、`conf/contentTemplate.cfg` | 部分 |
| 发送方替换（`senderReplacement.cfg`、高级替换） | `SMSOptionAdvancedSenderReplacement`、`SenderMapping` | 部分 |
| 拒绝区域 | `SMSOptionRejectRegions` | 待补 |
| 通道限速暂存放行，积压和超时处理 | `SMSOptionCoreProcessorThrottlingNum`、`CoreHoldMaxSeconds`、`CoreHold*` | 已覆盖 |
| outbox：只认第一个结果，超时收回或判失败 | `CoreOutboundOutbox*`、`CoreOutboundUnreadStallMs`、`CoreOutboundRespTimeoutMs` | 已覆盖 |

### 长短信

| 功能 | 主要配置 | 回归现状 |
|---|---|---|
| Redis 模式分段合并、收齐后外发，任意 Core 可完成 | `CsmReassembleMode=redis` | 已覆盖 |
| 超时扫描、回过期 DR | `CoreCsmExpiry*`、`CsmIntegrityCacheTimeout` | 已覆盖 |
| legacy 模式 | `CsmReassembleMode=legacy` | 部分 |

### 结果与重试

| 功能 | 主要配置 | 回归现状 |
|---|---|---|
| 消费 submit_resp、DR_resp，分片结果 stream | `stream:core:results:*` | 已覆盖 |
| 按失败码重试（RMQ）、重试策略 | `SMSOptionSubmitRetryCode`、`SMSOptionMTRetryPolicy`、`SMSOptionSenderRetryPolicy`、`DefaultRetryPolicy` | 已覆盖 |
| 失败改道（备用路由、DR 改道）和第二路由 | `NeedDRReroute`、`SMSOptionDRRerouteCode`、`RerouteDRDelayTimeInSec`、`A2PBackupRoutingRelay`、`A2PSec*RoutingRelay` | 部分 |
| 最后一次失败回失败 DR，错误码转 DR 状态 | `SMSOptionSubmitErrorCodesForFakeDR` | 已覆盖 |
| 重试和延迟消息由 MQM 写回 Core | `stream:mqm:submit:*`、`stream:mqm:report:*` | 部分 |

### DR

| 功能 | 主要配置 | 回归现状 |
|---|---|---|
| 消费供应商 DR，匹配原消息后转发客户，先到的 DR 等待重试 | `stream:core:inbound-dr:*` | 已覆盖 |
| DR 去重 | `CoreDrResultIdempotency` | 已覆盖 |
| DR 选择 Server 模块，心跳判断离线 | `SmppServerDrLivenessCheck` | 已覆盖 |
| 同步写出 DR | `CoreDrInlineDispatch` | 已覆盖 |
| 假 DR、延迟 DR | `SMSOptionSendFakeDR`、`SMSOptionSendDelayDRImmediately`、`SMSOptionDRDelayTimeMinMaxInSec`、`AutoInDR.*` | 已覆盖 |
| DR 状态映射 | `SMSOptionDRStatusMapping`、`SMSOptionIsNotSupportUnknowDRStatus` | 待补 |
| 主动查询 DR（REST） | `SMSOptinIsSupportHttpQueryMessage` | 待补 |
| DR 存储模式 | `StoreDRModeEnable`、`OperatorMode.properties` | 待补 |

### 运维

| 功能 | 主要配置 | 回归现状 |
|---|---|---|
| CDR 异步写入、统计 | `SMSOptionWrMonitorCDR`、`NeedStatisticMsgCount*` | 部分 |
| 全链路日志 | — | 已覆盖 |
| 热加载：cm、路由、GmmsConfig 等，内容写入 Redis | `RELOAD_*` | 已覆盖 |
| 改 shortName、改 ssid、连续 reload | `RELOAD_CUSTOMER` | 已覆盖 |

## 3. 不支持、已停用或有限制的功能

### 3.1 代码里已停用

| 功能 | 现状 | 依据 |
|---|---|---|
| 入口统一按 DNS 查运营商（`iosmsRouteToOperator`） | 调用已注释。运营商路由只有配置了 ENUM、IR21 前缀的客户才生效 | `IOSMSDispatcher.java:111` |
| 路由策略控制（`PolicyControl`） | 调用已注释，不生效 | `IOSMSDispatcher.java:116` |
| 租号（SenderRent） | 同步路径不走租号，需要租号的消息走 DB 兜底。`SMSOptionRentAddrPrefix`、`SMSOptionConditionToRentAddr` 实际不能完成租号 | `SyncSubmitRouter.java` |
| 异步运营商查询（`ASYQueryOP`） | 代码保留了分支，当前 `dispatch` 没有看到返回它的路径 | `SyncSubmitRouter.java`、`IOSMSDispatcher.java` |
| `handleOutSubmit4CmccTestSms` | 只有定义，没有调用方 | `MessageStoreManager.java` |

### 3.2 配置存在但不起作用

| 配置 | 说明 |
|---|---|
| `SMSOptionPiggyBackVirtualDC` | 只在 `A2PCustomerInfo` 解析，没有读取方 |
| `NeedCheckSession` | 只在旧的异步路径（`CorePrioritrySender`）使用，stream 主流程不用 |
| `DRInterval`、`DNSTimeOut` | 配置说明已标注不再使用 |
| `ServerDrWindowFullRetryBatchSize` | 配置说明已标注不再使用（Server 侧） |

### 3.3 有限制的行为

| 功能 | 限制 | 依据 |
|---|---|---|
| 通道限速 | 按单个 Core 节点生效，总速率 = 配置值 × Core 节点数，不是全局限速 | 配置说明 4.9 |
| 不重复外发 | 不保证严格一次。结果超时（2700）后如果重发，供应商可能收到两次 | 配置说明 4.8 |
| DR 不重复 | 可能重复。设计上接受少量重复 DR | Server 批次决定 |
| 客户断线时的 DR | 没有宽限期，按决定不做（D5）。客户不在线时 DR 回失败，走重试策略 | Server 第 2 批设计 |
| 计费退款 | Core 不做计费；Server 扣费后，Core 判失败也不退款。按决定不改 | Server 第 1 批设计 |
| 长短信 Redis 模式 | state Redis 必须是 `noeviction`，否则分段可能被清掉 | 配置说明 4.7 |
| Core 故障接管 | 分片是静态配置，Core 挂掉后不会自动接管。积压等待重启，或手动改分片后执行 `RELOAD_CORE_SHARDS` | `FT-06` |
| `Relay=a;b` | 配置能写但不生效，`;` 后面的部分被丢弃。改道用 `A2PBackupRoutingRelay` | `RT-05` |
| 消息顺序 | 没有端到端顺序保证。同一发送方和接收方进同一分片，但重试、改道、长短信都可能改变顺序（读代码推断，未实测） | `calculateShardId` |

### 3.4 Core 不负责的

SMPP/HTTP 协议、bind 鉴权、入向限流和计费扣费在 Server；真正发给供应商、窗口和供应商限速在 Client；定时扫描 RMQ、SMQ 等重试队列在 MQM。

## 4. 测试用例

现状含义同第 2 节。预期来自配置说明和代码阅读；**待补的用例还没有实测，实现时如果行为不同，要先确认是预期写错还是产品缺陷。**

### 一、MT 主链路

| ID | 级别 | 配置 / 前置 | 场景 | 预期 | 现状 | 现有用例 |
|---|---|---|---|---|---|---|
| MT-01 | P0 | 路由配好，通道在线 | 提交一条 MT | 写入 `stream:mt:routed`；登记 outbox；写 IN_SUBMIT CDR | 已覆盖 | CoreE2EIT |
| MT-02 | P0 | 两个 Core 分片 | 不同发送方和接收方的 MT | 只被负责该分片的 Core 消费 | 已覆盖 | ShardGroupIT、Batch4IT |
| MT-03 | P0 | 同一条 MT 被重投 | 确认前崩溃，然后重投 | 不重复写 CDR、DR、外发 | 已覆盖 | Batch3IT |
| MT-04 | P0 | 处理中的 MT | 另一个 Core 在占用期内领取 | 不重复处理；占用过期后可接手 | 已覆盖 | Batch3IT |
| MT-05 | P1 | 超过最大投递次数 | 反复失败 | 进 `stream:dlq:submit` | 已覆盖 | Batch4IT |
| MT-06 | P1 | 无效消息体 | stream 中有无法解析的条目 | 进死信，不阻塞后续消息 | 已覆盖 | Batch4IT |
| MT-07 | P1 | 大小客户同时积压 | 大客户积压，小客户少量 | 小客户不被长时间阻塞 | 已覆盖 | Batch4IT |

### 二、前置检查

| ID | 级别 | 配置 / 前置 | 场景 | 预期 | 现状 | 现有用例 |
|---|---|---|---|---|---|---|
| CK-01 | P0 | 发送方在全局黑名单 | 提交 MT | 拒绝；写 CDR；要 DR 时回拒绝 DR | 已覆盖 | CoreCheckIT |
| CK-02 | P0 | 发送方在客户黑名单 | 提交 MT | 同上 | 已覆盖 | CoreCheckIT |
| CK-03 | P1 | `CustomerTypeBySender=VIP` | 发送方在黑名单 | 跳过发送方检查 | 已覆盖 | CoreCheckIT |
| CK-04 | P1 | 接收号以 65 开头（`+65`、`0065`、`065` 同样处理） | `NonPA`（默认）：只查全局黑名单，再查客户自己的白名单，默认白名单不起作用；`PA`：查全局和客户黑名单，没有客户白名单时查默认白名单 | `NonPA` 没有命中客户白名单就拒绝（`REJECTED_SDWL`），命中全局黑名单回 `REJECTED_SDBL`；替换条目（`rp`）和全国家条目（`all`）同样生效；`PA` 按默认白名单判断 | 已覆盖 | CoreCheckIT |
| CK-05 | P0 | 客户发送方白名单 | 不在名单内的发送方 | 拒绝 | 已覆盖 | CoreCheckIT |
| CK-06 | P0 | 内容命中全局或客户黑名单 | 提交 MT | 拒绝，回 DR | 已覆盖 | CoreCheckIT |
| CK-07 | P1 | 不要求 DR | 被黑名单拒绝 | 不回 DR，只写 CDR | 已覆盖 | CoreCheckIT |
| CK-08 | P0 | 接收号非数字 | 提交 MT | 失败，走失败处理（Server 不拦截，见 Server 报告 S-12、S-13） | 已覆盖 | CoreCheckIT |
| CK-09 | P1 | `SMSOptionRecNumberLenList` | 长度不符 | 失败 | 已覆盖 | CoreCheckIT |
| CK-10 | P1 | `SMSOptionAllowOriPrefixList` | 前缀不符 | 失败 | 已覆盖 | CoreCheckIT |
| CK-11 | P1 | `recipientRuleFile.cfg` | 不符合规则 | 拒绝，rssid 记为 `BlackholeSsid` | 已覆盖 | CoreCheckIT |
| CK-12 | P1 | 非 ASCII 超过 140 字节 | 提交 MT | 拒绝，记 `BlackholeSsid` | 已覆盖 | CoreCheckIT |
| CK-13 | P1 | `SMSOptionRecipientMaxSendCountIn24H=2` | 同号第 3 条 | `RECIPIENT_ERROR_BY_MAX_COUNT`；重投不重复计数 | 已覆盖 | CoreCheckIT |
| CK-14 | P1 | `DuplicateMessagePeriod` | 时间内重复 | 按重复处理；超过时间正常 | 已覆盖 | CoreCheckIT |
| CK-15 | P2 | `CheckDuplicateMessageContent` | 同号不同内容 | 不算重复 | 已覆盖 | CoreCheckIT |
| CK-16 | P1 | `SMSOptionSupportIncomingAntiSpam`、`SMSOptionSupportOutgoingAntiSpam`、`antiSpam.cfg` | 命中入向、出向关键字 | 不外发，回失败 DR（10700）；干净内容正常 | 已覆盖 | CoreContentIT |
| CK-17 | P2 | `MaxServiceTypeID=3` | service type id 为 5、3 | 5 校验失败，回失败 DR（10500）；3 正常 | 已覆盖 | CoreContentIT |
| CK-18 | P1 | 消息没带有效期 | 提交 MT | 用客户 `ExpireTime`（**单位是小时**）；没配用全局默认 | 已覆盖 | CoreContentIT |
| CK-19 | P1 | `SMSOptionParseValidityPeriod` | 有效期在允许范围内、超范围 | 范围内采用；超范围用默认值。解析在 Server 的协议层（`Smpp.java`），不在 Core，要放到 Server 用例里测 | 待补 |  |
| CK-20 | P1 | 消息带定时时间（`ScheduleDeliveryTime`） | 未到、已过定时时间 | 未到：写 SMQ，不外发；已过：直接外发。SMQ 到时间后由 MQM 取出这一步要查 DB，没有测 | 部分 | CoreContentIT |

### 三、路由

| ID | 级别 | 配置 / 前置 | 场景 | 预期 | 现状 | 现有用例 |
|---|---|---|---|---|---|---|
| RT-01 | P0 | `A2PRoutingRelay` | 按前缀匹配 | 走对应通道；最长前缀优先 | 已覆盖 | CoreRouteIT |
| RT-02 | P0 | `A2PSenderRoutingRelay` | 匹配发送方 | 优先于前缀路由 | 已覆盖 | CoreRouteIT |
| RT-03 | P0 | `A2PContentKeywordRoutingRelay` | 匹配关键字 | 优先于前缀路由，低于发送方路由 | 已覆盖 | CoreRouteIT |
| RT-04 | P0 | `Relay=a:20,b:80` | 发 1000 条 | 比例接近配置 | 已覆盖 | CoreRouteIT |
| RT-05 | P0 | `Relay=a;b` | 主通道失败 | **`;` 后面的部分在加载时被丢弃，不是主备切换**：只走 a，失败后回失败 DR，不切到 b。要改道用 `A2PBackupRoutingRelay`（RE-12） | 已覆盖 | CoreDrBizIT |
| RT-06 | P0 | `A2PBackupRoutingRelay` | 主路由未命中 | 走备用路由 | 已覆盖 | CoreRouteIT |
| RT-07 | P1 | 号码路由，`Relay` 写通道 ssid | 匹配号码；一组多个号码；同号多个 `Priority`；按比例分流；未列出的号码；其他客户；ssid 不存在 | 优先于发送方和前缀路由；`Priority` 小的优先；号码路由按客户生效；未匹配或 ssid 不存在时走前缀路由 | 已覆盖 | CoreRouteIT |
| RT-08 | P1 | 运营商路由和备用 | 运营商已知 | 先走运营商路由，失败走备用，最后回前缀路由 | 待补 |  |
| RT-09 | P1 | `SMSOptionEnumQueryRecPrefix` | 查询成功、失败、超时 | 成功按运营商路由；失败走普通路由，消息不丢 | 待补 |  |
| RT-10 | P2 | `SMSOptionIR21EnumQueryRecPrefix` | IR21 查询 | 同上 | 待补 |  |
| RT-11 | P1 | `smsOptionNeedSupportSecondRouting` | 重发时 | 走第二路由；有效期内记录生效 | 待补 |  |
| RT-12 | P1 | `{CustomerServiceType}_default` 路由文件 | 客户没有路由 | 走默认路由 | 已覆盖 | CoreRouteIT |
| RT-13 | P0 | 无路由 | 提交 MT | DB 兜底，写 CDR，回失败 DR 或重试 | 已覆盖 | Batch7IT |
| RT-14 | P1 | 路由指向已删除通道 | 提交 MT | 按未路由失败（2120） | 已覆盖 | RenameReloadIT |
| RT-15 | P0 | 路由变更 | `RELOAD_ROUTING` | 只加载改动的文件；单文件错误不影响其他 | 已覆盖 | IncrementalRoutingIT |
| RT-16 | P1 | `Role=blackhole` 通道；`BlackholePercent=100` | 提交 MT | 进黑洞，不外发，写 CDR 后走延迟 DR；按比例黑洞时，计时窗口内第一条仍外发 | 已覆盖 | CoreRouteIT |
| RT-17 | P2 | `BlackholePercent=30` | 发 1000 条 | 比例接近配置；RMQ 重试的消息不再进黑洞 | 待补 |  |

### 四、外发与内容处理

| ID | 级别 | 配置 / 前置 | 场景 | 预期 | 现状 | 现有用例 |
|---|---|---|---|---|---|---|
| OUT-01 | P0 | SMPP 通道 | 外发 | 写 routed 流 | 已覆盖 | CoreE2EIT |
| OUT-02 | P0 | HTTP 通道 | 外发 | 写 `stream:http:submit`，不写 SMPP stream | 已覆盖 | CoreContentIT、ClientBatch2IT |
| OUT-03 | P0 | 超长消息，`ChlMsgSplit=yes`、`MsgSplitLen` | 按长度拆分 | 段数正确，段序号正确，拼起来等于原文；每段都有 outbox | 已覆盖 | CoreContentIT、Batch3IT |
| OUT-04 | P1 | 客户 `SupportMsgSplit=no` | 超长消息 | 不拆分，整条外发 | 已覆盖 | CoreContentIT |
| OUT-05 | P1 | `SMSOptionConvert2GSM7bit`（需 `SMSOptionSupportOutgoingGSM7bit` 不为 0）、`SMSOptionForceTrans2ASCII` | 转码 | ASCII 转 GSM 7bit；7bit 消息在不支持 7bit 的通道转 ASCII | 已覆盖 | CoreContentIT |
| OUT-06 | P1 | `SmsOptionIsSupportUDH=No` | 带 UDH 的消息 | UDH 被去掉；其他通道保留；只有一段的拼接头被去掉。`SMSOptionRemoveUdh` 在 Client 编码时生效，这里没有测 | 已覆盖 | CoreContentIT |
| OUT-07 | P1 | `SMSOptionContentReplacementPolicy` | 匹配内容 | `prefix=all` 和号码前缀的规则都生效；其他国家只用 `all` | 已覆盖 | CoreContentIT |
| OUT-08 | P1 | 路由 `ReplaceContentByTemplate`、`contentTemplate.cfg` | 内容里有、没有验证码 | 有：用模板，代入验证码。没有：不外发，回失败 DR（10503）；`DrSuccForTemplateReplaceFailed` 时回 DELIVERED | 已覆盖 | CoreContentIT |
| OUT-09 | P1 | `senderReplacement.cfg` | 匹配规则 | 发送方被替换；reload 后生效 | 部分 | RenameReloadIT |
| OUT-10 | P1 | `SMSOptionAdvancedSenderReplacement`、`senderReplacement.cfg` | 高级替换 | 匹配的发送方被替换；其他发送方、没开选项的客户不变 | 已覆盖 | CoreContentIT |
| OUT-11 | P1 | `SMSOptionRejectRegions` | 目标区域被拒绝 | 不外发，失败处理。要用 `conf/phone.dat`，回归环境没有这个文件，所以没有测 | 待补 |  |
| OUT-12 | P0 | `SMSOptionCoreProcessorThrottlingNum=2` | 发 10 条 | 按速率放行，超限不丢 | 已覆盖 | ChannelRateIT |
| OUT-13 | P0 | 积压超过上限 | 继续提交 | 2600 失败 | 已覆盖 | ChannelRateIT |
| OUT-14 | P1 | 超过 `CoreHoldMaxSeconds` | 暂存超时 | 10200，写 CDR，回失败 DR | 已覆盖 | ChannelRateIT |
| OUT-15 | P0 | Client 不读取 | 超过 `CoreOutboundUnreadStallMs` | 从 routed 收回，回 2500 | 已覆盖 | Batch3IT、CoreE2EIT |
| OUT-16 | P0 | Client 已读无结果 | 超过 `CoreOutboundRespTimeoutMs` | 回 2700 | 已覆盖 | Batch3IT |
| OUT-17 | P1 | 迟到或重复的结果 | 超时后才到 | 被丢弃（`[RESULT-DROPPED]`） | 已覆盖 | Batch3IT |

### 五、长短信

| ID | 级别 | 配置 / 前置 | 场景 | 预期 | 现状 | 现有用例 |
|---|---|---|---|---|---|---|
| CSM-01 | P0 | Redis 模式 | 乱序收齐分段 | 合并后外发一次 | 已覆盖 | Batch7IT |
| CSM-02 | P0 | 重复分段 | 同一段收到两次 | 不重复外发 | 已覆盖 | Batch7IT |
| CSM-03 | P0 | 两个 Core | 分段落在不同 Core | 任一个 Core 都能完成合并 | 已覆盖 | Batch7IT |
| CSM-04 | P0 | 分段未收齐 | 超过等待时间 | 每个已收到的分段回一条过期 DR | 已覆盖 | Batch7IT |
| CSM-05 | P1 | `SmsOptionIsSupportExpiredDR` | 开启、关闭 | EXPIRED、UNDELIVERABLE | 已覆盖 | Batch7IT |
| CSM-06 | P1 | 扫描中崩溃 | 占用到期 | 其他 Core 可接手 | 已覆盖 | Batch7IT |
| CSM-07 | P2 | `CsmReassembleMode=legacy` | 合并长短信 | 收齐后外发 | 部分 | Batch7IT（只验证选到旧模式） |

### 六、结果与重试

| ID | 级别 | 配置 / 前置 | 场景 | 预期 | 现状 | 现有用例 |
|---|---|---|---|---|---|---|
| RE-01 | P0 | 成功 submit_resp | 写结果 | 清 outbox；记录 DR 匹配信息；写 CDR | 已覆盖 | CoreE2EIT |
| RE-02 | P0 | 失败码在 `SMSOptionSubmitRetryCode` | 失败结果 | 写 RMQ，`RetriedNumber=1`，下次时间取策略第 1 个点；不回 DR；写 OUT_SUBMIT CDR | 已覆盖 | CoreRetryIT |
| RE-03 | P0 | 失败码不在重试列表 | 失败结果 | 不写 RMQ；立即回失败 DR | 已覆盖 | CoreRetryIT |
| RE-04 | P0 | `SMSOptionMTRetryPolicy=5S,1M,2M` | 一直失败 | 策略有 N 个点就**一共发送 N 次**（重试 N-1 次）；中间失败更新 RMQ，最后一次失败删除记录并回失败 DR；每次发送一条 OUT_SUBMIT，只有一条 IN_SUBMIT | 已覆盖 | CoreRetryIT |
| RE-05 | P1 | 通道 `SMSOptionMTRetryPolicy`、客户 `SMSOptionSenderRetryPolicy`、`SMSOptionMORetryPolicy`、`DefaultRetryPolicy` | 各级策略 | 优先级：通道 MT → 客户发送方 → 客户 MO → 全局默认 | 已覆盖 | CoreRetryIT |
| RE-06 | P1 | 下次重试时间超过有效期 | 失败结果 | 写 RMQ 但不设 `NextRetryTime`，重发查询不再选中，由 MQM 过期任务回 DR | 部分 | CoreRetryIT（过期任务要查 DB，未测） |
| RE-07 | P0 | MQM 写回 `stream:mqm:submit` | 备机不发，主机发 | Core 重新外发，保留重试次数和来源队列，不重复写 IN_SUBMIT | 已覆盖 | CoreRetryIT（RMQ 查询 SQL 未执行） |
| RE-08 | P0 | `stream:mqm:report` | MQM 重发 DR；客户收到后的结果；发送失败 | DR 写入客户 stream；成功后删除 RDQ 记录；发不出去时写 RDQ | 已覆盖 | CoreRetryIT |
| RE-09 | P1 | 重试时 DB 失败 | 写 RMQ 失败 | 不确认消费，留待重试 | 已覆盖 | Batch7IT |
| RE-10 | P1 | `SMSOptionSubmitErrorCodesForFakeDR`（客户配置） | 匹配的失败码 | 最后一次失败回成功 DR；没配的客户回失败 DR | 已覆盖 | CoreRetryIT |
| RE-11 | P1 | `NeedDRReroute`、`SMSOptionDRRerouteCode`、`A2PBackupRoutingRelay` | 供应商 DR | 改道码且在时间窗内：改走备用路由，不回 DR；其他失败码或超过 `RerouteDRDelayTimeInSec`：回 DR。时间窗有写死的时区假设，见第 10.2 节 | 已覆盖 | CoreDrBizIT |
| RE-12 | P1 | `A2PBackupRoutingRelay` | 外发失败 | 改走备用路由，不回 DR；备用路由也失败时不再改道，回失败 DR | 已覆盖 | CoreRetryIT |
| RE-13 | P1 | `SMSOptionOnlySupportDRRetry=true` | MT 失败且失败码在重试列表 | **实际仍写 RMQ 重试**，并改用全局默认策略（`10M*144`）；这一行为待确认，见第 9.2 节 | 已覆盖 | CoreRetryIT |
| RE-14 | P0 | 结果分片 | 同一通道结果落在多个 stream | 全部被消费 | 已覆盖 | Batch5IT |
| RE-15 | P1 | 结果重投 | 同一结果处理两次 | 不重复写 CDR、不重复写 RMQ | 已覆盖 | Batch6IT |

### 七、DR

| ID | 级别 | 配置 / 前置 | 场景 | 预期 | 现状 | 现有用例 |
|---|---|---|---|---|---|---|
| DR-01 | P0 | 供应商 DR | 匹配到原消息 | 转发给客户，状态正确；写 CDR | 已覆盖 | CoreDrFlowIT |
| DR-02 | P0 | DR 比 submit_resp 先到 | 匹配不到 | 等待后重试，最终转发 | 已覆盖 | CoreDrFlowIT |
| DR-03 | P1 | 始终匹配不到 | 超过重试次数 | 按规则处理，不阻塞 | 部分 | CoreDrFlowIT |
| DR-04 | P0 | DR 重投 | 处理两次 | 客户只收到一条 | 已覆盖 | Batch6IT |
| DR-05 | P0 | Server 被 kill | 心跳过期 | DR 发往有连接的 Server | 已覆盖 | DrLivenessIT、MultiServerIT |
| DR-06 | P0 | 客户没有活跃 Server | 写 DR | 写不出去时进 RDQ，不丢；Server 恢复后 MQM 重发，DR 写入客户 stream | 已覆盖 | CoreDrBizIT |
| DR-07 | P1 | `SMSOptionSendFakeDR` | 供应商接受 MT | 回假 DR，真实 DR 不再转发；供应商拒绝时回失败 DR；**没配 `SMSOptionDRSuccessRatio`（默认 0）时假 DR 是失败状态** | 已覆盖 | CoreDrBizIT |
| DR-08 | P1 | 客户 `SMSOptionSendFakeDR` 加通道 `SMSOptionSendDRBeforeOutSubmitInDC` | 供应商还没应答 | 先回假 DR；供应商应答后不再回第二条 | 已覆盖 | CoreDrBizIT |
| DR-09 | P1 | `SMSOptionSendDRByInSubmitInCU`、`SMSOptionSendFakeDRByInSubmitRecPrefixInCU` | 收到 submit | 立即回假 DR，真实 DR 不重复转发；按前缀配置时其他前缀不回 | 已覆盖 | CoreDrBizIT |
| DR-10 | P1 | `SMSOptionDRDelayTimeMinMaxInSec=3,4`、`AutoInDR.DirectStreamEnable=true` | 延迟 DR | 到时间前不发，在范围内发出 | 已覆盖 | CoreDrBizIT |
| DR-11 | P1 | `AutoInDR.*` | 调度线程停止再启动 | 到期 DR 留在 Redis，启动后发出。领取超时接手、真实进程重启、默认的 `DirectStreamEnable=false` 路径没有测 | 部分 | CoreDrBizIT |
| DR-12 | P1 | `SMSOptionDRSuccessRatio=70` | 发 100 条 | 成功率接近配置（实测 67%） | 已覆盖 | CoreDrBizIT |
| DR-13 | P1 | `SMSOptionDRStatusMapping` | 匹配状态 | 转成映射后的状态 | 待补 |  |
| DR-14 | P2 | `SMSOptionIsNotSupportUnknowDRStatus` | UNKNOWN 状态 | 按映射处理 | 待补 |  |
| DR-15 | P2 | `SMSOptinIsSupportHttpQueryMessage` | 主动查询 DR | 查询结果正确写回 | 待补 |  |
| DR-16 | P2 | `StoreDRModeEnable`、`DRStoreMode` | 处于存储模式 | DR 先存储，之后发送 | 待补 |  |
| DR-17 | P1 | `SMSOptionDRNotPaidCode` | 欠费状态码 | 按欠费处理 | 待补 |  |

### 八、配置热加载

| ID | 级别 | 配置 / 前置 | 场景 | 预期 | 现状 | 现有用例 |
|---|---|---|---|---|---|---|
| CF-01 | P0 | `RELOAD_CUSTOMER` | 改客户配置 | 只影响改动的客户；处理中的消息不受影响 | 已覆盖 | RenameReloadIT、RapidReloadIT |
| CF-02 | P0 | `RELOAD_GMMS_CONFIG` | 改 GmmsConfig | 白名单内的配置生效，写入 Redis | 已覆盖 | GmmsReloadIT、ConfigReloadIT |
| CF-03 | P1 | `RELOAD_CONTENT_TEMPLATE` | 改模板 | 新消息用新模板 | 已覆盖 | CoreContentIT |
| CF-04 | P1 | `RELOAD_PHONE_PREFIX` | 改发送方替换 | 新消息生效 | 已覆盖 | RenameReloadIT |
| CF-05 | P1 | `RELOAD_RECIPIENT_RULE` | 改接收号规则 | 新消息按新规则拒绝 | 已覆盖 | CoreContentIT、ConfigFileIT |
| CF-06 | P1 | `RELOAD_ANTISPAM` | 改反垃圾规则 | 新关键字生效 | 已覆盖 | CoreContentIT |
| CF-07 | P1 | `RELOAD_SYSTEM_REPLACE`、`RELOAD_VENDOR_REPLACE` | 改替换规则 | 新消息生效 | 待补 |  |
| CF-08 | P1 | 黑白名单 | 修改名单文件 | `RELOAD_SENDER_BLACKLIST` 后新名单生效。白名单、内容名单的 reload 生效没有测 | 部分 | CoreContentIT |
| CF-09 | P1 | 连续 reload | 处理流量时连续 reload | 不丢消息 | 已覆盖 | RapidReloadIT |

### 九、故障与恢复

| ID | 级别 | 配置 / 前置 | 场景 | 预期 | 现状 | 现有用例 |
|---|---|---|---|---|---|---|
| FT-01 | P0 | Core 崩溃 | 处理中崩溃 | 自动重投后完成，不重复 | 已覆盖 | Batch3IT |
| FT-02 | P0 | Redis 短暂不可用 | 处理中 | 消息不丢；恢复后继续 | 部分 | Batch7IT |
| FT-03 | P0 | DB 不可用 | 需要写 DB | 不确认消费，恢复后完成 | 已覆盖 | Batch7IT |
| FT-04 | P1 | CDR 写入积压 | 后台写入暂停 | 不阻塞消息处理，恢复后写入 | 已覆盖 | Batch4IT |
| FT-05 | P1 | 暂存被挂掉的 Core 读走 | 超过 `CoreHoldRecoverIdleMs` | 其他 Core 接手，不消耗令牌 | 已覆盖 | ChannelRateIT |
| FT-06 | P0 | 两个真实 Core 进程，各负责一个分片 | `kill -9` 一个 Core | **不会自动接管**：它的分片积压等待，另一分片不受影响，消息不丢。重启后，或用 `RELOAD_CORE_SHARDS` 把分片交给另一个 Core 后，积压和处理中的消息都完成，每条只外发一次 | 已覆盖 | CoreFailoverIT |
| FT-07 | P1 | submit、report、state Redis 分开部署 | 三个实例 | 所有 stream 和标记走对的实例 | 待补 |  |
| FT-08 | P1 | 自动重投和确认竞争 | 同一条消息有两份副本（重投领走的、工作线程批次里的），一份已确认，另一份才开始处理 | 后到的副本发现记录已不在，直接丢弃：只外发一次，只写一条 CDR，不留标记（DEF-C2，已修复，见第 10.1 节） | 已覆盖 | CoreDrBizIT |

### 十、性能基线

| ID | 级别 | 配置 / 前置 | 场景 | 预期 | 现状 | 现有用例 |
|---|---|---|---|---|---|---|
| PF-01 | P1 | 单 Core | 固定速率 MT 全链路 | 记录 p50、p99、CPU | 待补 |  |
| PF-02 | P1 | 多分片、多通道 | 逐步加压 | 记录上限和瓶颈 | 待补 |  |
| PF-03 | P2 | 积压 100 万条 | 恢复消费 | 消费速度，内存不增长 | 待补 |  |

## 5. 覆盖汇总

| 分组 | 用例数 | 已覆盖 | 部分 | 待补 |
|---|---|---|---|---|
| MT 主链路 | 7 | 7 | 0 | 0 |
| 前置检查 | 20 | 18 | 1 | 1 |
| 路由 | 17 | 12 | 0 | 5 |
| 外发与内容处理 | 17 | 15 | 1 | 1 |
| 长短信 | 7 | 6 | 1 | 0 |
| 结果与重试 | 15 | 14 | 1 | 0 |
| DR | 17 | 10 | 2 | 5 |
| 配置热加载 | 9 | 7 | 1 | 1 |
| 故障与恢复 | 8 | 6 | 1 | 1 |
| 性能基线 | 3 | 0 | 0 | 3 |
| **合计** | **120** | **95** | **8** | **17** |

| 级别 | 已覆盖 | 部分 | 待补 |
|---|---|---|---|
| P0 | 45 | 1 | 0 |
| P1 | 48 | 6 | 11 |
| P2 | 2 | 1 | 6 |

## 6. 建议补用例的顺序

P0 已经没有待补用例。接下来建议：

1. **其余路由（RT-08～RT-11）**：运营商、ENUM、第二路由。
2. **DR 映射和查询（DR-13～DR-17）**：其中状态映射、UNKNOWN 处理、欠费码的代码在 Client 协议层（`Smpp.java`），改放到 `docs/Client模块功能与测试用例清单.md` 的 VD-06～VD-08。
3. **剩余热加载（CF-07）和拒绝区域（OUT-11，需要 `phone.dat`）**。
4. **Redis 分开部署（FT-07）和性能基线（PF-01～PF-03）**：需要更多环境资源。

## 7. 未核实的部分

- 第 2 节“待补”的功能只确认了代码和配置存在，没有实测。
- 第 3.1 节只读了主路径，停用功能在其他模块是否还有入口，没有逐一排查。
- OTT 等其他业务（代码里有 `OTTSMSDispatcher`）、Peering 通道、`SystemManager` 相关逻辑没有列入。

## 8. 第一批结果

新增 `CoreCheckIT`（51 项）和 `CoreRouteIT`（21 项），已加入 `regression/suites.txt`，全部通过。

### 8.1 缺陷

第一批没有发现缺陷。曾登记的 DEF-C1（号码路由写通道名）已撤销：`A2PNumberRoutingRelay` 的 `Relay` 按约定写通道 ssid，写 ssid 的场景都正常（RT-07）。

### 8.2 用例确认的行为

- **新加坡规则（CK-04）：** `NonPA` 是默认类型，属于正常业务规则。代码逻辑如下：

  | 客户类型 | 黑名单 | 白名单 | 结果 |
  |---|---|---|---|
  | `VIP` | 不查 | 不查 | 继续检查内容 |
  | `NonPA` | 只查全局 | 只看客户自己的 `65` 或 `all` 条目 | 没有命中就拒绝，默认白名单不起作用 |
  | `PA` | 全局和客户 | 没有客户白名单时查默认白名单 | 按白名单结果 |

  接收号先去掉 `+`、`00`、`0` 再判断；只有开头是 `65` 才适用，号码中间出现 `65` 不受影响。
- **号码路由（RT-07）：** `Number` 用 `;` 分隔多个号码；同一号码取 `Priority` 最小的一条；`Relay` 支持 `a:50,b:50` 分流；ssid 对应的通道不存在时走前缀路由。
- **黑白名单按国家匹配（CK-01b）：** 列表每行是 `国家前缀:值`，同一个发送方发到其他国家不受影响。
- **白名单必须有默认文件：** `senderactiondir/-1_sender_whitelist.cfg` 不存在时，所有白名单都不加载，只打一行错误日志（读代码确认，没有单独用例）。行格式是 `国家前缀:wl或rp:发送方:替换值`，少于 4 段的行被忽略。
- **路由优先级（RT-01～RT-07）：** 号码 → 发送方 → 关键字 → 前缀（最长前缀优先）→ 备用。
- **默认路由的 key 是 `CustomerServiceType`（RT-12）：** 不是 `ServiceType`。文件名是 `{CustomerServiceType}_default_routingInfo.cfg`。
- **按比例黑洞（RT-16）：** 计时窗口内的第一条不进黑洞。黑洞消息的 DR 默认走延迟 DR，不是立即写入。
- **同号上限（CK-13）：** 计数 key 是 `Duplicate:{接收号}`，不区分客户，配了上限的多个客户共用同一个计数（读代码确认，没有单独用例）。

### 8.3 这批用例的限制

- 第 8.2 节标注“读代码确认”的项没有测试验证。
- `CK-08`、`CK-09`、`CK-10`、`CK-11`、`CK-12`、`CK-13b` 只判断“不外发并有失败 DR”，没有逐项校验状态码。
- 单进程运行，只验证 Core，消息由测试直接写入 stream，没有经过 Server。

## 9. 第二批结果

新增 `CoreRetryIT`（40 项），覆盖 RE-02～RE-08、RE-10、RE-12、RE-13，已加入 `regression/suites.txt`，全部通过。

### 9.1 测试方式

回归环境没有 MySQL，`RMQ`、`RDQ` 用下面的方式验证：

- **SQL：** 测试替换了消息存储的执行层，记录 Core 写出的 SQL，并按 SQL 里的 `RetriedNumber`、`NextRetryTime` 断言。
- **MQM 取数：** 由测试扮演 `getResendMessage`，把消息按 RMQ 读出后的状态交给真实的 `MQMMessageSender` 或 MQM stream。
- **真实组件：** MT 消费者、结果消费者、MQM 消费者和 DR 处理都是真实代码。

### 9.2 用例确认的行为

- **策略点数等于总发送次数（RE-04）：** `5S,1M,2M` 一共发送 3 次，也就是重试 2 次，间隔是 5 秒和 1 分钟，最后一个点不作为间隔使用。默认的 `10M*144` 一共发送 144 次。
- **策略优先级（RE-05）：** 通道 MT → 客户发送方 → 客户 MO → 全局默认。
- **有效期不够下次重试（RE-06）：** 写 RMQ 但不设 `NextRetryTime`，此时不回 DR，等 MQM 过期任务处理。
- **改道走备用路由（RE-12）：** 外发失败后 Core 用的是 `A2PBackupRoutingRelay`，不是 `A2PSec*RoutingRelay`；改道先于重试判断，不需要配置重试码。
- **无在线 Server 时（RE-08）：** MQM 重发的 DR 没有目标模块，会回到 `RDQ` 等下次重试，不会丢失。

**待确认：`SMSOptionOnlySupportDRRetry=true`（RE-13）。** `docs/配置项说明.md` 原来写“不做 MT 重试”，但实测 MT 失败码在重试列表时仍写 RMQ，只是改用 `DefaultRetryPolicy`（`RetryPolicyManager.getRetryPolicy` 走到 else 分支，并打印 `Unsupport message type`）。用例按当前行为断言，产品代码没有改。

### 9.3 这批用例的限制

- `getResendMessage`、`getExpiredMessage` 的 SELECT 没有在真实数据库上执行，RMQ 到期筛选和 MQM 过期任务没有验证（RE-06 记为部分）。
- 这批都用立即失败的结果驱动，没有验证真实等待到 `NextRetryTime`，也没有经过真实 Client 外发。
- `A2PSec*RoutingRelay` 第二路由（RT-11）和 DR 改道（RE-11）还没有用例。

## 10. 第三批结果

新增 `CoreFailoverIT`（14 项，两个真实 Core 进程，`kill -9`）和 `CoreDrBizIT`（38 项），已加入 `regression/suites.txt`。

### 10.1 发现并修复的缺陷

**DEF-C2（低，已修复）：自动重投和确认竞争时重复外发。**

- **原因：** 防重复靠标记 `core:mt:{msgId}`，确认时标记和 stream 记录一起删除。但消息可能同时有两份副本：工作线程批次里的一份，和重投线程领走的一份。一份完成确认后，另一份调用 `begin` 查不到标记，就当成新消息再处理一遍，重复外发并多写一条 CDR。两个方向都会发生。
- **触发条件：** 消息从读取到确认超过 `RedisStreamAutoClaimIdleMs`（默认 60 秒）。
- **修复：** `begin` 之后再查一次 stream 记录（`StreamAttempt.entryGoneAfterBegin`）。记录不在就删掉刚建的标记，丢弃这份副本，并打 INFO 日志。Redis 出错时不处理，消息留给下次重投。
- **性能：** 只查可能有另一份副本的情况，也就是重投领走的消息，以及读取超过空闲阈值还没处理的批次消息。正常消息不增加 Redis 调用。
- **范围：** Core 的 MT、外发结果、供应商 DR 三条路径。

**验证：**

| 场景 | MT 重复外发 |
|---|---|
| 修复前，3 秒阈值，真实 `kill -9` | 每轮 5～27 条（2 轮） |
| 只修重投路径，3 秒阈值 | 6 轮中有 2 轮出现（2 条、1 条），查出是反方向的竞争 |
| 完整修复，3 秒阈值 | **8 轮 0 条，拦下 40 次竞态** |

关掉修复再跑 `FT-08`，MT 和外发结果都会重复；供应商 DR 不重复，所以 `FT-08i`、`FT-08j` 只说明这条路径没有问题，不能证明修复对它有效。

**DEF-C3（中，已修复）：Client 外发有同样的竞争，供应商会真的收到两条。** `ClientSendMarker` 确认时同样会删除标记，另一份副本 `claim` 时查不到标记就返回 `SEND`。Core 把第二个结果当重复丢弃（`RESULT-DROPPED`），所以从 CDR 和 DR 上看不出来，只能在 Client 日志看到 `ack_returned_zero`。第二次发送还会留下一个“已发送”标记，最长 24 小时。

- **修复：** `claim` 得到 `SEND` 后、发送之前再查一次 stream 记录，记录不在就删除标记，不发送（`Claim.GONE`），也不写失败结果。查询条件和 Core 相同，正常消息不增加 Redis 调用。Redis 出错时不发送，消息留给下次重投。
- **范围：** `OutboundStreamConsumer`（SMPP）、`HttpClientStreamConsumer`（HTTP submit）。HTTP 推送 DR 不用标记，没有改。
- **验证：** 两个方向都有确定性用例（`ClientBatch1IT` 的 `C1-06a～f`、`ClientBatch2IT` 的 `H-C`）。修复前 SMPP 供应商收到 2 次，HTTP 的 `session.submit` 调用 2 次；修复后都是 1 次。这次修复只跑了 Client 相关的 7 个套件（90 项通过），没有跑全量回归。
- **限制：** HTTP 用例的通道没有配模板，统计的是 `session.submit` 调用次数，没有真实发出 HTTP 请求。Client 没有做多进程 `kill -9` 验证。

**没有改的路径：** MQM（`CoreMQMStreamConsumer`）、Server DR（`DRStreamConsumer`）也用自动重投，但没有用这类标记，竞争时可能重复处理，没有验证。

### 10.2 用例确认的行为

- **Core 不会自动接管（FT-06）：** 分片是静态配置。一个 Core 挂掉后，它的分片积压等待，另一个 Core 不受影响，消息不丢。恢复方式是重启，或改 `system:core:shards:{模块}` 后执行 `RELOAD_CORE_SHARDS@{模块}`。两种方式下 1500 条压力中 `kill -9`，都没有丢失和重复。
- **`Relay=a;b` 不是主备（RT-05）：** `;` 后面的部分在加载时被丢弃。
- **假 DR 的成功率默认是 0（DR-07d）：** 只开 `SMSOptionSendFakeDR`、不配 `SMSOptionDRSuccessRatio` 时，假 DR 全部是失败状态（10400）。
- **供应商拒绝时不回假成功（DR-07c）：** 假 DR 只在供应商接受后才回。
- **无在线 Server 时 DR 不丢（DR-06）：** 进 RDQ，恢复后 MQM 重发。

**待确认：DR 改道时间窗写死了东八区（RE-11）。** `drRerouteDelayMs` 从时间差里固定减 8 小时，而消息时间戳来自 `getGMTTime()`（当前时间减服务器时区偏移）。读代码推算：服务器是东八区时，时间窗正好等于 `RerouteDRDelayTimeInSec`；服务器是 UTC 时，窗口多 8 小时；东十一区时，默认 1800 秒的窗口算出来小于 0，永远不改道。用例按东八区构造时间戳，没有在其他时区实测。

### 10.3 这批用例的限制

- 多进程只覆盖 MT 消费。结果、供应商 DR、MQM 的消费者没有在多进程下 `kill -9`。
- 外发次数按写入出向 stream 统计，没有真实 Client 和供应商。
- 故障用例的 `CoreMtLeaseMs`、`RedisStreamAutoClaimIdleMs` 是 10 秒，不是默认的 60 秒。
- 假 DR 和延迟 DR 在 `AutoInDR.DirectStreamEnable=true` 下测试，默认值 `false` 的路径（写延迟队列或 RDQ）没有验证。
- `DR-06` 的“恢复后重发”由测试把消息写回 MQM stream，没有经过真实的 RDQ 查询。

## 11. 第四批结果

新增 `CoreContentIT`（35 项），已加入 `regression/suites.txt`，连跑 2 轮结果一致。这批只加测试，没有改产品代码，单独跑了这个套件，没有跑全量回归。

### 11.1 用例确认的行为

- **`ExpireTime` 的单位是小时（CK-18）：** 代码是 `getInt * 60` 转成分钟。`docs/配置项说明.md` 原来写“分钟”，已更正。
- **拆分要同时满足两个条件（OUT-03、OUT-04）：** 通道 `ChlMsgSplit=yes`，且客户 `SupportMsgSplit` 不是 `no`。否则超长消息整条外发。
- **拼接头会被去掉（OUT-06c）：** `05 00 03 …` 这种长短信拼接头由 Core 解析后清除，不作为 UDH 发出。其他 UDH（如端口寻址）原样保留。
- **模板失败（OUT-08）：** 内容里找不到验证码（默认正则 `(?:[0-9-]){4,}`）时不外发，回 10503；配了 `DrSuccForTemplateReplaceFailed` 时回 DELIVERED，同样不外发。
- **定时消息（CK-20）：** 定时时间未到时写 SMQ；时间按 GMT 比较。

### 11.2 这批用例的限制

- **单一进程、不经过 Server：** 消息由测试直接写入 stream，字段由测试设置。
- **`SMSOptionRemoveUdh`：** 在 Client 编码时生效，这里没有测。
- **`OUT-11`：** 回归环境没有 `conf/phone.dat`，查不到归属地，没有测。
- **`CK-19`：** 有效期解析在 Server 协议层，要放到 Server 用例里。
- **`OUT-09`：** `senderReplacement.cfg` 改完 reload 后对新消息生效，没有实测。
