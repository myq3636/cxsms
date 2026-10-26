# A2P 客户配置 reload 线上测试用例

2026-10-27

本清单用于在测试环境 / 线上验证客户配置重构（设计文档第 0 步 + 第 2 步）。共 83 条用例，按 P0 → P2 执行；P0 全部通过才能上线。「本地」列标 ✓ 的用例已在本机用验证程序覆盖过逻辑，线上仍需按业务表现复核一次。

相关文档：[A2P客户配置模块重构设计.md](A2P客户配置模块重构设计.md)

## 测试前准备

- **环境**：JDK 17；`jar/` 包含 `commons-pool2-2.12.0.jar`、`json-20231013.jar`；用当前分支构建的 a2p.jar。
- **备份**：备份线上 cm.cfg、routingInfo.cfg、phonePrefix.cfg，以及 `cmTempFile` 指向的 reload 记录文件。
- **测试客户**：准备 6 个专用测试客户，覆盖下表角色，ssid 记录在执行记录里。

  | 代号 | 角色（A2PPlayerType） | 协议 / 连接 | 用途 |
  | --- | --- | --- | --- |
  | T-OP | operator | SMPP 客户端，ConnectionType 1 | 大部分配置项、会话重建 |
  | T-CH | channel | HTTP（CommonHttp） | HTTP 接入、DR 查询线程 |
  | T-MN | operator | SMPP，ConnectionType MN | 多节点连接 |
  | T-SRV | channel | SMPP 服务端（ServerID / SPID） | 服务端认证 |
  | T-HUB | 3rdPartyHUB | SMPP | 角色变更、删除 |
  | T-NEW | channel | SMPP | 新增客户 |

- **触发 reload**：MGT telnet 的 `customer` 命令，或向 Redis 频道 `channel:gmms:control` 发布 `RELOAD_CUSTOMER`。
- **日志级别**：`A2PCustomerManager` 至少 INFO。
- **执行记录**：每条用例记录执行时间、模块、结果、日志截取。

## 关键日志

| 日志 | 含义 |
| --- | --- |
| `ConfBlock change, confFileVersion=…, md5Sum=…` | 检测到 cm.cfg 变化，开始 reload |
| `No need to load cm.cfg or md5Sum error` | 文件未变化，跳过 |
| `customer snapshot N published, changed customers: X, failed customers: [...]` | 新快照已发布；X 为变化客户数，失败列表应为空 |
| `load customer failed, shortName=…, keep its current config` | 该客户新配置无效，继续使用旧配置 |
| `index customer failed, shortName=…` | 该客户索引出错 |
| `customer reload already queued, event merged` | 重复的 reload 事件被合并 |
| `parse cm.cfg failed, keep the current customers` | 整个文件解析失败，未发布 |
| `customers not loaded at startup: [...]` | 启动时有客户未加载 |
| `customer snapshot changed during reload` | 有代码绕过执行线程写快照，**出现即为缺陷** |
| `break customer sessions error` / `init customer sessions error` | 会话断开 / 重建失败 |
| `update dynamic throttling rate error` / `init routing ALLOW policy error` | 限流 / ALLOW 更新失败 |
| `Dynamic update rate for SSID` / `Throttling disabled (removed limiter)` | 分布式限流已更新 / 已解除 |

以下日志在新版本中**不应再出现**：`ConcurrentModificationException`、`initCustomerInfo error,ignored!`、`clearCustomerInfo with null map`。

## A. 启动加载

| 编号 | 优先级 | 场景与操作 | 预期结果 | 本地 |
| --- | --- | --- | --- | --- |
| A-01 | P0 | 用当前线上 cm.cfg 启动各模块 | 启动成功；无 `customers not loaded at startup`；客户数量与 cm.cfg 一致 | |
| A-02 | P0 | 启动后抽查 T-OP / T-CH / T-SRV 收发 MT、DR | 与旧版本行为一致 | |
| A-03 | P1 | cm.cfg 中放一个 CCBSSID 非数字的客户后启动 | 该客户出现在 `customers not loaded at startup`；其他客户正常；`getConfFileVersion()` 返回真实 md5，不为 null | ✓ |
| A-04 | P1 | 启动后检查重试策略 | 有 `SMSOptionMORetryPolicy` 等配置的客户，重试行为与旧版一致 | ✓ |
| A-05 | P1 | 启动后检查当前 A2P | `getCurrentA2Ps()` 包含本机 A2P；MessageStoreManager 查询 SQL 中包含当前 A2P | ✓ |
| A-06 | P2 | cm.cfg 文件不存在或不可读时启动 | 启动报 `Failed to initCustomerConfigure`，行为与旧版一致 | |

## B. 增量 reload 基础

每条用例执行后检查：`customer snapshot … published` 中的 changed 数量与预期一致，failed 为空；`cmTempFile` 记录了新增 / 修改 / 删除的客户。

| 编号 | 优先级 | 场景与操作 | 预期结果 | 本地 |
| --- | --- | --- | --- | --- |
| B-01 | P0 | 不改 cm.cfg，触发 reload | `No need to load cm.cfg`；不发布新快照；无会话断开 | ✓ |
| B-02 | P0 | 只改 T-OP 的一个非连接配置项（如 DeliveryReportMode），触发 reload | changed = 1；仅 T-OP 生效；其他客户收发不受影响 | ✓ |
| B-03 | P0 | 新增 T-NEW，触发 reload | changed = 1；T-NEW 可收发；其他客户不受影响 | ✓ |
| B-04 | P0 | 删除 T-HUB，触发 reload | changed = 1；T-HUB 不可再收发；其 ssid 从所有查询中消失（见 C 组）；其他客户不受影响 | ✓ |
| B-05 | P0 | 同时修改 T-OP、新增 T-NEW、删除 T-HUB | changed = 3；三者各自生效 | ✓ |
| B-06 | P0 | 修改 T-OP 的 CCBSSID（如 101 → 111） | 新 ssid 生效；旧 ssid 查不到；重试策略、限流跟随新 ssid；连接初始化到新 ssid | ✓ |
| B-07 | P1 | 修改 T-HUB 的 A2PPlayerType（3rdPartyHUB → channel） | `isHub` 变为 false，`isChannel` 变为 true；透传模式随之变化 | ✓ |
| B-08 | P1 | 修改后再改回原配置，触发 reload | 结果与首次启动时完全一致 | ✓ |
| B-09 | P1 | 只修改 cm.cfg 的注释或空行（md5 改变、内容未变） | 发布新快照，changed = 0；无会话断开 | |
| B-10 | P1 | 连续修改 3 次，每次 reload | 每次的 generation 加 1；最终状态与最后一次配置一致 | ✓ |
| B-11 | P2 | 修改带 service 后缀的 shortName（如 `X_SMS`）客户 | 只影响该客户；`getSSidbyCustomerNameshort("X")` 结果正确 | |

## C. 各索引业务回归

对 T-OP 依次**设置 → reload → 验证 → 移除该配置项 → reload → 验证恢复**。每条用例都要同时验证「生效」和「移除后失效」。

| 编号 | 优先级 | 配置项 | 验证方式（业务表现 / 查询方法） | 本地 |
| --- | --- | --- | --- | --- |
| C-01 | P0 | `SPID` | HTTP / SMPP 服务端用新 SPID 认证成功，旧 SPID 失败（`getCustomerBySpID`） | ✓ |
| C-02 | P0 | `ServerID`、`ChlInit` | 服务端按 ServerID 识别客户（`getCustomerByServerID`） | ✓ |
| C-03 | P0 | `Protocol` | 对应 HTTP Servlet 能找到该客户（`getSsidByProtocol`）；删除客户后找不到 | ✓ |
| C-04 | P0 | `SMSOptionCoreProcessorThrottlingNum`、`PriorityPercent` | 优先级队列被 CorePrioritrySender 消费；删除客户或移除该项后不再消费，日志无 CME | ✓ |
| C-05 | P0 | `MCCMNC` | 按 MCCMNC 路由到该客户（`getSmsSsidByMncMcc`）；修改后旧值失效 | ✓ |
| C-06 | P0 | `ShortCode` | 短号识别（`isShortcode`），短号扩展路由 | ✓ |
| C-07 | P1 | `ChlIsSupportDeliveryReport: No` | 不向该通道下发 DR（`isRssidNotSupportDR`） | ✓ |
| C-08 | P1 | `DeliveryReportMode` | DR 模式生效（`getDRMode`） | ✓ |
| C-09 | P1 | `SmsOptionIsSupportUDH: No` | 长短信不带 UDH（`isNotSupportUDH`） | ✓ |
| C-10 | P1 | `SmsOptionIsSupport7Bit: yes` | 7bit 编码（`isSupport7Bit`） | ✓ |
| C-11 | P1 | `SmsOptionIsSupportExpiredDR: yes` | 过期消息下发 DR（`isSupportExpiredDR`） | ✓ |
| C-12 | P1 | `SupportMsgSplit`、`ChlMsgSplit` | 长短信拆分（`allowSplit`） | ✓ |
| C-13 | P1 | `SplitSuffix`、`MsgSplitLen`、`SMSOptionAsciiMsgSplitLen` | 拆分长度与后缀（`getRopSupportLength`、`supportSplitSuffix`） | ✓ |
| C-14 | P1 | `RequiredCharsets` | 对指定 relay 的字符集要求生效（`getRequiredCharsets`）；被引用的 relay 客户删除后失效 | ✓ |
| C-15 | P1 | `ContentScan` | 内容扫描字符集转换生效（ContentScanMilter）；修改后立即生效，无需重启 | ✓ |
| C-16 | P1 | `LocalDNSPrefix` | 本地 DNS 前缀（`getCustomerLocalDNSPrefix`） | ✓ |
| C-17 | P1 | `PhonePrefix` / `PhoneSubPrefix` | 号码前缀校验（`checkRecPrefixAndLen`） | ✓ |
| C-18 | P1 | `SMSOptionRecUserPhoneLenList` / `SMSOptionRecNumberLenList` | 收件人号码长度校验（`checkRecNumberLen`）；未配置时默认 6–15 位 | ✓ |
| C-19 | P1 | `CustomerID`、`IOSMSSSID` | `getSsidByCustID`、`getSsidByIosmsSsid` 结果正确；修改后旧值查不到 | ✓ |
| C-20 | P1 | `A2PPlayerType`（各角色） | `isA2P` / `isOperator` / `isVirtualOperator` / `isChannel` / `isHub` / `isPartition`；`getTransparency` | ✓ |
| C-21 | P1 | `SMSOptionMORetryPolicy` 等重试策略 | 消息重试按新策略执行；reload 期间重试不中断 | ✓ |
| C-22 | P1 | `SMSOptionOutgoingThrottlingNum` | 分布式限流按新值生效（日志 `Dynamic update rate for SSID`）；改为 0 后解除 | ✓ |
| C-23 | P2 | 名称以 `VFKK2G_JP` / `Jphone_JP` 开头的客户 | `getVFKKssid` | ✓ |
| C-24 | P2 | `NoticePrefix` | 通知前缀（`getNoticeInfo`） | ✓ |
| C-25 | P2 | `SMSOptinIsSupportHttpQueryMessage` | 启动时创建 DR 查询线程；reload 后由 `createCustomerSessions` 重载 | |
| C-26 | P2 | `ChlURL` 包含本机 ServiceIP 的 A2P 客户 | `getCurrentA2P`、`getPeeringTcpVersion` | ✓ |

## D. 失败与容错

| 编号 | 优先级 | 场景与操作 | 预期结果 | 本地 |
| --- | --- | --- | --- | --- |
| D-01 | P0 | 把 T-OP 的 CCBSSID 改成非数字，同时修改 T-CH，reload | T-OP 出现在 failed 列表，继续按旧配置收发；T-CH 新配置生效 | ✓ |
| D-02 | P0 | 修复 D-01 的配置，不改其他内容，reload | 即使其他内容未变也会重新 reload；T-OP 新配置生效；failed 为空 | ✓ |
| D-03 | P0 | 让 T-NEW 与 T-OP 使用相同 CCBSSID | 后加入的一方失败并保留旧配置（新增的直接不加载）；T-OP 不受影响 | ✓ |
| D-04 | P0 | cm.cfg 写成无法解析的内容（或文件权限不可读），reload | `parse cm.cfg failed` 或无变化；所有客户继续正常收发 | |
| D-05 | P1 | 配置一个格式错误的多值项（如 `RequiredCharsets` 缺 `Charset`），reload | 该客户出现 `index customer failed`；其他客户正常 | |
| D-06 | P1 | reload 时某个客户的会话断开失败（如连接卡死） | 日志 `break customer sessions error`；该客户新配置仍生效；其他客户继续处理 | |
| D-07 | P1 | reload 时 Redis 不可用 | 限流更新报错但不中断；客户配置正常生效 | |
| D-08 | P2 | reload 期间 cm.cfg 被再次修改 | 本次按读取时的内容发布；下一次 reload 拾取新内容 | |

## E. 并发与事件

| 编号 | 优先级 | 场景与操作 | 预期结果 | 本地 |
| --- | --- | --- | --- | --- |
| E-01 | P0 | 1 秒内连续发 2 条 RELOAD_CUSTOMER（复现 10-22 事故） | 无 CME；无 `initCustomerInfo error`；只发布一次或依次发布；所有客户正常 | ✓ |
| E-02 | P0 | 压测流量下（含 CorePriority 客户）每分钟 reload 一次，持续 30 分钟 | 无 CME、无 NPE、无 `getCustomerBySSID` 返回 null 导致的失败；成功率与无 reload 时一致 | ✓ |
| E-03 | P0 | 同时发 RELOAD_CUSTOMER 和 RELOAD 路由（routingInfo） | 两者依次执行，日志线程名均为 `CustomerReload`；路由 reload 正常返回结果 | |
| E-04 | P1 | 并发发 10 条 RELOAD_CUSTOMER | 出现 `event merged`；最多发布 2 次；结果与最后一份 cm.cfg 一致 | ✓ |
| E-05 | P1 | 不同节点（多实例）同时收到 Redis 广播 reload | 每个节点各自只 reload 一次，最终配置一致 | |
| E-06 | P1 | reload 执行期间发送 telnet `customer` 命令 | 命令立即返回，不阻塞 telnet 会话 | |
| E-07 | P2 | 模块关闭（shutdown）时正在 reload | 进程能正常退出，不挂住 | |

## F. 连接与会话（只能在线上 / 测试环境验证）

每个模块类型执行一遍：Router（Core）、MGT、MultiSmppClient、MultiSmppServer、SSLSmppServer / SSLSmppClient、Peering2 Client / Server、CommonHttpClient。

| 编号 | 优先级 | 场景与操作 | 预期结果 | 本地 |
| --- | --- | --- | --- | --- |
| F-01 | P0 | 修改 T-OP（SMPP 客户端，ConnectionType 1）的 ChlURL，reload | 旧连接断开、新连接建立；`getCustomerBySSID` 在 reload 后返回连接工厂转换的对象（SingleNodeCustomerInfo）；收发正常 | |
| F-02 | P0 | 只修改 T-OP 的非连接配置项（如 DeliveryReportMode），reload | 会话会被重建（当前行为与旧版一致），重建后收发正常；记录断开时长 | |
| F-03 | P0 | 删除 T-OP，reload | 连接断开且不再重连；连接工厂覆盖层被清除，`getCustomerBySSID` 返回 null | |
| F-04 | P0 | 新增 T-NEW（SMPP 客户端，ChlInit true），reload | 自动建立连接并可收发 | |
| F-05 | P0 | 修改 T-SRV（SMPP 服务端）的 SPID / 密码，reload | 旧账号无法再绑定，新账号可以绑定 | |
| F-06 | P0 | 修改 T-MN（多节点）的节点配置，reload | 按新节点建立连接；serverInfoMap 无残留（`logServerInfoMapSummary("reload")`） | |
| F-07 | P1 | 修改 T-CH（CommonHttp）的查询配置，reload | DR 查询线程按新配置重启 | |
| F-08 | P1 | Router 模块修改 T-OP，reload | ConnectionManagementForCore 先清理旧 ssid、再初始化新 ssid；路由到 T-OP 的消息正常 | |
| F-09 | P1 | 修改 Peering2 客户，reload | Peering 连接按新配置重建 | |
| F-10 | P1 | B-06（修改 SSID）在 SMPP 客户端模块执行 | 旧 ssid 的连接与覆盖层被清除；新 ssid 建立连接 | |
| F-11 | P2 | 会话断开过程中有在途消息 | 在途消息按重试策略处理，不丢失 | |

## G. 快照之外的副作用

| 编号 | 优先级 | 场景与操作 | 预期结果 | 本地 |
| --- | --- | --- | --- | --- |
| G-01 | P0 | 删除客户 | 其重试策略被移除；分布式限流不再作用于该客户 | ✓ |
| G-02 | P0 | routingInfo 中 T-OP 有 ALLOW 配置，修改 T-OP 后 reload | ALLOW 策略仍然生效（`isPolicyControlPermit`） | ✓ |
| G-03 | P1 | 修改客户后检查去重 | 去重缓存被清空（与旧版一致），重复消息检测重新开始 | |
| G-04 | P1 | 修改客户后检查本地限流（ThrottlingControl） | 本地限流缓存被清空并按新值生效 | |
| G-05 | P1 | 未变化的客户 | 限流状态（`applyInThrottleFlag`）、收件人缓存保持不变（客户对象复用） | ✓ |
| G-06 | P2 | 检查 `cmTempFile` | 记录本次删除的客户和新增 / 修改客户的完整配置 | |

## H. 其他 reload 不回归

这些 reload 的逻辑本次未修改，只是改为在同一个执行线程上排队执行。

| 编号 | 优先级 | 场景与操作 | 预期结果 | 本地 |
| --- | --- | --- | --- | --- |
| H-01 | P0 | routingInfo reload（`-a` / `-r`） | 正常返回结果；路由生效；与客户 reload 不互相阻塞超过一次 reload 的耗时 | |
| H-02 | P1 | phonePrefix reload | 号码前缀与发送方替换生效 | |
| H-03 | P1 | 发送方黑白名单、收件人黑名单、内容黑白名单 reload | 名单生效 | |
| H-04 | P1 | recipientRule、vendorTemplate、systemVendorReplace reload | 生效 | |
| H-05 | P2 | 客户 reload 后再做 routingInfo reload | 路由能引用新增客户 | |

## I. 长稳与回滚

| 编号 | 优先级 | 场景与操作 | 预期结果 | 本地 |
| --- | --- | --- | --- | --- |
| I-01 | P0 | 生产规模 cm.cfg（全部客户）reload 一次，记录耗时 | 从 `ConfBlock change` 到 `published` 的耗时；建议 < 1 秒（不含会话重建） | |
| I-02 | P1 | 每 5 分钟 reload 一次，运行 24 小时 | 内存平稳，无线程数增长，无 `CustomerReload` 线程堆积 | |
| I-03 | P1 | 回滚演练：换回旧版 a2p.jar 并重启 | 旧版本用相同 cm.cfg 正常启动 | |

## 本地已验证范围

本机验证程序（未放入仓库）覆盖了上表标 ✓ 的逻辑：46 个快照字段全部有数据；修改 / 新增 / 删除 / 修改 SSID / 修改角色 / 恢复后，增量 reload 与全新构建逐字段一致；配置错误与 SSID 冲突保留旧配置；300 次 reload 期间 4 个读线程无 null、无 CME；20 个并发事件只发布一次；重试策略、ALLOW、`updateRate` 按预期更新。连接、会话与外部依赖（Redis、数据库、连接工厂）未在本机验证。
