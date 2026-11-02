# SMPP Server 功能测试清单

> 结果见《SMPP-Server功能测试报告》。本清单只验证现有代码，没有修改产品代码。预期按 SMPP 3.4 协议和《配置项说明》判定。

清单共 85 条用例（136 项检查）。级别：P0 影响收发或安全，P1 影响部分客户或配置项，P2 协议细节。

### 一、Bind 与鉴权

| ID | 级别 | 功能 / 参数 | 场景 | 预期 | 结果 |
|---|---|---|---|---|---|
| B-01 | P0 | `IncomingConnection` | 正确账号 bind_transceiver | 成功；回 system_id、sc_interface_version | 通过 |
| B-02 | P0 | `password` | 密码错误 | `ESME_RINVPASWD`，然后断开 | 通过 |
| B-03 | P0 | `userName` | system_id 不存在 | `ESME_RINVSYSID` | 通过 |
| B-04 | P1 | `password` | 空密码 | `ESME_RINVPASWD` | 通过 |
| B-04b | P1 | — | 密码超过 SMPP 长度上限 | 回错误 bind_resp | **失败** |
| B-04c | P1 | — | system_id 超过 SMPP 长度上限 | 回错误 bind_resp | **失败** |
| B-05 | P0 | — | 连续 1000 次错误密码 | 每次都先收到 bind_resp 再断开 | **失败** |
| B-06 | P1 | `URL` | 账号只有一个连接，来源 IP 不符 | 拒绝（需确认设计意图） | **失败** |
| B-06b | P1 | `URL` | 账号有多个连接，都不匹配来源 IP | 拒绝 | 通过 |
| B-06c | P1 | `URL` | 账号有多个连接，一个匹配来源 IP | 成功 | 通过 |
| B-07 | P1 | `Version` | V33/V34 与 bind 版本匹配或不匹配 | 匹配成功；不匹配 `ESME_RBINDFAIL` | 通过 |
| B-08 | P1 | `BindMode=R` | 三种 bind | 只接受 bind_transmitter | 通过 |
| B-09a | P1 | `BindMode=T` | bind_receiver | 成功 | 通过 |
| B-09b | P1 | `BindMode=T` | bind_transmitter | 拒绝（需确认设计意图） | **失败** |
| B-09c | P1 | `BindMode=T` | bind_transceiver | 拒绝 | 通过 |
| B-10 | P1 | `BindMode=TRX` | bind_receiver、bind_transmitter | 拒绝 | 通过 |
| B-11 | P0 | `SessionNumber=2` | 第 3 个 bind，再释放一个 | 第 3 个拒绝；释放后可再 bind | 通过 |
| B-12 | P1 | `SmppServerModules` | 客户不属于本模块 | 拒绝 | **失败** |
| B-13 | P2 | — | 已 bind 后再次 bind | `ESME_RALYBND` | **失败** |
| B-14 | P0 | `SmppServerBindTimeoutMs` | 连上不 bind | 超时断开 | 通过 |
| B-15 | P0 | `SmppServerMaxConnections`、`SmppServerMaxConnectionsPerIp` | 超过总数、单 IP 上限；再放开 | 新连接断开；热加载放开后恢复 | 通过 |

### 二、会话保活

| ID | 级别 | 功能 / 参数 | 场景 | 预期 | 结果 |
|---|---|---|---|---|---|
| K-01 | P0 | — | 客户发 enquire_link | 回 resp，sequence 一致 | 通过 |
| K-02 | P0 | — | unbind（含连续 100 次） | 回 unbind_resp 后断开 | 通过 |
| K-03 | P1 | `ConnectionSilentTime` | 有流量、无流量 | 有流量不断开；静默超时断开 | 通过 |
| K-04 | P1 | `ServerEnquireLink`、`EnquireLinkTime`、`FailEnquireLinkTime`、`EnquireLinkFailureNum` | 客户响应、不响应 | 响应时保持；不响应时累计失败后断开 | 通过 |
| K-05 | P0 | 会话登记 | 客户直接断开，不发 unbind | 登记被删除 | 通过 |
| K-06 | P2 | `SmppServerSessionKeeperShared=false` | 每连接保活线程模式 | 会话正常 | 通过 |

### 三、Submit 提交

| ID | 级别 | 功能 / 参数 | 场景 | 预期 | 结果 |
|---|---|---|---|---|---|
| S-01 | P0 | — | 普通 submit | 成功；stream 里 ssid、发送方、接收方、内容、模块正确 | 通过 |
| S-02 | P0 | — | 200 条 | message_id 全部不同 | 通过 |
| S-03 | P0 | `data_coding=8` | UCS2 | 正确解码 | 通过 |
| S-04 | P1 | `data_coding=3` | Latin-1 | 正确解码 | 通过 |
| S-05 | P1 | `data_coding=4` | 二进制 | 接受 | 通过 |
| S-06 | P1 | `message_payload` | sm_length=0，用 TLV 带内容 | 内容保留 | 通过 |
| S-07 | P0 | `esm_class=0x40` | UDH 长短信 | UDH 和正文分开保存 | 通过 |
| S-08 | P0 | SAR TLV | `sar_*` 三个参数 | 分段信息正确 | 通过 |
| S-09 | P0 | `registered_delivery` | 0 和 1 | 是否要 DR 正确保留 | 通过 |
| S-10 | P1 | — | 254 字节 | 完整接受 | 通过 |
| S-11 | P2 | — | 空内容 | 有响应 | 通过 |
| S-12 | P2 | — | 空接收号 | 按协议回 `ESME_RINVDSTADR`（需确认） | **失败** |
| S-13 | P2 | — | 非数字接收号 | 按协议回 `ESME_RINVDSTADR`（需确认） | **失败** |
| S-14 | P2 | — | 空发送方 | 有响应 | 通过 |
| S-15 | P0 | — | 逐字节发送 | 正常解析 | 通过 |
| S-16 | P0 | — | 一个包 3 条 PDU | 3 条响应 | 通过 |
| S-17 | P1 | — | 对端发 deliver_sm | 回 deliver_sm_resp | 通过 |
| S-18 | P1 | `SMSOptionSMPPIsGenHexMsgId`、`SMSOptionSupportMaxInMsgID`、SMPP 3.3 | 三种 ID 生成方式 | ID 格式正确 | 通过 |

### 四、限流、计费与队列

| ID | 级别 | 功能 / 参数 | 场景 | 预期 | 结果 |
|---|---|---|---|---|---|
| T-01 | P0 | `SMSOptionIncomingThrottlingNum` | 限 5，突发 60 | 超限回 `ESME_RTHROTTLED` | 通过 |
| T-02 | P0 | Redis key `thcon` | 删除、恢复 | 全部限流，enquire_link 正常；恢复后正常 | 通过 |
| T-03 | P0 | `SMSOptionChargeByGateWay` | 余额 2，发 3 条；无余额 key | 第 3 条和无 key 时回 `ESME_RPROVNOTALLWD` | 通过 |
| T-04 | P0 | `SMSOptionChargeCountryByGateWay` | CN 余额 1；非法接收号 | 超额拒绝；非法号也有响应 | 通过 |
| T-05 | P0 | Redis 故障 | `CLIENT PAUSE` 6 秒 | submit 有错误响应；恢复后同一连接可用 | 通过 |
| T-06 | P0 | `SmppServerInboundPerSsidQueueSize` | 队列 20，突发 2000 | 全部有响应，超出部分被拒 | 通过 |
| T-07 | P1 | `SmppServerInboundQueueFullCommandStatus` | 配成数字 20 | 状态码生效 | 通过 |

### 五、DR 下发

| ID | 级别 | 功能 / 参数 | 场景 | 预期 | 结果 |
|---|---|---|---|---|---|
| D-01 | P0 | `DrSwapAddr`（默认） | 普通 DR | esm_class=4；含 id 和 stat；地址交换；无 receipted_message_id | 通过 |
| D-02 | P0 | 状态映射 | DELIVERED、EXPIRED、UNDELIVERED、DELETED | stat 正确（DELETED 按代码映射为 UNDELIV） | 通过 |
| D-03 | P1 | `SMSOptionSMPPNeedReceiptedMessageId` | 打开 | 带 receipted_message_id | 通过 |
| D-04 | P1 | `DrSwapAddr=false` | 关闭 | 地址不交换 | 通过 |
| D-05 | P0 | 多连接 | 3 条连接 12 条 DR | 每条只送一次 | 通过 |
| D-06 | P0 | `WindowSize=2` | 不回 resp，然后回 | 窗口满不再发；回 resp 后全部送达，不重复 | 通过 |
| D-07 | P1 | `SMPPDRPendingMode=redis` | redis 模式 | 送达，收到 resp 后无残留 | 通过 |
| D-08 | P0 | `BufferTimeout` | 不回 deliver_sm_resp | 超时并记录 | 通过 |
| D-09 | P0 | — | 客户不在线 | 记录 session not found；stream 记录已确认 | 通过 |
| D-10 | P0 | bind 方式 | 接收方式、发送方式 | 接收方式收到；发送方式连接上收不到 | 通过 |

### 六、协议健壮性

| ID | 级别 | 功能 / 参数 | 场景 | 预期 | 结果 |
|---|---|---|---|---|---|
| P-01 | P0 | — | 未 bind 时 submit | `ESME_RINVBNDSTS`，断开 | 通过 |
| P-02 | P1 | — | 未 bind 时 enquire_link | `ESME_RINVBNDSTS` | 通过 |
| P-03 | P1 | — | 未知命令 | generic_nack `ESME_RINVCMDID` | **失败** |
| P-04 | P1 | — | submit 体被截断 | 回错误 | **失败** |
| P-05 | P1 | — | 300 个 0 字节的 submit | 回错误 | **失败** |
| P-06 | P2 | — | query_sm、cancel_sm、replace_sm、data_sm | `ESME_RINVCMDID` | **失败** |
| P-07 | P1 | — | 意外的响应、generic_nack | 忽略，会话继续可用 | 通过 |
| P-08 | P1 | — | `command_length` 为 8、0、15、-1 | generic_nack `ESME_RINVCMDLEN` 或断开 | **失败** |
| P-09 | P0 | — | 声明 2GB 后发 50MB | 堆内存不增长 | 通过 |
| P-10 | P1 | — | 接收方式连接发 submit | `ESME_RINVBNDSTS` | **失败** |

### 七、安全与日志

| ID | 级别 | 功能 / 参数 | 场景 | 预期 | 结果 |
|---|---|---|---|---|---|
| SEC-01 | P0 | `SmppPduLogEnable` | bind 后查日志 | 日志没有密码 | **失败** |
| SEC-02 | P1 | — | 300 次错误密码 | 有限制 | **失败** |
| SEC-03 | P1 | — | 多次错误后用正确密码 | 能登录 | 通过 |
| SEC-04 | P2 | `SmppPduLogEnable=false` | 关闭 | 不打 PDU 日志 | 通过 |

### 八、热加载

| ID | 级别 | 功能 / 参数 | 场景 | 预期 | 结果 |
|---|---|---|---|---|---|
| R-01 | P0 | `RELOAD_CUSTOMER` | 改密码 | 旧密码拒绝、新密码可用；其他客户不受影响 | 通过 |

### 九、SMPP over TLS（`SslSmppServer`，`Ssl.*`）

| ID | 级别 | 功能 / 参数 | 场景 | 预期 | 结果 |
|---|---|---|---|---|---|
| TLS-00 | P1 | `Ssl.KeyStorePath` | 启动 | 成功 | 通过 |
| TLS-01 | P1 | `Ssl.SSLProtocol` | 握手 | 成功 | 通过 |
| TLS-02 | P1 | `Protocol=SSLSMPP` | bind | 成功 | 通过 |
| TLS-03 | P1 | — | submit | 成功 | 通过 |
| TLS-04 | P1 | 会话登记 | bind 后 | 已登记 | 通过 |
| TLS-05 | P0 | — | 给 TLS 客户发 DR | 收到 deliver_sm | **失败** |
| TLS-06 | P1 | — | 错误密码 | `ESME_RINVPASWD` | 通过 |
| TLS-07 | P1 | — | 明文连 TLS 端口 | 无法 bind | 通过 |

### 九-B、多个 Server（`MultiServerIT`）

两个真实的 `MultiSmppServer` 进程（各自模块名、节点、端口），共用一个 Redis。测试进程扮演 Core，用产品代码解析和写入 DR。

| ID | 级别 | 功能 / 参数 | 场景 | 预期 |
|---|---|---|---|---|
| MS-01 | P0 | `SmppServerModules` | 只配 Server 1、只配 Server 2、配两个、不配 | 只能 bind 在配置的 Server 上；不配时两个都可以 |
| MS-02 | P1 | 会话登记 | 两个客户各连一个 Server | 登记在各自的 Server 下 |
| MS-03 | P0 | submit | 分别从两个 Server 提交 | stream 记录的是实际收到消息的 Server |
| MS-04 | P0 | DR | 两个客户各连一个 Server | DR 从各自的 Server 发出 |
| MS-05 | P0 | DR | 客户同时连两个 Server | DR 走消息原来的 Server |
| MS-06 | P0 | DR | 原 Server 上已没有连接，另一个有 | DR 跟到另一个 Server |
| MS-07 | P0 | DR | 客户从 Server 1 重连到 Server 2 | DR 跟到 Server 2 |
| MS-10 | P0 | `SmppServerModules` + `RELOAD_CUSTOMER` | 客户从 Server 1 切到 Server 2 | 新 Server 可 bind，旧 Server 拒绝且断开会话；其他客户不受影响；DR 和 submit 走新 Server；Core 不再查到旧 Server |
| MS-11 | P0 | 同上 | 切回 Server 1 | 恢复 |
| MS-12 | P1 | 同上 | 从一个 Server 扩到两个，再收回 | 扩容时会话重建后两边都可 bind；收回后只能 bind 在原 Server |
| MS-13 | P0 | `RELOAD_CUSTOMER` | reload 新增只属于 Server 2 的客户，再删除 | 只能 bind 在 Server 2；删除后拒绝并断开 |
| MS-14 | P1 | `RELOAD_CUSTOMER` | 改密码 | 旧密码拒绝，新密码可用 |
| MS-20 | P0 | 故障 | kill -9 Server 1 | 它的客户断开；Server 2 上的客户不受影响 |
| MS-21 | P0 | `SmppServerDrLivenessCheck` | Server 1 已死 | 只配 Server 1 的客户不能转到 Server 2；不限制的客户可以重连到 Server 2，心跳过期后 DR 跟过去 |
| MS-22 | P0 | 故障恢复 | 客户只配在已死的 Server 上；然后重启 | 死的时候 Core 拿不到 DR stream（走 DR 重试策略）；重启后恢复，客户重连，新 DR 送达 |

## 十、已有自动化套件覆盖的功能

这些功能已有用例，本次重新执行，不再重复设计。

| 套件 | 覆盖的功能和参数 | 检查数 | 结果 |
|---|---|---|---|
| `ServerBatch1IT` | 共享收包池（`SmppServerInboundWorkerMax`、`MaxInFlightPerSsid`）、线程不随连接增长、enquire_link 优先、`SmppServerMaxConnections*`、DR 窗口满不重复、会话失败清理 | 16 | 通过 |
| `ServerBatch2IT` | 日志链路、`ServerDrAckOnResp`、bind 超时、静默断开、`ServerEnquireLink`、`BufferTimeout`、resp 前后到达都确认 | 21 | 通过 |
| `ServerBatch3IT` | DR 结果与确认合并、`EnquireLinkTime` 间隔、pending 管理器不重复创建 | 10 | 通过 |
| `RegistryBatchIT` | 会话登记批量写入、PDU 兜底、对账加固、`SmppServerSessionRegistry*` | 32 | 通过 |
| `DrLivenessIT` | `SmppServerDrLivenessCheck`、Server 宕机后 DR 路由 | 9 | 通过 |
| `ShardGroupIT` | 分片校验 | 8 | 通过 |
| `LogChainClientIT` | 日志链路 | 11 | 通过 |
| `RenameReloadIT`（Server 模块） | cm.cfg 改 shortName、改 ssid，只影响改动的客户 | 30 | 通过 |
| `RapidReloadIT`（Server 模块） | 连续 reload | 6 | 通过 |

## 十一、未执行的功能

| 功能 / 参数 | 未执行原因 | 建议 |
|---|---|---|
| 2 万条连接 | 本机线程上限约 7500 | 测试环境验证 |
| 多机部署；kill 时已发出未确认的 DR 重投（X-S21）；`ServerDrStreamShardFilter=true`（多节点共用模块名） | `MultiServerIT` 在一台机器上用不同模块名，没有覆盖 | 测试环境验证 |
| Core 把 DR 交给重试策略之后的完整链路（MS-22a 只验证到拿不到 stream） | 属于 Core 链路 | 走 Core 用例确认 |
| `ConnectionType=MN`（多节点客户）、单连接客户 | 本次只配了 `MC` 客户 | 需要时补充 |
| `SmppServerSendQueueMax`、`SmppServerSendQueueHighWater`（客户不读 socket） | 本机难以触发，已有报告列为 X-S22 | 测试环境用真实慢客户验证 |
| `MultiSmppServer.LongConnectionCustomer*` | 默认关闭，本次没有测 | 启用前补测 |
| `SMSOptionSupportIncomingGSM7bit`、`SMSOptionParseValidityPeriod`、`SMPPDRPendingRedis*`、`SmppPendingDr*` | 本次没有覆盖 | 需要时补充 |
| 非法接收号到了 Core 之后的处理 | 这是 Core 链路，本次只测 Server 的响应 | 走 Core 用例确认 |
| `Ssl.WantClientAuth`、`Ssl.NeedClientAuth`、客户端证书 | 本次只测单向 TLS | 使用前补测 |
