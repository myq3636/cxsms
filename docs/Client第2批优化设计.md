# Client 第 2 批优化设计：HTTP 通道限速、按通道配置超时

## 1. 问题

问题清单见《Client模块架构分析与改进》的 S2、T3。按决定，本批只处理 HTTP 通道；会话断开时立即判失败（S1）暂不改。

| # | 现象 | 原因 |
|---|---|---|
| T3 | HTTP 供应商没有限速，worker 有多快就发多快 | `HttpClientStreamConsumer` 没有调用 `OutgoingThrottlingNum`，只有 SMPP 外发用到了它 |
| S2 | 一个慢的 HTTP 供应商，每条请求最长占用 worker 90 秒 | 超时只有模块级的 `ConnectTimeout`（30 秒）和 `ReadTimeout`（60 秒），所有 HTTP 通道共用；而且是在会话创建时读取的，不能按通道调整 |

## 2. 方案

### 2.1 HTTP 外发限速

做法与 Client 第 1 批的 SMPP 外发完全相同：

- 外发到 HTTP 供应商（`stream:http:submit:*`）前，用 `tryAcquire` 取通道的令牌。`OutgoingThrottlingNum`（cm.cfg 中的 `SMSOptionOutgoingThrottlingNum`）按**单个 Client 节点**生效，使用本地 Guava 限流器。
- 拿不到令牌时，本批剩下的消息延后约一个令牌的间隔再处理，worker 立即释放。延后的消息用 `XCLAIM ... JUSTID` 保持在自己名下，不增加投递次数。延后期间，这个 stream 不启动其他任务。
- 限速通道每次最多读取 TPS 条。
- 自动重投领到的消息拿不到令牌时，也放进同一个延后列表。
- 任务处理 `ClientStreamTaskMaxBatches` 批或 `ClientStreamTaskMaxMs` 毫秒后让出 stream，与 SMPP 外发相同。
- 推送给 HTTP 客户的 DR（`stream:http:dr:*`）不限速。

延后逻辑从 `OutboundStreamConsumer` 中抽成公共类 `StreamDeferrals`，SMPP 和 HTTP 外发共用；SMPP 外发的行为不变。

### 2.2 HTTP 超时按通道配置

在 cm.cfg 的通道块中新增两个配置：

| 配置项 | 单位 | 默认值 | 说明 |
|---|---|---|---|
| `SMSOptionHttpConnectTimeout` | 秒 | 0 | 建立连接的超时。0 或不配置时使用模块的 `ConnectTimeout`（默认 30） |
| `SMSOptionHttpReadTimeout` | 秒 | 0 | 等待响应的超时。0 或不配置时使用模块的 `ReadTimeout`（默认 60） |

- 每次请求时读取通道配置，热加载 cm.cfg 后的下一个请求就会使用新值，不需要重建会话。
- 模块默认值保持不变（30 秒、60 秒），只有配置了的通道才生效。
- 生效范围：`HttpSession` 的 GET、POST 和带 header 的请求，包括 `CommonHttpSession`、`SecCommonHttpSession`、`CommonRESTHttpSession`。`WebServiceSession` 使用 JVM 全局的超时设置，不在本批范围。

## 3. 不在本批范围

- 会话断开时立即判失败（S1）。
- 改用异步 HTTP 客户端。JDK 的 `HttpURLConnection` 本身会复用 keep-alive 连接，每个主机默认缓存 5 个空闲连接。
