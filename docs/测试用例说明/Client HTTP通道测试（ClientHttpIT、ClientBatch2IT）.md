# Client HTTP 通道测试（ClientHttpIT、ClientBatch2IT）

> 日期：2026-11-27。用例代码：`regression/test/it/ClientHttpIT.java`（68 项）、`ClientBatch2IT.java`（18 项），都在 `regression/suites.txt`，it 模式。
> SMPP 通道在《Client SMPP连接与PDU测试》《Client发送速率与发送过程测试》。

## 1. 测什么

HTTP 通道的 MT 走 `stream:http:submit:{通道}:*`，由 `HttpClientStreamConsumer` 读出、发真实的 HTTP 请求；给 HTTP 客户推送的 DR 走 `stream:http:dr:{客户}:*`。

| 部分 | 套件 | 内容 |
|---|---|---|
| 发送和应答 | ClientHttpIT | 真实请求带上号码、发送方；应答按接口文件解析出状态和供应商消息 ID；应答的码、HTTP 状态码、连接失败、读超时映射成内部状态 |
| 没有自定义处理类 | ClientHttpIT | 用默认处理类发 Common XML |
| 推 DR 给客户 | ClientHttpIT | 状态按接口文件映射；客户服务器的 HTTP 状态码分成最终失败和临时失败（重试） |
| DR 重发 | ClientHttpIT | 推不出去的 DR 由 Core 留在 RDQ，重发时带着真实状态 |
| 只发一次 | ClientHttpIT、ClientBatch2IT | auto-claim 和工作线程同时拿到同一条；Redis 写不进发送标记时不发；结果写不进去时重试写，不重发请求 |
| 超时 | ClientBatch2IT | 每个通道自己的读超时、连接超时，重新加载后生效 |
| 限速和隔离 | ClientBatch2IT | HTTP 通道的 `SMSOptionOutgoingThrottlingNum`；限速通道积压时不拖累别的通道 |

## 2. 怎么跑、怎么判定

```
regression/run.sh ClientHttpIT
regression/run.sh ClientBatch2IT
cat regression/.work/results/{ClientHttpIT,ClientBatch2IT}.txt
```

- 环境：测试服务器上的回归环境（JDK 17，本地 Redis 16379）。
- ClientHttpIT 在本进程里起一个 HTTP 服务（`127.0.0.1:18091`）扮演供应商和客户的服务器：记下每个请求体，按用例返回指定的 HTTP 状态码、应答内容、延迟。端口 18099 没有服务（连接被拒）。
- ClientHttpIT 的 Redis 经过一个 TCP 代理（`RedisGate`，17796 → 16379），可以把连接切断，模拟 Redis 写不进去。
- 不连数据库（`RecordingStore` 记 SQL，同《发送失败重试测试》）。
- 本程序扮演 Core：往 `stream:http:submit:*`、`stream:http:dr:*` 写，读结果流 `stream:core:results*`。

## 3. 公共配置

### 3.1 ClientHttpIT 的接口文件 `conf/ITHTTP.xml`

```xml
<ITHTTP>
<CharsetMapping><ascii value="ascii">ASCII</ascii></CharsetMapping>
<StatusMapping>
<SUBMITRESP><ok value="0"><code>0</code><text>ok</text></ok><badto value="2120"><code>9</code><text>bad</text></badto></SUBMITRESP>
<DR><done value="10000"><code>D</code><text>DELIVRD</text></done><und value="10400"><code>U</code><text>UNDELIV</text></und></DR>
<DRRESP><ok value="10000"><code>0</code><text>ok</text></ok></DRRESP>
</StatusMapping>
<MO><DRRequest><HandlerClass>com.king.gmms.protocol.commonhttp.CommonDeliveryReportHttpHandler</HandlerClass><inMsgID>msgid</inMsgID><statusCode>stat</statusCode><statusText>txt</statusText></DRRequest></MO>
<MT><SubmitRequest><HandlerClass>it.ItHttpHandler</HandlerClass></SubmitRequest><SubmitResponse><outMsgID>id</outMsgID><StatusCode>code</StatusCode><StatusText>text</StatusText></SubmitResponse></MT>
</ITHTTP>
```

意思：
- 供应商应答是 `id=…&code=…&text=…`，`code=0` 是成功（0），`code=9` 是 2120；
- 推给客户的 DR 是 `msgid=…&stat=…&txt=…`，DELIVERED 写成 `D / DELIVRD`，UNDELIVERABLE 写成 `U / UNDELIV`；
- 发送用测试的处理类 `it.ItHttpHandler`。

`conf/ITPLAIN.xml` 和它一样，只是没有发送的 `HandlerClass`（用默认处理类）。

### 3.2 ClientHttpIT 的通道

```
[HTV51]
CCBSSID: 51
A2PPlayerType: operator
Protocol: ITHTTP
ChlQueue: CommonHttpClient
ChlInit: true
ChlURL: http://127.0.0.1:18091/sms
SMSOptionHttpReadTimeout: 2
SMSOptionHttpConnectTimeout: 1
```

| 块 | 不同 |
|---|---|
| HTV51 | 如上（也作为接收 DR 推送的 HTTP 客户） |
| HTV52 | `ChlURL` 端口 18099（没有服务） |
| HTV53 | `Protocol: ITPLAIN` |

GmmsConfig 追加：

```
AsyncSqlWriterEnable=false
Redis_master_port=17796
HttpResultRetryIntervalMs=300
HttpResultRetryMax=20
```

供应商默认应答：HTTP 200，`id=VH1&code=0&text=ok`。

### 3.3 ClientBatch2IT 的通道（回归环境基础 cm.cfg）

```
[HTTP14]
CCBSSID: 14
A2PPlayerType: operator
Protocol: HTTP
ChlQueue: CommonHttpClient
ChlInit: true
ChlURL: http://127.0.0.1:18080/sms
SMSOptionHttpReadTimeout: 1

[HTTP15]
CCBSSID: 15
A2PPlayerType: operator
Protocol: HTTP
ChlQueue: CommonHttpClient
SMSOptionOutgoingThrottlingNum: 2

[HTTP16]
CCBSSID: 16
A2PPlayerType: operator
Protocol: HTTP
ChlQueue: CommonHttpClient
```

HTTP15、HTTP16 没有 `ChlInit`（没有会话），每条处理过的 MT 正好一个失败结果，用来数处理次数。GmmsConfig 在测试里改：auto-claim 打开（空闲 1500 毫秒、间隔 500 毫秒），`RedisStreamConsumer.HttpClientSubmit.WorkerMaxPoolSize=2`。

## 4. 用例

### 4.1 发送和应答（ClientHttpIT，客户 8888 → HTV51）

| ID | 情况 | 预期 |
|---|---|---|
| HT-07a/b | 发一条 | 一个真实请求，带接收号码和发送方 |
| HT-07c/d | 应答 `id=VH1&code=0` | 成功（0），供应商消息 ID `VH1`，作为提交结果写出 |
| HT-07e/f | — | 流空了，没有待确认，没有残留标记 |
| HT-07g | HTV53，没有发送的 `HandlerClass` | 默认处理类发 Common XML（`<MESSAGES><AUTHENTICATION>…`），成功 |
| HT-06/06b | 没有会话的通道 | 失败结果 2500，确认 |

**HT-08**：应答映射成内部状态：

| 应答 | 内部状态 |
|---|---|
| `code=9`（接口文件里的） | 2120 |
| 接口文件里没有的码 | 9000 |
| 不是 key=value | 2130 |
| 空的应答 | 2130 |
| HTTP 400 | 2300 |
| HTTP 401、403 | 5000 |
| HTTP 408 | 2500 |
| HTTP 500、504 | 2300 |
| HTTP 503 | 2600 |
| 连接被拒（HTV52） | 9000（不是 SMPP 通道的 2500） |
| 读超时 | 9000 |

### 4.2 推 DR 给客户（ClientHttpIT，HTV51 作为客户）

| ID | 情况 | 预期 |
|---|---|---|
| HT-09a/b | DELIVERED 的 DR | 推送 `msgid=IN-…&stat=D&txt=DELIVRD`，带客户的消息 ID |
| HT-09c/d | — | 写 DR 应答结果 10000；流空了 |
| HT-09e | UNDELIVERABLE | `stat=U&txt=UNDELIV` |

**HT-10**：客户服务器的回应映射成 DR 应答结果：

| 客户服务器 | 结果 | 意思 |
|---|---|---|
| HTTP 401、403、511 | 10500 | 最终失败，不重试 |
| HTTP 400、404 | 10900 | 最终失败 |
| HTTP 504、408、500、503 | 11005 | 临时失败，DR 重试 |
| 连接被拒、读超时 | 11005 | 临时失败 |

| ID | 情况 | 预期 |
|---|---|---|
| HT-10h | — | 每个条目都确认 |
| HT-14a/b | 临时失败 | 结果是 11005，真实状态留在状态文字里（`Delivered`、`Undeliverable for Delivery Report`）；重试时带真实状态（`stat=D` / `stat=U`） |
| HT-15a | 推不出去（临时失败） | Core 把 DR 留在 RDQ 等重试 |
| HT-15b | HTTP 401 | 最终失败，不留 |
| HT-15c/d | 重发成功 | 删掉 RDQ 那一行；带真实状态 `stat=D` |

### 4.3 只发一次（ClientHttpIT）

| ID | 情况 | 预期 |
|---|---|---|
| HT-05 | auto-claim 在工作线程确认前读走；或 auto-claim 先做完 | 都是一个真实请求、一个结果 |
| HT-13a/b | Redis 断开，写不进发送标记 | 不发请求，条目留在待确认里 |
| HT-13c～e | Redis 恢复 | 条目能被领取，一个请求；一个结果（写结果时碰到断开的连接会再写），1 个请求 / 1 个结果 |
| HR-02a/b | 请求发出了，两次写结果都失败 | 标记写着已发送，结果等着重写（请求 1，标记 D，重试 1）；日志说明是这种情况，不是 `failed before HTTP response` |
| HR-03a/b | 结果在重试时，同一个消费者重投、或本进程另一个消费者领取 | 都不再发 |
| HR-04a～c | Redis 又能写 | 写出真实结果：成功，供应商消息 ID `VHR1`；一个请求、一个结果，没有待确认、没有标记 |
| HR-05a/b | 重试 `HttpResultRetryMax` 次都失败 | 放弃，打日志；之后重投也不再发（由 Core 的 outbox 报 2700） |
| HR-06a～c | HTTP 503 且结果写不进去 | 留着重写；写出时是应答的状态 2600，一个请求 |
| HR-08/08b | 停止时 | 再试一次，剩下的写进日志；（这里进程还在跑）之后写出，一个请求 |

### 4.4 超时（ClientBatch2IT，HTTP14，本进程的 HTTP 服务 4 秒后才应答）

| ID | 情况 | 预期 / 2026-11-27 实测 |
|---|---|---|
| H-T0 | — | HTTP14 的会话是 `CommonHttpSession` |
| H-T1 | 读超时、连接超时 | `SMSOptionHttpReadTimeout: 1` → 读 1000 毫秒；连接没配 → 模块的 `ConnectTimeout` 30000 毫秒 |
| H-T1b | HTTP16 | 没有初始化（没有会话），用于限速测试 |
| H-T2 | 4 秒才应答 | 请求到了，1010 毫秒后放弃（读超时 1 秒，不是模块的 60 秒） |
| H-T3 | 重新加载改成 6 秒 | 同一个会话下一次请求用新的超时（6000 毫秒） |
| H-T4 | — | 收到 4 秒的应答（4004 毫秒） |

### 4.5 限速和隔离（ClientBatch2IT）

| ID | 情况 | 预期 / 实测 |
|---|---|---|
| H-R3 | 2 个工作线程，HTTP15（2 TPS）有积压 | 不限速的 HTTP16 的 20 条 105 毫秒处理完，这时 HTTP15 才到 4/20 |
| H-R4 | — | 没有线程在 `RateLimiter.acquire` 里等 |
| H-R1a～c | HTTP15 20 条 | 都处理；每条一次（虽然等待超过 auto-claim 的空闲 1.5 秒）；用了 9609 毫秒（守住 2 TPS） |
| H-R2 | — | 推迟的不算投递次数，没有进死信 |
| H-C | auto-claim 在工作线程确认前读走；或 auto-claim 先做完 | `session.submit` 只执行一次，另一个副本丢掉，给 Core 一个结果 |

## 5. 最近一次结果

| 时间 | 环境 | 结果 |
|---|---|---|
| 2026-11-27 | 测试服务器全量回归（提交 `32c4d33` 的代码） | ClientHttpIT 68 项、ClientBatch2IT 18 项，全部通过 |

## 6. 没有覆盖到的

| 内容 | 说明 |
|---|---|
| 生产上各家供应商自己的 HTTP 接口文件和处理类 | 只用了测试的 ITHTTP、ITPLAIN |
| HTTPS、证书 | 只测了 http |
| 供应商推 DR 给网关（入向 HTTP DR）、HTTP 的 MO | 没有用例 |
| HTTP 通道的长短信 | 没有用例 |
