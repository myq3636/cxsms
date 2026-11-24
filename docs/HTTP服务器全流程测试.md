# HTTP 服务器全流程测试（HT001）

> 2026-11-24。对象：CommonHttpServer 模块的四个 servlet：`CommonHttpServlet`（XML 提交）、`CommonJsonHttpServlet`（JSON 提交）、`AliCainiaoJsonHttpServlet`（菜鸟 JSON 提交，带签名）、`CommonHttpDRServlet`（通道回 DR）。在测试机上启动 CommonHttpServer1，用真实的 Core、CommonHttpClient、Redis 跑完整的消息流程。

## 1. 流程

```
客户 ──XML/JSON/菜鸟──> CommonHttpServer1（Tomcat 9）──stream:mt:pending──> Core（路由到 HTTP 通道）
     ──stream:http:submit──> CommonHttpClient1 ──Common XML──> 通道（模拟）
通道 ──DR──> CommonHttpServer1 /Common/DRReply（CommonHttpDRServlet）──stream:core:inbound-dr──> Core（DR 匹配）
     ──stream:http:dr──> CommonHttpClient1 ──按客户的接口格式推 DR──> 客户的 ChlDRURL（模拟）
```

## 2. 运行

```
cd /usr/local/a2p/routetest && bin/http_case.sh                              # 自带的 Tomcat 9，端口 8080
cd /usr/local/a2p/routetest && RT_HTTP_SERVER=installed bin/http_case.sh     # 本机部署的 CommonHttpServer1，端口 80
```

`RT_HTTP_SERVER=installed` 用 `$A2P/bin/CommonHttpServer1.sh` 重启本机部署的 CommonHttpServer1（套用用例配置之后），端口 `RT_HTTP_PORT`（默认 80），测完不停。

- `bin/run_case.sh` 套用用例（cm、路由、`conf/` 下的接口文件），重启模块（默认列表外加 `CommonHttpClient1`）；新增的 `RT_DRIVER` 环境变量让它运行别的驱动程序，不设时仍是 `rt.RouteCase`。
- `bin/http_driver.sh`：在 `TOMCAT9_HOME`（默认 `/usr/local/apache-tomcat-9.0.124`）用自己的 `CATALINA_BASE`（`routetest/.tomcat9`）启动 Tomcat 9，Gmms 上下文的部署描述符用用例的 `tomcat/web.xml`（`altDDName`，`Gmms/WEB-INF/web.xml` 不动），`-Dmodule=CommonHttpServer1`；然后运行 `rt.HttpFlow`，结束后停 Tomcat，日志放进结果目录。
- `rt.HttpFlow`（`routetest/src/rt/HttpFlow.java`，只用 JDK）扮演三个客户和通道：通道收到 MT 回空应答（Common 接口里表示成功），500 ms 后向 `/Common/DRReply` 发 DR；客户的 DR 推送地址也在它这里。结果在 `cases/HT001_http_servers/result/{时间}/report.txt`。

用例的配置（`cases/HT001_http_servers`）：

| 角色 | cm | 说明 |
|---|---|---|
| XML 客户 RTHX 9951 | `protocol:HTXMLC`、`SPID:htxml`、`ChlDRURL` | DR 按 `HTXMLC.xml`（Common 的 MO 格式）推送 |
| JSON 客户 RTHJ 9952 | `protocol:HTJSONC`、`SPID:htjson` | 同上 |
| 菜鸟客户 RTHC 9953 | `protocol:CommonCainiao`、`SPID:htcainiao`、`chlPwdr`（签名密钥） | DR 按 `CommonCainiao.xml`（`AliCainiaoDeliveryReportHttpHandler`）推送菜鸟 JSON |
| 通道 RTHV 9961 | `protocol:Common`、`ChlURL`、`chlPwdr`（发给通道的 PRODUCTTOKEN） | 只有通道用 `Common`：`CommonHttpDRServlet` 取 `Protocol` 为接口名的第一个 ssid 当作回 DR 的通道 |

客户和通道的 `ChlQueue` 都是 `CommonHttpClient`。

## 3. 检查项和结果

测试机，2026-11-24 07:43，15 项全部通过（`result/20261124_074320`）：

| 编号 | 检查 | 结果 |
|---|---|---|
| HT-01 | XML 提交被接受，应答 `{REFERENCE}:OK` | 通过 |
| HT-02 | XML：未知 PRODUCTTOKEN 被拒（`Error:Invalid product token`） | 通过 |
| HT-03 | JSON 提交被接受，应答 `{"msgid":…,"status":"OK"}` | 通过 |
| HT-04 | JSON：没有 FROM 被拒（`Sender Address Error`） | 通过 |
| HT-05 | 菜鸟提交（签名正确）被接受：`code=OK`、`requestId`、`bizId`、`smsSize=1` | 通过 |
| HT-06 | 菜鸟：签名错误被拒（`Verification.Fail`） | 通过 |
| HT-07～09 | 三个客户的 MT 都到了通道：Common XML，PRODUCTTOKEN、FROM、BODY、REFERENCE 正确 | 通过 |
| HT-10 | 被拒的三条都没有发到通道 | 通过 |
| HT-11 | `CommonHttpDRServlet` 收下通道的 3 个 DR，都回 200 | 通过 |
| HT-12、13 | DR 推给 XML、JSON 客户：`reference` 是客户的 REFERENCE，`statuscode=2`（Delivered），`sent`、`received` | 通过 |
| HT-14 | DR 推给菜鸟客户：菜鸟 JSON，`requestId`、提交应答里的 `bizId`、`DELIVERED`、`extend` 原样带回、有签名 | 通过 |
| HT-15 | 每条接受的消息一个 DR，被拒的没有 | 通过 |

第二次（2026-11-24 08:32，`RT_HTTP_SERVER=installed`，测试机部署的 CommonHttpServer1，Tomcat 9，端口 80，`result/20261124_083220`）加了菜鸟的中文、长短信，21 项全部通过：

| 编号 | 检查 | 结果 |
|---|---|---|
| HT-16 | 菜鸟中文内容被接受，`smsSize=1` | 通过 |
| HT-17 | 菜鸟 200 个字符被接受，`smsSize=2` | 通过 |
| HT-18 | 中文 MT 按 UCS2（DCS 8）到通道，内容不变 | 通过 |
| HT-19 | 长 MT 完整到通道（Common 通道一条发出，200 个字符） | 通过 |
| HT-20、21 | 中文、长短信的 DR 按菜鸟格式推回，`smsSize` 分别为 1、2 | 通过 |
| HT-11、15 | 改为按每条消息核对：通道的 5 个 DR 都回 200，5 条接受的消息各有 DR，被拒的没有 | 通过 |

Core 日志里菜鸟的三条都是 `IN_SUBMIT` → `MT-ROUTE`（RTHC → RTHV）→ `MT-TO-CLIENT`（`path=CommonHttpServer1>CoreEngine*`）→ `VDR-MATCH` → `DR-TO-HTTP` → `Delivery Report Resp status=10000`。

## 4. 测试中发现并修改的问题

| # | 问题 | 影响 | 修改 |
|---|---|---|---|
| 1 | `AbstractHttpServer.putGmmsMessage2RouterQueue` 还走内部连接（Redis 流之前的方式），而 HTTP Server 已不再建内部连接 | `CommonHttpServlet` 等用它的 servlet 收到的 MT 一条也进不了 Core，却给客户回成功；还会伪造一个 REJECTED DR | 改为 Redis 流：提交写 `mt:pending`（`produceSubmitMessage`），通道 DR 写 `core:inbound-dr`（`produceInboundDeliveryReport`），内部确认（INNER_ACK）不再需要；DR 查询等没有流的类型仍走原路径。用到它的还有 `HttpBatchDRServlet`、`CommonRESTHttpDRServlet`、`AnHuaInsuranceHttpServlet` 等 |
| 2 | `CommonHttpServlet` 写入失败时伪造 REJECTED DR 后仍回成功 | 同上 | 和 `CommonJsonHttpServlet` 一样回 `SERVER_ERROR`，客户重发 |
| 3 | `CommonHttpServlet` 用 `SAXBuilder.build(String)` 解析，JDOM 1 把参数当 URI | 任何 XML 提交都解析失败，回鉴权错误 | `build(new StringReader(xml))` |
| 4 | 三个提交 servlet 的"按国家扣费"用应答用的空消息取号码 | 开了 `SmsOptionChargeCountryByGateWay` 的客户，请求抛异常后没有任何应答 | 用提交的消息的号码 |
| 5 | Core 只在 `Protocol` 等于 `HTTP` 时把 MT 写 `http:submit`、DR 写 `http:dr`；而 CommonHttpClient 用 `Protocol` 作接口名（`Common`、`CommonCainiao`、…） | 按接口名配置的 HTTP 通道、HTTP 客户在流模式下收不到 MT、DR（进了 SMPP 的 `mt:routed`、`dr:pending`） | `A2PCustomerInfo.isHttpProtocol()`：`Protocol` 为 `HTTP`，或 `ChlQueue` 为 `CommonHttpClient`；Core 的 5 处判断都用它 |
| 6 | `Common` 接口作为通道时，`createXml` 的 JAXB 代码被注释（Java 17 没有 JAXB），返回空串 | 发往 Common 通道的 MT 全部 "Can't send empty message"，2500 | 手工生成 `CommonHttpServlet` 能解析的 XML（转义特殊字符）；`ClientHttpIT` HT-07g 改为检查发出的 Common XML |
| 7 | `HttpSession` 要求有 `ChlURL` | 只配 `ChlDRURL` 的 HTTP 客户推 DR 时空指针，DR 进重试（11005） | 没有 `ChlURL` 用 `ChlDRURL`，反之亦然 |

## 5. 测试机部署（2026-11-24 已改）

| 项 | 原来 | 现在 |
|---|---|---|
| Tomcat | `/usr/local/tomcat` → `tomcat-6.0.53`，在 Java 17 上起不来（`-Djava.endorsed.dirs` 不支持；a2p.jar 是 Java 17 编译的） | `/usr/local/tomcat` → `apache-tomcat-9.0.124`（sha512 已校验；Tomcat 10 以上改用 `jakarta.*`，这些 servlet 不能用）。`tomcat-6.0.53` 目录保留 |
| 端口 | 80；8080（3000 线程，acceptCount 800）；8443（没有配 keystore，`~/.keystore` 不存在，用不了）；AJP 8009 | 80；8080（3000 线程，minSpareThreads 100，acceptCount 800）。不配 8443、AJP（Tomcat 9 的 AJP 要 secret）。原文件 `conf/server.xml.orig` |
| 模块名 | Tomcat 6 `catalina.sh` 写死 `-Dmodule=CommonHttpServer2`（`a2p_modules.conf` 里没有） | Tomcat 9 `bin/setenv.sh`：`-Dmodule=CommonHttpServer1 -Da2p_home=/usr/local/a2p/ -Dlog4j.configurationFile=…`，堆 1～2 GB，GC 日志 `log/CommonHttpServer1_gc.log`，`CATALINA_PID` |
| 上下文 | `conf/Catalina/localhost/Gmms.xml` | 同一文件拷到 Tomcat 9 |
| `Gmms/WEB-INF/web.xml` | `org.apache.logging.log4j.core.web.Log4jContextListener`（log4j-core 里已没有，webapp 起不来） | `org.apache.logging.log4j.web.Log4jServletContextListener`（WEB-INF/lib 里已有 log4j-web-2.24.0）；新增 `AliCainiaoJsonHttpServlet`：`/Gmms/CommonCainiao/MTRequest`（protocol `CommonCainiao`）。只改这两处，CRLF 不变，原文件 `web.xml.bak.20261124` |
| 菜鸟客户的接口 | 没有 | `conf/CommonCainiao.xml`（DR 用 `AliCainiaoDeliveryReportHttpHandler` 推菜鸟 JSON）：菜鸟客户配 `protocol:CommonCainiao`、`SPID`（= supplier）、`chlPwdr`（签名密钥）、`ChlDRURL`、`ChlQueue:CommonHttpClient` |
| `bin/CommonHttpServer1.sh` | stop：`ps | grep tomcat` 拿 pid，有两个进程时 `[ -n $pid ]` 报错，不 kill；Tomcat 9 停止后 a2p 的线程让 JVM 不退出，于是一直留着，再 start 时被 PID 文件挡住 | stop：`catalina.sh stop 20 -force`（20 秒没退出按 CATALINA_PID 强杀，实测 21 秒停完，进程、PID 文件都清掉）；start、restart 同前。原文件 `CommonHttpServer1.sh.bak.20261124`。`CommonHttpServer2.sh`、`3.sh` 没改（模块列表里没有） |

## 6. webapp 停止时关闭 a2p（2026-11-24）

原来 Tomcat 停止后 JVM 退不掉：线程转储里留着的非守护线程是 `CDRFileMonitor` ×2、`CDRFileWriter`、`OPModeConfigMonitor`、`AsyncSqlWriterThread`；`SystemListener.contextDestroyed` 只停了计费定时器。现在 `contextDestroyed` 调 `GmmsUtility.shutdown()`：

1. `GracefulStop.runNow("UNDEPLOY")`：模块停止时同一批写出内存数据的任务（SQL 队列、CDR 队列、SDQ、异常消息队列等），限时 `ModuleStopMaxWaitSeconds`，不退出 JVM；SQL 异步写线程在这里写完队列后退出。
2. `close()`：线程池、控制订阅、模块状态上报（原有）。
3. `CDRManager.shutdown()`：异步 CDR 写入器先停（新的 CDR 交给同步写入器），写完队列，停监视线程和写线程（中断它们的 sleep/poll），关文件（移到 `cdr/link/`）；再停同步写入器、关文件。
4. `A2PCustomerManager.stopConfigMonitor()`：`OPModeConfigMonitor` 结束。
5. `RedisClient.close()`：异步写线程写完队列后结束，所有连接池（state、submit/report MQ 各主）关闭。
6. `DataControl.close()`：所有 Hikari 连接池关闭。

实测（测试机，HT001 跑完后 `catalina.sh stop`，不加 `-force`）：1 秒内 JVM 退出；日志 `[STOP] UNDEPLOY: 4 of 4 jobs finished in 20 ms`、CDR 两个写入器关闭（文件在 `link/`）、`Redis client closed: 5 pool(s)`、两个 DB 池关闭、`a2p shutdown done`。`CommonHttpServer1.sh stop` 仍用 `-force`，只作兜底。

## 7. SMPP 长短信经 Yulore HTTP 通道（HT002，2026-11-24）

```
SMPP 客户 RTCUST ──submit_sm ×2（UDH 05 00 03，153 + 47 字符）──> MultiSmppServer1 ──> Core（两段合成一条）
     ──stream:http:submit──> CommonHttpClient1（YuloreRESTJsonHttpHandler）──JSON──> Yulore 通道（模拟）
通道 ──DR（REFERENCE、STATUS=3）──> CommonHttpServer1 /Gmms/Yulore/DRReply（CommonHttpDRServlet，conf/Yulore.xml）
     ──> Core（DR 匹配，按段拆成两个）──stream:dr:pending──> MultiSmppServer1 ──deliver_sm ×2──> SMPP 客户
```

运行：`RT_HTTP_SERVER=installed bin/http_case.sh cases/HT002_smpp_long_yulore`（驱动 `rt.SmppHttpFlow`，`case.properties` 的 `driver=`；`bin/http_driver.sh` 按它选驱动，默认 `rt.HttpFlow`）。

配置：
- 通道 RTYV 9962：`protocol:Yulore`、`ChlQueue:CommonHttpClient`、`ChlURL`、`ChlAcctr`（Yulore.xml 的 `Token`，`<chlAcctNamer type="ccb">`）。
- 路由：RTCUST（基础配置里的 SMPP 客户 9801）全部到 RTYV。
- 测试机 `Gmms/WEB-INF/web.xml` 新增 `YuloreDR`：`CommonHttpDRServlet`，`/Gmms/Yulore/DRReply`，protocol `Yulore`（按 URL 里的接口名加载 `conf/Yulore.xml` 的 MT DRRequest：`REFERENCE` 是 outMsgID，`STATUS` 3 是 Delivered；按 `Protocol` 为 `Yulore` 的第一个 ssid 认出通道）。

结果（`result/20261124_090214`，部署的 CommonHttpServer1，端口 80），7 项全部通过：

| 编号 | 检查 | 结果 |
|---|---|---|
| LY-01 | SMPP bind_transceiver（rtcust） | 通过 |
| LY-02 | 两段都被接受，各有 message_id | 通过 |
| LY-03 | MT 以 JSON 到 Yulore 通道：`Token`（ChlAcctr）、`Sender`、`Recipient`、`Charset=ASCII`，Content-Type `application/json` | 通过 |
| LY-04 | 通道收到完整的 200 个字符（Core 合成一条，一个请求） | 通过 |
| LY-05 | `/Yulore/DRReply` 收下通道的 DR，回 200 | 通过 |
| LY-06.1、.2 | 客户两段各收到一个 `deliver_sm` DR：`id` 是各段的 message_id，`stat:DELIVRD` | 通过 |

Core 日志：`IN_SUBMIT_PARTIAL` → `IN_SUBMIT`（`inMsgId=1410650000,1410650001`）→ `MT-ROUTE`（RTCUST → RTYV）→ `MT-TO-CLIENT protocol=HTTP`（`path=MultiSmppServer1>CoreEngine3`）→ `HTTP-OUT outMsgId=YL…`（Yulore 应答的 `message`）→ `VDR-MATCH` → 两个 `DR-TO-SERVER`、`DR-OUT`、`DR-RESP pduStatus=0` → 两个 `IN_DELI_REP status=10000`。
