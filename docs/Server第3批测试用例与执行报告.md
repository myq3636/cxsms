# Server 第 3 批：测试用例与执行报告（DR 结果写入与确认消费合并、enquire_link 间隔、pending 管理器重复创建）

> 设计见《Server第3批优化设计》。同时对此前所有批次做了全量回归。

## 1. 测试环境

与 Server 第 2 批相同：在测试进程里启动真实的 `MultiSmppServer`，用测试代码中的 SMPP 客户端连接，本地 Redis 7.2.5，JDK 17。

为本批调整的客户配置（cm.cfg）：

| 客户 | 配置 | 用途 |
|---|---|---|
| SMPPCUST32 | `ServerEnquireLink: true`、`EnquireLinkTime: 1`、`FailEnquireLinkTime: 10` | enquire_link 间隔 |
| SMPPCUST36 | `ServerEnquireLink: true`、`EnquireLinkTime: 1`、`FailEnquireLinkTime: 2`、`EnquireLinkFailureNum: 3` | 不回 enquire_link 时断开 |
| SMPPCUST31 | `BufferTimeout: 3` | DR 超时、写结果失败后重试 |

## 2. 用例（`ServerBatch3IT`）

| ID | 场景 | 预期 / 实测 | 级别 |
|---|---|---|---|
| S3-01a | 给立即应答的客户发 200 条 DR，用 Redis 命令统计计算 Server 侧每条 DR 的 Lua 调用数 | 合并后为 **2.03 次/条**；`ServerDrAckOnResp=false`（发出后确认消费）时为 **3.01 次/条**，少了一次往返。剩下的约 1 次是测试里逐条写入 DR 时，每条都会唤醒一次 stream 的门铃 | P0 |
| S3-01b | 同上 | 每条 DR 恰好确认消费一次：200 条全部在写结果的同一段 Lua 中完成（其他几次运行中有 1 到 2 条走了"响应比登记还快"的路径，单独确认，也正确） | P0 |
| S3-02 | 客户不回 resp，`BufferTimeout` 3 秒 | 超时后写入 1 条失败结果，同时确认消费 | P0 |
| S3-03 | 写结果被 stream 背压拦住（`RedisStreamBackPressureEnable=true`、阈值 1） | 拦住期间：没有写入结果，也没有确认消费。解除后：pending 管理器重试，结果写入，同时确认消费 | P0 |
| S3-04 | 客户正常应答 enquire_link（`EnquireLinkTime` 1 秒、`FailEnquireLinkTime` 10 秒） | 5.5 秒内收到 5 个 enquire_link（每秒一个）。原来的行为是每 10 秒才一个 | P0 |
| S3-05 | 客户不回 enquire_link（`FailEnquireLinkTime` 2 秒、`EnquireLinkFailureNum` 3） | 发出 3 个 enquire_link 后，约 4.1 秒断开 | P0 |
| S3-06 | 连续 10 个新连接，每次 bind 后立刻有 20 条 DR 同时来自两个分片 stream | 200 条全部送达；每个 resp 都能匹配上（0 条 `Can't find the original message`）；全部确认消费 | P0 |

## 3. 测试中发现并修复的问题

### DR pending 管理器重复创建（原有问题，影响线上）

第一次全量回归中，S3-01b 少了一条确认消费。追查发现，客户回的 `deliver_sm_resp seq=1` 被记录为 `Can't find the original message`。

**原因**：两个 DR worker 同时给同一个新会话发出第一批 DR，各自创建了一个 pending 管理器，后创建的覆盖了前一个。前一个管理器里登记的 DR 再也查不到。

**线上的后果**：
- 客户已经回了 resp，却匹配不上。
- 约 60 秒后，被覆盖的那个管理器按超时给 Core 回失败结果。
- Core 按重试策略重发，客户收到重复的 DR，CDR 里多一条失败记录。

**修复**：pending 管理器改为加锁、只创建一次。Client 侧创建 submit pending 管理器的地方是同样的写法，一并修复。S3-06 专门覆盖这个场景，修复后连续 3 次单独运行和最终的全量回归都通过。

### 测试代码的调整

| 问题 | 处理 |
|---|---|
| S3-01a 第一版断言"每条 DR 少于 1.1 次" | 实测在测试场景下有一次额外的门铃唤醒，改为断言"比发出后确认消费少 1 次" |
| S3-03b 第一版把背压阈值设为 0 | 阈值 0 表示关闭背压，改为 1 |
| S3-05 第一版过早判断连接被断开 | 测试客户端的 `isClosedByServer()` 会设置 1 毫秒的读超时，导致客户端自己的读线程退出。改为只看读线程是否还活着 |
| S3-04 第一版只收到 2 个 enquire_link | 这是代码问题：没有发 enquire_link 时，下一次检查要再等一整个 `EnquireLinkTime`，间隔被拉长到将近两倍。已修复为在"空闲满 `EnquireLinkTime`"的那一刻检查 |

## 4. 不在本地范围的项

| ID | 项 | 建议做法 |
|---|---|---|
| X-S31 | 线上 Redis 下的 DR 吞吐 | 压测时对比本批前后，单节点每秒能处理的 DR 数量（每条 DR 从 2 次往返降到 1 次） |
| X-S32 | 已开启 `ServerEnquireLink` 的客户 | enquire_link 会从约每 10 秒一次变为每 `EnquireLinkTime`（默认 30 秒）一次。上线前确认这些客户的 `EnquireLinkTime` 配置合理 |
| X-S33 | pending 管理器重复创建的修复 | 线上统计 `Can't find the original message` 日志，上线后应基本消失 |

## 5. 回归结果

执行时间 2026-10-30 14:21（AEDT）；`run-all.sh` 从头全量编译后执行 19 个套件。

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
| **ServerBatch3IT（新增）** | 10 | 10 | 0 |
| **合计** | **404** | **404** | **0** |
