# Server DR pending 内存模式优化：设计与测试报告

> 日期：2026-11-03。范围：Server 的 `SmppPendingDrManager`、Client 的 `SmppPendingSubmitManager`，用例 `ServerBatch3IT`、`ClientPendingIT`。没有新增配置。Client 部分见第 6 节。

## 1. 问题

全量回归里 `S3-01a` 每轮失败。用 Redis `MONITOR` 抓取后，查出两件事。

### 1.1 用例的统计时段被污染

`true` 模式这一轮共 640 次 EVAL：

| 来源 | 次数 | 说明 |
|---|---|---|
| 测试写入 DR | 200 | 统计时减掉 |
| 写结果并确认消费 | 200 | 合并的 Lua |
| 删除 `smpp:pending:dr` | 200 | 见 1.2 |
| `ConfigFilePublisher` 写 `config:*` | 33 | 启动时的配置写入，和 DR 无关 |
| 响应早于登记，单独确认消费 | 5 | 设计如此 |
| 门铃清理 | 2 | 和 DR 数量无关 |

配置写入功能是 11-01 加入的，晚于这条用例（10-30），它落进统计时段后，结果从 2.03 变成 2.20，差值低于断言的 0.9。产品没有退化：在原代码上多等 8 秒再统计，结果是 2.01 对 3.03。

### 1.2 memory 模式每条 DR 多一次 Redis 调用

`SmppPendingDrManager.remove()` 不分模式，都会调用 `deleteRedisPending`。memory 模式不往 Redis 写 pending，这次调用只是删除一个不存在的 key。旧报告把这一次误判成门铃唤醒。

## 2. 设计

`deleteRedisPending` 先查本进程记录的 `redisKeyBySequence`：

| 情况 | 处理 | 原因 |
|---|---|---|
| 序号在记录里 | 删除 | 本进程写过，包括写入后客户切到 memory 的情况 |
| 不在记录里，客户是 `redis` 或 `hybrid` | 删除 | 和原来一样 |
| 不在记录里，客户是 `memory` | **不访问 Redis** | 本进程没有写过 |

**重启遗留的 key：** 客户从 `redis` 改成 `memory` 并重启后，旧进程的 key 还在 Redis。这些 key 由超时扫描线程处理并写超时结果。原来的代码只有在序号碰巧相同时才会删掉它，而且不写结果；现在不再误删，结果也不会丢。

**没有改的部分：** 超时扫描线程仍然常驻，每秒读一次 zset 登记集合，这和 DR 数量无关。

## 3. 用例修改

- **统计时段：** `ServerBatch3IT` 启动前关闭 `ConfigFilePublishEnable`、`GmmsConfigPublishEnable`，避免启动写入被计入。不用固定等待，因为这批写入和启动之间有几秒空档，等待时长不可靠。
- **新增绝对值断言：** `S3-01c`，每条 DR 少于 1.2 次。

## 4. 测试结果

`ServerBatch3IT` 连跑 2 轮，24 项全部通过。

| 指标 | 优化前 | 优化后 |
|---|---|---|
| 每条 DR 的 Lua 调用（合并确认） | 2.01 | 1.02～1.04 |
| 每条 DR 的 Lua 调用（`ServerDrAckOnResp=false`） | 3.03 | 2.04～2.05 |

优化前的数字来自多等 8 秒的对照实验。

| ID | 场景 | 预期 | 结果 |
|---|---|---|---|
| S3-01a | 两种确认方式对比 | 合并后少约 1 次 | 通过 |
| S3-01c | memory 模式绝对值 | 少于 1.2 次 | 通过 |
| PM-01 | memory 模式，50 次 put 和 remove | 几乎没有 EVAL（实测 0 次） | 通过 |
| PM-02 | memory 模式 | Redis 没有 pending key | 通过 |
| PM-03、PM-04 | `redis`、`hybrid` 模式 | 写入 key，响应后删除 | 通过 |
| PM-05a、b | 写入后客户切到 memory | key 保留，响应时仍删除 | 通过 |
| PM-06 | 从 memory 切到 redis | 删除旧序号没有副作用，新消息写 key | 通过 |
| PM-07a、b | memory 模式，有无关的遗留 key | 响应不误删；扫描线程写超时结果 | 通过 |
| PM-08a、b | redis 模式走真实会话，20 条 DR | 全部送达和确认，没有遗留 key | 通过 |

## 5. 限制

- PM-01～PM-07 直接调用 pending 管理器，模式切换用反射修改客户配置，没有走真实的 cm reload。
- 没有实测进程重启：遗留 key 由测试直接写入 Redis 模拟。
- 数字来自本地单机，只比较调用次数，没有测吞吐。

## 6. Client：`SmppPendingSubmitManager`

Client 的写法和 Server 相同，按同样的规则修改（第 2 节的表格同样适用）。修改前先在原代码上实测，确认确实需要。

### 6.1 修改前后

用例走真实 SMPP 链路（Client 会话绑定到同进程的 Server，由 Server 扮演供应商），只统计操作 `smpp:pending:submit:*` 的 Lua：

| 指标 | 修改前 | 修改后 |
|---|---|---|
| memory 模式，100 条 submit 的 pending Lua（CP-01b） | 100 次 | 0 次 |
| redis 模式，20 条 submit（CP-02b） | 20 次 | 20 次（不变） |
| 重启遗留 key 碰上相同序号（CP-06） | key 被响应删除，**超时结果丢失** | key 保留，扫描线程写超时结果 |

### 6.2 用例

新增 `ClientPendingIT`，已加入 `regression/suites.txt`，连跑 2 轮结果一致。

| ID | 场景 | 预期 | 结果 |
|---|---|---|---|
| CP-00、CP-01a | memory 模式，真实链路 100 条 | 全部收到响应并写结果 | 通过 |
| CP-01b、c | 同上 | 没有 pending Lua，Redis 没有 pending key | 通过 |
| CP-02a～c | redis 模式，真实链路 20 条 | 全部写结果，有 pending Lua，没有遗留 key | 通过 |
| CP-03a、b | `hybrid` 模式 | 写入 key，响应后删除 | 通过 |
| CP-04a、b | 写入后供应商切到 memory | key 保留，响应时仍删除 | 通过 |
| CP-05 | 从 memory 切到 redis | 删除旧序号没有副作用，新消息写 key | 通过 |
| CP-06a、b | memory 模式，有无关的遗留 key | 响应不误删；扫描线程写超时结果 | 通过 |

### 6.3 限制

和第 5 节相同：模式切换用反射修改配置，遗留 key 由测试写入，没有实测进程重启，也没有测吞吐。Client 每条 submit 的其他 Redis 调用（如结果写入）没有改动，也没有统计。
