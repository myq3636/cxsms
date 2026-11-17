# PF001 DB 积压：wdq、rdq 各 50 万行前后，同样 50 TPS 的流量，比较 MT 和 DR 的延迟，找出变慢的原因

**对应用例清单**：docs/DB积压性能测试设计.md（`docs/DB积压性能测试设计.md`）

**目的**：由 `bin/run_dbbacklog.sh` 运行两次（A：表里只有现有的数据；B：wdq、rdq 各加 50 万行），每次 50 TPS 6 分钟（前 1 分钟不计入），测试工具记下每条 MT 的三段延迟；同时采 MySQL 的表锁等待、慢查询、正在等锁的语句，B 中间对 3 个 Core 抓线程栈。最后比较 A、B，给出是否变慢和原因的证据。

## 配置

`routing/RTSVCQ_default_routingInfo.cfg`：

```text
A2PMainRouting:(Priority=10,PhonePrefix=8613,Relay=RTVQ1);(Priority=10,PhonePrefix=8615,Relay=RTVQ2)
A2PBackupRouting:(Priority=10,PhonePrefix=8615,Relay=RTVQ1)
```

`cm.extra.cfg`（追加到 base/cm.cfg）：RTVQ1, RTVQ2, RTCQ1, RTCQ2。

通道（测试工具）的行为：

| 通道 | 行为 |
|---|---|
| rtvq2（ssid 9976） | submitStatus=0x45 |

（submitStatus：submit_sm_resp 的 command_status，0x45 → 内部 2300、0x0B → 2120、0x58 → 2600；submitStatusSeq：同一条 MT 第几次到这个通道就取第几个，用完后一直用最后一个；dr：DR 的 stat，none = 不回 DR）

## 步骤和预期

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|
| 1 | OTP → 86138 | * |  |  | 普通 MT + DR（每条 DR 都会 DELETE FROM WDQ）；客户 `rtcq1`；weight `70` |
| 2 | OTP → 86150 | * |  |  | RTVQ2 回 0x45，改道到 RTVQ1；客户 `rtcq1`；weight `10` |
| 3 | OTP → 86138 | * |  |  | 3 段长短信；段数 `3`；客户 `rtcq1`；weight `10` |
| 4 | OTP → 86138 | * |  |  | 客户不回 deliver_sm_resp：DR 进 RDQ，MQM 重发；客户 `rtcq2`；weight `10` |

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR）；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致。

## 注意

`mt.N` 是流量模板（同 MEM001）：RTCQ2（`customer.rtcq2.drResp=false`）不回 deliver_sm_resp，它的 DR 发送超时进 RDQ、由 MQM 重发，覆盖 RDQ 的写入和删除。不要单独用 run_case.sh 跑（那样只是一次压测，没有对比和灌数据）。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/PF001_db_backlog`（换配置并重启模块）。
