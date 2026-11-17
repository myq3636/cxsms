# RC010 ROP 失败模式：进 SMQ 等通道恢复，不走备份；恢复后发原来的通道

**对应用例清单**：I-04 I-05（`features/01_路由重构/路由重构测试用例清单.md`）

**目的**：主路由选出的通道在 ROP 失败模式（OperatorMode.properties 的 ROP-FailedMode）：MT 进 SMQ，不切到备份；通道恢复后（去掉 ROP-FailedMode）由 MQM 发到原来的通道。

## 配置

`routing/RTCUSTR_routingInfo.cfg`：

```text
CCBSSID:9808
A2PMainRouting:(Priority=10,PhonePrefix=86,Relay=RTV1)
A2PBackupRouting:(Priority=10,PhonePrefix=86,Relay=RTV2)
```

`cm.extra.cfg`（追加到 base/cm.cfg）：RTCUSTR。

`conf/OperatorMode.properties`（运行时替换 conf 下的文件，结束后恢复）：

```text
ROP-FailedMode=9811
```

`conf.append/MQMTaskConfig.properties`（运行时追加到 conf 下的文件，结束后恢复）：

```text
# RC010: SendROPFailedMessageTask only resends the SMQ messages of these rssids
SMQ.SendROPFailedMessageTask.OPList=9811
```

## 步骤和预期

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|
| 1 | OTP → 8613800001001 | rtv1 | `rssid=9811 by main prefix=86 priority=10 file=RTCUSTR channel in ROP failed mode: insert into SMQ` | DELIVRD | I-04：先进 SMQ（这时 RTV1、RTV2 都收不到）；I-05：20 秒后去掉 ROP-FailedMode，MQM 发到 RTV1（不是备份 RTV2）；最多等（ms） `240000`；DB 行 `smq:RSsID=9811` |

**第 2 阶段**：第 1 阶段的 MT 发完 20 秒后，换成 `phase2/conf/` 的文件，再发：

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR、CDR、DB）；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致。

## 注意

OperatorMode.properties 每 5 秒按修改时间重新读取，第 2 阶段只换文件、不用命令。MQM 的 SendROPFailedMessageTask 每 30 秒把 SMQ 里通道已恢复的消息重发，最多等 4 分钟。**ROP 失败模式的消息按客户的重试策略处理**：选到通道后、设置 RA2P 之前就返回了，`RetryPolicyManager` 走“RA2P=0”的分支取客户的 `SMSOptionMORetryPolicy`，客户没配时用全局 `DefaultRetryPolicy`（这台机器是 `20M*1`，只有一个点：第一次就是最后一次，不进 SMQ，直接回失败 DR）。路由重构前也是这样。所以这个用例用配了 `SMSOptionMORetryPolicy:2M,2M` 的客户 RTCUSTR；等待期间检查 `smq` 表里有这条消息。**MQM 只重发 `MQMTaskConfig.properties` 的 `SMQ.SendROPFailedMessageTask.OPList` 里列出的通道的 SMQ 消息**（不配就什么都不发；这台机器这一行是注释掉的），所以用例运行时追加 `OPList=9811`（MQM 启动时读取）。消息在 SMQ 里还要等到 `NextRetryTime`（客户策略的第一个点，2 分钟）之后才会被取出。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/RC010_rop_failed_smq`（换配置并重启模块）。
