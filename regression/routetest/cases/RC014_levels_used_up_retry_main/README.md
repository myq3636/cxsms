# RC014 级别用完：按重试策略重试主路由的通道；主路由再失败不再走备份；RoutingLevel 写进 DB；CDR MsgID

**对应用例清单**：K-05 K-06 K-07 K-08 K-09 N-04 N-05b P-07（`features/01_路由重构/路由重构测试用例清单.md`）

**目的**：所有级别都失败（码都在重试码里）后按重试策略（1M,1M）等 1 分钟，重试主路由的通道（不是最后失败的备份通道），级别保留；重试时主路由再失败不再走备份；DB 的 RMQ 行记着 RSsID=主路由、RoutingLevel；重试主路由的 CDR MsgID 是原来的 msgId。

## 配置

`routing/RTCUST_routingInfo.cfg`：

```text
CCBSSID:9801
A2PMainRouting:(Priority=10,PhonePrefix=86,Relay=RTV1);(Priority=10,PhonePrefix=64,Relay=RTV3);(Priority=10,PhonePrefix=44,Relay=RTV5)
A2PBackupRouting:(Priority=10,PhonePrefix=86,Relay=RTV2);(Priority=10,PhonePrefix=64,Relay=RTV4);(Priority=10,PhonePrefix=44,Relay=RTV6)
```

通道（测试工具）的行为：

| 通道 | 行为 |
|---|---|
| rtv1（ssid 9811） | submitStatusSeq=0x45,0 |
| rtv2（ssid 9812） | submitStatus=0x45 |
| rtv3（ssid 9813） | submitStatusSeq=0x45,0x0B |
| rtv4（ssid 9814） | submitStatus=0x45 |
| rtv5（ssid 9815） | submitStatus=0x45 |
| rtv6（ssid 9816） | submitStatus=0x45 |

（submitStatus：submit_sm_resp 的 command_status，0x45 → 内部 2300、0x0B → 2120、0x58 → 2600；submitStatusSeq：同一条 MT 第几次到这个通道就取第几个，用完后一直用最后一个；dr：DR 的 stat，none = 不回 DR）

## 步骤和预期

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|
| 1 | OTP → 8615000001401 | rtv1>rtv2>rtv1 | `backup levels used up (level 1, rssid 9812): retried on the main route rssid 9811` | DELIVRD | K-05、K-08：RTV1 失败 → 备份 RTV2 失败 → 1 分钟后重试主路由 RTV1，这次成功；最多等（ms） `180000`；DB 行 `rmq:RSsID=9811,RoutingLevel=1`；CDR MsgID `OUT_SUBMIT={id};OUT_SUBMIT={id}-9812;OUT_SUBMIT={id};OUT_DELI_REP={id};IN_DELI_REP={id}` |
| 2 | OTP → 64210001402 | rtv3>rtv4>rtv3 | `not rerouted: status 2120 not in SMSOptionSubmitRetryCode of rssid 9813` | !DELIVRD | K-07：重试主路由 RTV3 时回 0x0B（不在重试码里）：失败 DR；最多等（ms） `180000` |
| 3 | OTP → 44770001403 | rtv5>rtv6>rtv5 | `no backup level after level 1 (rssid=9815` | !DELIVRD | K-06、K-09：重试主路由 RTV5 再失败，不再走备份 RTV6；重试策略 1M,1M（两个点 = 一共发两轮）用完，失败 DR，RTV6 只收到 1 次；最多等（ms） `240000` |

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR、CDR、DB）；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/RC014_levels_used_up_retry_main`（换配置并重启模块）。
