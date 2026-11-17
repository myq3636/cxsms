# RC006 二次路由：同一号码短时间内第二次发走二次层；二次层不命中往下看

**对应用例清单**：B-07 B-08 J-13（`features/01_路由重构/路由重构测试用例清单.md`）

**目的**：客户开了二次路由：号码第一次按主路由发，之后 300 秒内再发同一号码，先看二次路由层（A2PSecRouting）；二次层没有规则命中时照常看号码层、普通层。

## 配置

`routing/RTCUST2_routingInfo.cfg`：

```text
CCBSSID:9802
A2PSecRouting:(Priority=10,PhonePrefix=8613,Relay=RTV2);(Priority=10,PhonePrefix=8615,Sender=BANK,Relay=RTV3);(Priority=10,PhonePrefix=8616,Relay=RTV4)
A2PSecBackupRouting:(Priority=10,PhonePrefix=8616,Relay=RTV5)
A2PBackupRouting:(Priority=10,PhonePrefix=86,Relay=RTV6)
A2PMainRouting:(Priority=10,PhonePrefix=86,Relay=RTV1)
```

`cm.extra.cfg`（追加到 base/cm.cfg）：RTCUST2。

通道（测试工具）的行为：

| 通道 | 行为 |
|---|---|
| rtv4（ssid 9814） | submitStatus=0x45 |

（submitStatus：submit_sm_resp 的 command_status，0x45 → 内部 2300、0x0B → 2120、0x58 → 2600；submitStatusSeq：同一条 MT 第几次到这个通道就取第几个，用完后一直用最后一个；dr：DR 的 stat，none = 不回 DR）

## 步骤和预期

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|
| 1 | OTP → 8613800000601 | rtv1 | `by main prefix=86 priority=10` | DELIVRD | 第一次：号码不在二次路由缓存里，主路由；之后号码进缓存 |
| 2 | OTP → 8613800000601 | rtv2 | `by second prefix=8613 priority=10` | DELIVRD | B-07：3 秒后同一号码，二次层；前一条之后（ms） `3000` |
| 3 | OTP → 8615000000603 | rtv1 | `by main prefix=86 priority=10` | DELIVRD | 第一次 |
| 4 | OTP → 8615000000603 | rtv1 | `by main prefix=86 priority=10` | DELIVRD | B-08：二次层 8615 只有 BANK 的规则，不命中，往下看主路由；前一条之后（ms） `3000` |
| 5 | BANK → 8615000000605 | rtv1 | `by main prefix=86 priority=10` | DELIVRD | 第一次 |
| 6 | BANK → 8615000000605 | rtv3 | `by second prefix=8615 priority=10` | DELIVRD | B-07：BANK 第二次，二次层 8615；前一条之后（ms） `3000` |
| 7 | OTP → 8616000000607 | rtv1 | `by main prefix=86 priority=10` | DELIVRD | 第一次 |
| 8 | OTP → 8616000000607 | rtv4>rtv5 | `by second prefix=8616 priority=10`<br>`[MT-REROUTE] level=1 from=9814 to=9815 code=2300 attempt=1 by second backup1 prefix=8616` | DELIVRD | J-13：第二次走二次层的 RTV4，RTV4 失败，用二次层的备份 1 RTV5（不是普通层备份 1 的 RTV6）；前一条之后（ms） `3000` |

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR）；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致。

## 注意

二次路由的号码缓存在每个 Core 的内存里（smsOptionNeedSupportSecondRoutingExpiredTime 秒过期）；MT 按 ossid + 发送方 + 号码分到 Core，所以同一号码的两条用同一个发送方，一定是同一个 Core。走了二次路由的 MT 不会刷新缓存。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/RC006_second_routing`（换配置并重启模块）。
