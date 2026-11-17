# RC004 排除条件：ExcludeSender（不区分大小写）、退到短前缀和 all、ExcludePhonePrefix、排除的 DEBUG 日志

**对应用例清单**：G-01 G-02 G-03 G-05 G-07 G-11（`features/01_路由重构/路由重构测试用例清单.md`）

**目的**：“除了某几个发送方都走某通道”，被排除的 MT 继续看同组下一条、更短的前缀、all；排除号段；排除时的 DEBUG 日志。

## 配置

`routing/RTCUST_routingInfo.cfg`：

```text
CCBSSID:9801
A2PMainRouting:(Priority=10,PhonePrefix=86,ExcludeSender=BANK;OTP,Relay=RTV1);(Priority=20,PhonePrefix=86,Relay=RTV2);(Priority=10,PhonePrefix=8613,ExcludeSender=PAY,Relay=RTV3);(Priority=10,PhonePrefix=852,ExcludePhonePrefix=85269,Relay=RTV4);(Priority=10,PhonePrefix=64,ExcludeSender=BANK,Relay=RTV5);(Priority=50,PhonePrefix=all,Relay=RTV9)
```

## 步骤和预期

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|
| 1 | PAY → 8620000000401 | rtv1 | `by main prefix=86 priority=10` | DELIVRD | G-01：PAY 不在排除里 |
| 2 | BANK → 8620000000402 | rtv2 | `not matched by ExcludeSender=bank`<br>`by main prefix=86 priority=20` | DELIVRD | G-01、G-11：BANK 被排除，同组下一条；DEBUG 日志写明排除条件 |
| 3 | otp → 8620000000403 | rtv2 | `by main prefix=86 priority=20` | DELIVRD | G-05：排除不区分大小写 |
| 4 | OTP → 8613800000404 | rtv3 | `by main prefix=8613 priority=10` | DELIVRD | 8613 组 |
| 5 | PAY → 8613800000405 | rtv1 | `by main prefix=86 priority=10` | DELIVRD | G-02：8613 排除了 PAY，退到 86（PAY 在 86 的 Priority 10 不被排除） |
| 6 | OTP → 85268000406 | rtv4 | `by main prefix=852 priority=10` | DELIVRD | 852 组 |
| 7 | OTP → 85269000407 | rtv9 | `not matched by ExcludePhonePrefix=85269`<br>`by main prefix=all priority=50` | DELIVRD | G-07：85269 被排除，852 组没有下一条，到 all |
| 8 | BANK → 64210000408 | rtv9 | `by main prefix=all priority=50` | DELIVRD | G-03：64 组只有排除 BANK 的规则，BANK 到 all |
| 9 | OTP → 64210000409 | rtv5 | `by main prefix=64 priority=10` | DELIVRD | 64 组 |

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR）；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/RC004_exclude_sender_prefix`（换配置并重启模块）。
