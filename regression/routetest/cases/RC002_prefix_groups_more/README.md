# RC002 前缀分组：三级前缀、宽泛规则优先级更小、长前缀不命中退到短前缀、多前缀规则、数字发送方完全相等

**对应用例清单**：E-05 E-06 E-07 E-09 E-10 E-11 F-01 F-03 F-06（`features/01_路由重构/路由重构测试用例清单.md`）

**目的**：补充 RC001：三级前缀由长到短、同一前缀里优先级小的先（即使它更宽泛）、长前缀组里没有规则命中时退到更短的前缀、一条规则写多个前缀、数字发送方完全相等（不是前缀）、优先级不要求连续。

## 配置

`routing/RTCUST_routingInfo.cfg`：

```text
CCBSSID:9801
A2PMainRouting:(Priority=30,PhonePrefix=8613,Relay=RTV1);(Priority=30,PhonePrefix=861,Relay=RTV2);(Priority=30,PhonePrefix=86,Relay=RTV3);(Priority=2,PhonePrefix=8619,Relay=RTV6);(Priority=5,PhonePrefix=8619,Sender=BANK,Relay=RTV7);(Priority=10,PhonePrefix=8617,Sender=BANK,Relay=RTV8);(Priority=10,PhonePrefix=64;27,Sender=PAY,Relay=RTV9);(Priority=1,PhonePrefix=8616,Sender=10690001,Relay=RTV10);(Priority=97,PhonePrefix=8616,Relay=RTV11);(Priority=10,PhonePrefix=8618000000212,Relay=RTV4);(Priority=50,PhonePrefix=all,Relay=RTV12)
```

## 步骤和预期

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|
| 1 | OTP → 8613800000201 | rtv1 | `by main prefix=8613 priority=30` | DELIVRD | 三级前缀，最长的 8613 |
| 2 | OTP → 8612000000202 | rtv2 | `by main prefix=861 priority=30` | DELIVRD | 没有 8612 组，退到 861 |
| 3 | OTP → 8620000000203 | rtv3 | `by main prefix=86 priority=30` | DELIVRD | 退到 86 |
| 4 | BANK → 8619000000204 | rtv6 | `by main prefix=8619 priority=2` | DELIVRD | E-05：8619 组里不带条件的 Priority 2 先于 BANK 的 Priority 5 |
| 5 | BANK → 8617000000205 | rtv8 | `by main prefix=8617 priority=10` | DELIVRD | 8617 组 BANK 命中 |
| 6 | OTP → 8617000000206 | rtv2 | `by main prefix=861 priority=30` | DELIVRD | E-06：8617 组只有 BANK 的规则，OTP 退到 861 |
| 7 | PAY → 64210000207 | rtv9 | `by main prefix=64 priority=10` | DELIVRD | E-11：一条规则的两个前缀 64、27 |
| 8 | PAY → 27820000208 | rtv9 | `by main prefix=27 priority=10` | DELIVRD | E-11：同一条规则的 27 |
| 9 | OTP → 64210000209 | rtv12 | `by main prefix=all priority=50` | DELIVRD | F-06：64 只有 PAY 的规则，OTP 到 all |
| 10 | 10690001 → 8616000000210 | rtv10 | `by main prefix=8616 priority=1` | DELIVRD | F-03：数字发送方完全相等 |
| 11 | 106900012 → 8616000000211 | rtv11 | `by main prefix=8616 priority=97` | DELIVRD | F-03：多一位不算命中（不是前缀匹配）；E-09 优先级 97 不连续 |
| 12 | OTP → 8618000000212 | rtv4 | `by main prefix=8618000000212 priority=10` | DELIVRD | E-10：前缀和整个号码一样长，命中 |

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR）；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/RC002_prefix_groups_more`（换配置并重启模块）。
