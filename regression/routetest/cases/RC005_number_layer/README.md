# RC005 号码层：完全相等、多个号码、同一号码按优先级、号码层先于前缀、号码带 +

**对应用例清单**：B-02 B-09 C-01 C-02 C-03 C-04（`features/01_路由重构/路由重构测试用例清单.md`）

**目的**：号码路由（A2PNumberRouting）在普通路由之前；号码完全相等才命中；同一号码多条按 Priority；号码层有规则但号码不在里面时看普通层。

## 配置

`routing/RTCUST_routingInfo.cfg`：

```text
CCBSSID:9801
A2PNumberRouting:(Priority=1,Number=8613800000501;8613800000502,Relay=RTV1);(Priority=1,Number=8613800000503,Sender=BANK,Relay=RTV2);(Priority=2,Number=8613800000503,Relay=RTV3);(Priority=1,Number=8613800000599,Relay=RTV4)
A2PMainRouting:(Priority=10,PhonePrefix=86,Sender=BANK,Relay=RTV5);(Priority=50,PhonePrefix=86,Relay=RTV9)
```

## 步骤和预期

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|
| 1 | BANK → 8613800000501 | rtv1 | `by number number=8613800000501 priority=1` | DELIVRD | B-02：号码层先于普通层（普通层 BANK 会走 RTV5） |
| 2 | OTP → 8613800000502 | rtv1 | `by number number=8613800000502 priority=1` | DELIVRD | C-02：同一条规则的第二个号码 |
| 3 | BANK → 8613800000503 | rtv2 | `by number number=8613800000503 priority=1` | DELIVRD | C-03：同一号码 BANK 的 Priority 1 |
| 4 | OTP → 8613800000503 | rtv3 | `by number number=8613800000503 priority=2` | DELIVRD | C-03：其他发送方 Priority 2 |
| 5 | OTP → 8613800000504 | rtv9 | `by main prefix=86 priority=50` | DELIVRD | B-09：号码层有规则，但没有这个号码，看普通层 |
| 6 | OTP → 86138000005010 | rtv9 | `by main prefix=86 priority=50` | DELIVRD | C-01：比规则里的号码多一位，不算命中 |
| 7 | OTP → +8613800000599 | rtv4 | `by number number=8613800000599 priority=1` | DELIVRD | C-04：号码带 +，去掉后比较 |

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR）；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/RC005_number_layer`（换配置并重启模块）。
