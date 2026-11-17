# RC003 内容关键字：KeywordId、ExcludeKeywordId、发送方+关键字（并且）

**对应用例清单**：F-04 F-05 F-08 G-06 G-08（`features/01_路由重构/路由重构测试用例清单.md`）

**目的**：按内容关键字（contentTemplate.cfg 的模板 id）选路：命中关键字的规则、排除关键字的规则、发送方和关键字同时满足才命中。

## 配置

`routing/RTCUST_routingInfo.cfg`：

```text
CCBSSID:9801
A2PMainRouting:(Priority=10,PhonePrefix=86,KeywordId=RTK1,Relay=RTV1);(Priority=20,PhonePrefix=86,ExcludeKeywordId=RTK2,Relay=RTV2);(Priority=30,PhonePrefix=86,Relay=RTV3);(Priority=10,PhonePrefix=44,Sender=BANK,KeywordId=RTK1,Relay=RTV4);(Priority=20,PhonePrefix=44,Relay=RTV5);(Priority=10,PhonePrefix=64,Sender=BANK,ExcludeKeywordId=RTK1,Relay=RTV6);(Priority=20,PhonePrefix=64,Relay=RTV7)
```

`conf.append/contentTemplate.cfg`（运行时追加到 conf 下的文件，结束后恢复）：

```text
[RTK1]
English:your otp \d+.*

[RTK2]
English:promo .*
```

## 步骤和预期

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|
| 1 | OTP → 8613800000301 | rtv1 | `by main prefix=86 priority=10` | DELIVRD | F-04：内容命中 RTK1；内容 `your otp 1234` |
| 2 | OTP → 8613800000302 | rtv3 | `by main prefix=86 priority=30` | DELIVRD | G-06：内容命中 RTK2，被 Priority 20 排除，到 30；内容 `promo big sale` |
| 3 | OTP → 8613800000303 | rtv2 | `by main prefix=86 priority=20` | DELIVRD | F-08：普通内容，Priority 20（没有排除）；内容 `hello` |
| 4 | BANK → 44770000304 | rtv4 | `by main prefix=44 priority=10` | DELIVRD | F-05：BANK 并且 RTK1；内容 `your otp 5678` |
| 5 | BANK → 44770000305 | rtv5 | `by main prefix=44 priority=20` | DELIVRD | F-05：BANK 但不是 RTK1，不命中；内容 `hello` |
| 6 | OTP → 44770000306 | rtv5 | `by main prefix=44 priority=20` | DELIVRD | F-05：RTK1 但不是 BANK，不命中；内容 `your otp 9` |
| 7 | BANK → 64210000307 | rtv6 | `by main prefix=64 priority=10` | DELIVRD | G-08：BANK 且不是 RTK1；内容 `hello` |
| 8 | BANK → 64210000308 | rtv7 | `by main prefix=64 priority=20` | DELIVRD | G-08：BANK 的验证码被排除；内容 `your otp 1` |

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR）；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/RC003_keyword_conditions`（换配置并重启模块）。
