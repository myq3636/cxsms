# RC008 Relay：权重 70/30、轮流、; 后面不生效

**对应用例清单**：H-01 H-02 H-03 H-04（`features/01_路由重构/路由重构测试用例清单.md`）

**目的**：Relay 的三种写法：权重按比例随机、逗号轮流（每条规则自己计数）、`;` 后面的部分不生效（加载时 WARN）。

## 配置

`routing/RTCUST_routingInfo.cfg`：

```text
CCBSSID:9801
A2PMainRouting:(Priority=10,PhonePrefix=86,Relay=RTV1:70,RTV2:30);(Priority=10,PhonePrefix=64,Relay=RTV3,RTV4,RTV5);(Priority=10,PhonePrefix=27,Relay=RTV6;RTV7);(Priority=10,PhonePrefix=44,Relay=RTV8:100,RTV9:0)
```

## 步骤和预期

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|
| 1 | OTP → 8613800000801 | rtv1 或 rtv2 | `by main prefix=86 priority=10` | DELIVRD | H-01：200 条（同一号码，一个 Core 处理），比例 70/30 左右；条数 `200`；比例 `rtv1:70,rtv2:30,tolerance=10` |
| 2 | OTP → 64210000802 | rtv3 或 rtv4 或 rtv5 | `by main prefix=64 priority=10` | DELIVRD | H-02：9 条（同一号码），轮流各 3 条；条数 `9`；比例 `rtv3:33.3,rtv4:33.3,rtv5:33.3,tolerance=0.1` |
| 3 | OTP → 27820000803 | rtv6 | `by main prefix=27 priority=10` | DELIVRD | H-03：Relay=RTV6;RTV7 只用 RTV6 |
| 4 | OTP → 44770000804 | rtv8 | `by main prefix=44 priority=10` | DELIVRD | H-04：20 条，权重 0 的 RTV9 一条也收不到；条数 `20`；比例 `rtv8:100,rtv9:0,tolerance=0` |

**用例级检查**（这次运行期间）：

- `CoreEngine1` 日志里有一行同时包含 `Relay=RTV6;RTV7: the part after ';' is not used`

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR），用例级检查全部通过；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致；用例级检查的结果（实际取到的值、命令输出）在 `summary.txt` 的 `case checks` 下。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/RC008_relay_weight_turn`（换配置并重启模块）。
