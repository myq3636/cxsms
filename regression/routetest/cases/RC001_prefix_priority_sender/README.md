# RC001 主路由：最长前缀、组内优先级、发送方、all

对应《路由配置说明》例 1。路由（`routing/RTCUST_routingInfo.cfg`）：

| 规则 | 通道 |
|---|---|
| `(Priority=50,PhonePrefix=86)` | RTV1 |
| `(Priority=50,PhonePrefix=86135;86136;86137;86138;86139)` | RTV2 |
| `(Priority=10,PhonePrefix=86;86135…86139,Sender=BANK)` | RTV3 |
| `(Priority=50,PhonePrefix=852)` | RTV4 |
| `(Priority=50,PhonePrefix=all)` | RTV9 |

| MT | 发送方 → 号码 | 预期通道 | 为什么 |
|---|---|---|---|
| 1 | OTP → 8613800000001 | RTV2 | 最长前缀 86138，组内只有 Priority 50 |
| 2 | BANK → 8613800000002 | RTV3 | 86138 组里 BANK 的 Priority 10 先于 50 |
| 3 | BANK → 8615000000003 | RTV3 | 没有 86150 组，退到 86，BANK 的 Priority 10 |
| 4 | bank → 8615000000004 | RTV3 | 发送方不区分大小写 |
| 5 | OTP → 8615000000005 | RTV1 | 86 组 Priority 50 |
| 6 | OTP → 85261234567 | RTV4 | 852（这台机器的 recipientRuleFile 拦了 8521～8523 开头的香港号码，所以用 8526） |
| 7 | OTP → 447700900007 | RTV9 | all |

每条 MT 都核对：submit_sm_resp、哪个通道收到、Core 的 `[MT-ROUTE]`（层、前缀、优先级）、客户收到的 DR、CDR 的 `OUT_SUBMIT`。

运行：`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/RC001_prefix_priority_sender`。结果在 `result/{时间}/`：`summary.txt`、`messages.csv`、`evidence/M{n}_{msgId}.log`（这条 MT 在每个模块的日志和 CDR），`config/`（这次用的 cm.cfg 和路由文件）。
