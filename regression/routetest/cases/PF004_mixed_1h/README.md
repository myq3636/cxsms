# PF004 1 小时 5000 TPS：营销 + OTP，通道分开；抓包和 CDR 逐条对账，无多发、无丢失；之后空闲 10 分钟看内存

**运行**：`bin/run_hightps.sh cases/PF004_mixed_1h`（抓包 + `rt.FlowCheck` 对账 + `bin/run_case.sh` 的压测和内存采样）。

## 配置

`cm.extra.cfg`：营销通道 RTVM1（9981）、OTP 通道 RTVO1（9982），都 `SMSOptionOutgoingThrottlingNum:10000`、`WindowSize:1000`；营销客户 RTMK1～5（9971～9975）、OTP 客户 RTOT1～5（9976～9980），`SMSOptionIncomingThrottlingNum:10000`、`WindowSize:5000`。

`routing/RTSVCS_default_routingInfo.cfg`：86138 → RTVM1，86139 → RTVO1。`gmms.append/GmmsConfig.properties`：`ThrottlingControl.SystemIncomingThreshold=20000`。

## 流量

| 客户 | 每个 | 合计 | 连到 | 号码 → 通道 |
|---|---|---|---|---|
| RTMK1～5（营销） | 800 TPS | 4000 TPS | MultiSmppServer1 | 86138 → RTVM1 |
| RTOT1～5（OTP） | 200 TPS | 1000 TPS | MultiSmppServer2 | 86139 → RTVO1 |

5000 TPS × 3600 秒（预热 300 秒），排空 1800 秒，空闲 600 秒；每个客户 2 个 bind。

## 预期

- `flow/report.txt`：`RESULT: PASS`（抓包完整，每条 MT 抓包和 CDR 一致，没有丢失和多发）；
- `mem_report.txt`：`MEMORY: PASS`；
- `summary.txt`：`TRAFFIC: PASS`，按客户的延时（`latency after the warm-up by template`）。
