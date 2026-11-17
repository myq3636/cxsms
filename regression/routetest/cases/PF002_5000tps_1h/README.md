# PF002 高 TPS：5000 TPS 混合流量 1 小时；抓包和 CDR 逐条对账，无多发、无丢失；之后空闲 10 分钟看内存

**对应用例清单**：docs/高TPS一致性和内存测试设计.md（`docs/高TPS一致性和内存测试设计.md`）

**运行**：`bin/run_hightps.sh cases/PF002_5000tps_1h`（抓包 + `rt.FlowCheck` 对账 + `bin/run_case.sh` 的压测和内存采样）。

## 配置

`routing/RTSVCS_default_routingInfo.cfg`（同 MEM001）：

```text
A2PMainRouting:(Priority=10,PhonePrefix=8613,Relay=RTVS1);(Priority=10,PhonePrefix=8615,Relay=RTVS2);(Priority=10,PhonePrefix=8616,Relay=RTVS3)
A2PBackupRouting:(Priority=10,PhonePrefix=8615,Relay=RTVS1)
```

`cm.extra.cfg`（追加到 base/cm.cfg）：RTVS1～3（`SMSOptionOutgoingThrottlingNum:10000`、`WindowSize:1000`）、RTCS1～3（`SMSOptionIncomingThrottlingNum:10000`、`WindowSize:5000`）。

`gmms.append/GmmsConfig.properties`：`ThrottlingControl.SystemIncomingThreshold=20000`。`conf.sed/log4j2.xml`：`com.king` 日志 INFO。

通道（测试工具）：rtvs2 submitStatus=0x45，rtvs3 不回 DR。

## 流量

| MT | 发送方 → 号码 | 客户 | 比例 | 说明 |
|---|---|---|---|---|
| 1 | OTP → 86138… | rtcs1 | 50% | 普通 MT + DR（MultiSmppServer1） |
| 2 | OTP → 86138… | rtcs3 | 15% | 普通 MT + DR（MultiSmppServer2） |
| 3 | OTP → 86150… | rtcs1 | 10% | rtvs2 回 0x45，改道到 rtvs1 |
| 4 | OTP → 86138… | rtcs1 | 10% | 3 段长短信 |
| 5 | OTP → 44770… | rtcs1 | 5% | 没有路由，失败 DR |
| 6 | OTP → 86160… | rtcs2 | 10% | rtvs3 不回 DR，假 DR 方式 4（5 秒） |

5000 TPS × 3600 秒（预热 300 秒），排空 120 秒，空闲 600 秒；每个客户 2 个 bind。

## 预期

- `flow/report.txt`：`RESULT: PASS`（抓包完整，每条 MT 抓包和 CDR 一致，没有丢失和多发，ID 一致）；
- `mem_report.txt`：`MEMORY: PASS`；
- `summary.txt`：`TRAFFIC: PASS`（发出 ≥ 计划的 95%，submit_sm_resp 成功 ≥ 99%）。

2026-11-17 的结果（系统处理不了 5000 TPS、17:56 根分区写满）见 `docs/5000TPS一小时测试瓶颈分析.md`。
