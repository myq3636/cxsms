# RC019 长短信：第 1 段失败改道，整条按新通道重新拆分；后面的段跟第 1 段的通道和级别

**对应用例清单**：O-01 O-03 O-05（`features/01_路由重构/路由重构测试用例清单.md`）

**目的**：3 段的长短信（UDH），主路由的通道 RTV1 回第 1 段失败：整条改道到备份 RTV2，按 RTV2 重新拆分发出，所有段都在 RTV2，客户收到 DELIVRD。

## 配置

`routing/RTCUST_routingInfo.cfg`：

```text
CCBSSID:9801
A2PMainRouting:(Priority=10,PhonePrefix=86,Relay=RTV1)
A2PBackupRouting:(Priority=10,PhonePrefix=86,Relay=RTV2)
```

通道（测试工具）的行为：

| 通道 | 行为 |
|---|---|
| rtv1（ssid 9811） | submitStatus=0x45 |

（submitStatus：submit_sm_resp 的 command_status，0x45 → 内部 2300、0x0B → 2120、0x58 → 2600；submitStatusSeq：同一条 MT 第几次到这个通道就取第几个，用完后一直用最后一个；dr：DR 的 stat，none = 不回 DR）

## 步骤和预期

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|
| 1 | OTP → 8615000001901 | rtv1>rtv2 | `[MT-REROUTE] level=1 from=9811 to=9812` | DELIVRD | O-01、O-03：长短信 3 段，第 1 段在 RTV1 失败，整条到 RTV2；RTV2 收到全部的段（按内容拼回来和原文一样）；段数 `3`；通道收到的段 `rtv2:3` |

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR）；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/RC019_long_sms_reroute`（换配置并重启模块）。
