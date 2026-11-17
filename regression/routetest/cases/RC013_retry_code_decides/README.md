# RC013 改道码：失败码不在这次发送通道的 SMSOptionSubmitRetryCode 里，不改道、不重试，直接失败 DR

**对应用例清单**：K-01 K-02 K-03 K-04（`features/01_路由重构/路由重构测试用例清单.md`）

**目的**：只有失败码在这次发送的通道的 SMSOptionSubmitRetryCode 里才进下一级：主路由的码不在里面（0x0B → 2120）、通道没配重试码（RTVNR）、备份通道的码不在它自己的列表里，都直接回失败 DR，后面的级别收不到。

## 配置

`routing/RTCUST_routingInfo.cfg`：

```text
CCBSSID:9801
A2PMainRouting:(Priority=10,PhonePrefix=86,Relay=RTV1);(Priority=10,PhonePrefix=64,Relay=RTVNR);(Priority=10,PhonePrefix=44,Relay=RTV4)
A2PBackupRouting:(Priority=10,PhonePrefix=86,Relay=RTV2);(Priority=10,PhonePrefix=64,Relay=RTV3);(Priority=10,PhonePrefix=44,Relay=RTVNR)
A2PBackup2Routing:(Priority=10,PhonePrefix=44,Relay=RTV6)
```

通道（测试工具）的行为：

| 通道 | 行为 |
|---|---|
| rtv1（ssid 9811） | submitStatus=0x0B |
| rtvnr（ssid 9830） | submitStatus=0x45 |
| rtv4（ssid 9814） | submitStatus=0x45 |

（submitStatus：submit_sm_resp 的 command_status，0x45 → 内部 2300、0x0B → 2120、0x58 → 2600；submitStatusSeq：同一条 MT 第几次到这个通道就取第几个，用完后一直用最后一个；dr：DR 的 stat，none = 不回 DR）

## 步骤和预期

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|
| 1 | OTP → 8615000001301 | rtv1 | `not rerouted: status 2120 not in SMSOptionSubmitRetryCode of rssid 9811` | !DELIVRD | K-01：RTV1 回 0x0B（2120，不在 2300,2500,2600 里），不进备份 RTV2 |
| 2 | OTP → 64210001302 | rtvnr | `not rerouted: status 2300 not in SMSOptionSubmitRetryCode of rssid 9830` | !DELIVRD | K-04：RTVNR 没配重试码，任何失败都不改道 |
| 3 | OTP → 44770001303 | rtv4>rtvnr | `[MT-REROUTE] level=1 from=9814 to=9830`<br>`status 2300 not in SMSOptionSubmitRetryCode of rssid 9830` | !DELIVRD | K-02、K-03：RTV4 的 2300 在它的重试码里，改道到 RTVNR；RTVNR 的 2300 不在它的列表里（看这次发送的通道），不进备份 2 RTV6 |

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR）；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/RC013_retry_code_decides`（换配置并重启模块）。
