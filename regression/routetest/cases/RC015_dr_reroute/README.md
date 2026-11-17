# RC015 DR 失败改道：要通道开 NeedDRReroute；逐级；没开、超时、级别用完：失败 DR 回客户；submit 改道后 DR 再改道

**对应用例清单**：L-01 L-02 L-04 L-05 L-06 L-07 L-08 L-14 L-15（`features/01_路由重构/路由重构测试用例清单.md`）

**目的**：供应商回失败 DR：这次发送的通道开了 NeedDRReroute、状态在 SMSOptionDRRerouteCode 里（没配列表 = 任何失败）、在 RerouteDRDelayTimeInSec 内，进下一级，客户这时不收 DR；否则写 OUT_DELI_REP，把失败 DR 回给客户。

## 配置

`routing/RTCUST_routingInfo.cfg`：

```text
CCBSSID:9801
A2PMainRouting:(Priority=10,PhonePrefix=8613,Relay=RTVD1);(Priority=10,PhonePrefix=8615,Relay=RTVD1);(Priority=10,PhonePrefix=64,Relay=RTVD3);(Priority=10,PhonePrefix=44,Relay=RTVD4);(Priority=10,PhonePrefix=27,Relay=RTVD5);(Priority=10,PhonePrefix=8616,Relay=RTV1);(Priority=10,PhonePrefix=8617,Relay=RTV2)
A2PBackupRouting:(Priority=10,PhonePrefix=8613,Relay=RTVD2);(Priority=10,PhonePrefix=8615,Relay=RTVD4);(Priority=10,PhonePrefix=64,Relay=RTVD2);(Priority=10,PhonePrefix=44,Relay=RTVD2);(Priority=10,PhonePrefix=27,Relay=RTVD2);(Priority=10,PhonePrefix=8616,Relay=RTVD1);(Priority=10,PhonePrefix=8617,Relay=RTVD3)
A2PBackup2Routing:(Priority=10,PhonePrefix=8616,Relay=RTVD2);(Priority=10,PhonePrefix=8617,Relay=RTVD2)
```

`cm.extra.cfg`（追加到 base/cm.cfg）：RTVD1, RTVD2, RTVD3, RTVD4, RTVD5。

通道（测试工具）的行为：

| 通道 | 行为 |
|---|---|
| rtvd1（ssid 见 cm.extra.cfg） | dr=UNDELIV |
| rtvd2（ssid 见 cm.extra.cfg） | dr=DELIVRD |
| rtvd3（ssid 见 cm.extra.cfg） | dr=UNDELIV |
| rtvd4（ssid 见 cm.extra.cfg） | dr=UNDELIV |
| rtvd5（ssid 见 cm.extra.cfg） | dr=UNDELIV，drDelayMs=3000 |
| rtv1（ssid 9811） | submitStatus=0x45 |
| rtv2（ssid 9812） | submitStatus=0x45 |

（submitStatus：submit_sm_resp 的 command_status，0x45 → 内部 2300、0x0B → 2120、0x58 → 2600；submitStatusSeq：同一条 MT 第几次到这个通道就取第几个，用完后一直用最后一个；dr：DR 的 stat，none = 不回 DR）

## 步骤和预期

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|
| 1 | OTP → 8613800001501 | rtvd1>rtvd2 | `[MT-REROUTE] level=1 from=9851 to=9852` | DELIVRD | L-01：RTVD1 回 UNDELIV，改道到备份 RTVD2，客户只收到 RTVD2 的 DELIVRD（一个 DR） |
| 2 | OTP → 8615000001502 | rtvd1>rtvd4 | `[MT-REROUTE] level=1 from=9851 to=9854` | UNDELIV | L-02、L-04：RTVD4 也 UNDELIV，没有下一级：失败 DR 回客户，不重试 |
| 3 | OTP → 64210001503 | rtvd3 |  | UNDELIV | L-06：RTVD3 没开 NeedDRReroute：不改道，失败 DR 回客户 |
| 4 | OTP → 44770001504 | rtvd4>rtvd2 | `[MT-REROUTE] level=1 from=9854 to=9852` | DELIVRD | L-07：RTVD4 开了 NeedDRReroute 没配状态列表：任何失败都改道 |
| 5 | OTP → 27820001505 | rtvd5 |  | UNDELIV | L-05：DR 3 秒后才来，超过 RerouteDRDelayTimeInSec=1：不改道 |
| 6 | OTP → 8616000001506 | rtv1>rtvd1>rtvd2 | `[MT-REROUTE] level=1 from=9811 to=9851`<br>`[MT-REROUTE] level=2 from=9851 to=9852` | DELIVRD | L-14：submit 失败改道到 RTVD1（第 1 级），RTVD1 的 DR 失败从第 2 级接着找 |
| 7 | OTP → 8617000001507 | rtv2>rtvd3 | `[MT-REROUTE] level=1 from=9812 to=9853` | UNDELIV | L-15：看这次发送的通道 RTVD3（没开 NeedDRReroute），不进第 2 级 |

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR）；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/RC015_dr_reroute`（换配置并重启模块）。
