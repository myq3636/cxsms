# RC016 DR 改道码：配了 SMSOptionDRRerouteCode 只有列表里的码改道；按细分后的内部状态判断；NotPaid 不改道

**对应用例清单**：L-03 L-16 L-17 L-18（`features/01_路由重构/路由重构测试用例清单.md`）

**目的**：SMSOptionDRRerouteCode 配了列表时只有状态在里面才改道；比较的是 DR 错误码对照表细分后的内部状态（drErrorMapping.cfg）；命中 SMSOptionDRNotPaidCode 的 DR 变成 REJECTED_BYNC，不改道。

## 配置

`routing/RTCUST_routingInfo.cfg`：

```text
CCBSSID:9801
A2PMainRouting:(Priority=10,PhonePrefix=8613,Relay=RTVD6);(Priority=10,PhonePrefix=8615,Relay=RTVD7);(Priority=10,PhonePrefix=64,Relay=RTVD8)
A2PBackupRouting:(Priority=10,PhonePrefix=86,Relay=RTVD2);(Priority=10,PhonePrefix=64,Relay=RTVD2)
```

`cm.extra.cfg`（追加到 base/cm.cfg）：RTVD6, RTVD7, RTVD8, RTVD2。

`conf/drErrorMapping.cfg`（运行时替换 conf 下的文件，结束后恢复）：

```text
[RTVD7]
DRErr: (Stat=UNDELIV,Err=005,Code=104)
```

通道（测试工具）的行为：

| 通道 | 行为 |
|---|---|
| rtvd6（ssid 见 cm.extra.cfg） | dr=UNDELIV，drErr=001 |
| rtvd7（ssid 见 cm.extra.cfg） | dr=UNDELIV，drErr=005 |
| rtvd8（ssid 见 cm.extra.cfg） | dr=UNDELIV，drErr=077 |
| rtvd2（ssid 见 cm.extra.cfg） | dr=DELIVRD |

（submitStatus：submit_sm_resp 的 command_status，0x45 → 内部 2300、0x0B → 2120、0x58 → 2600；submitStatusSeq：同一条 MT 第几次到这个通道就取第几个，用完后一直用最后一个；dr：DR 的 stat，none = 不回 DR）

## 步骤和预期

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|
| 1 | OTP → 8613800001601 | rtvd6>rtvd2 | `[MT-REROUTE] level=1 from=9856 to=9852` | DELIVRD | L-16：RTVD6 的 UNDELIV err:001 是 10400，在列表里：改道 |
| 2 | OTP → 8615000001602 | rtvd7 |  | UNDELIV | L-17：RTVD7 的 err:005 按对照表细分成 104（10405），不在列表 10400 里：不改道（验收时看 evidence 里 Core 的 DR 状态是 10405） |
| 3 | OTP → 64210001603 | rtvd8 |  | !DELIVRD | L-18：err:077 命中 SMSOptionDRNotPaidCode，变成 REJECTED_BYNC：不改道（RTVD8 的列表是 10400，没有 10800；L-18 的前提是“列表里没有这个码”。没配列表时任何失败都改道，欠费也改道） |

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR）；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致。

## 注意

用例运行时用 `conf/drErrorMapping.cfg` 替换服务器上的对照表（只有 RTVD7 一块，没有 [DEFAULT]），结束后恢复；模块重启时加载，不用 RELOAD_DRERROR。RTVD6 的 UNDELIV 没有对照规则，状态保持 UNDELIVERABLE（10400）。验收时看 evidence 里 Core 的 `[DR-ERR] rssid=... code=... status=...` 行：M1 status=10400、M2 status=10405、M3 code=404。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/RC016_dr_reroute_codes`（换配置并重启模块）。
