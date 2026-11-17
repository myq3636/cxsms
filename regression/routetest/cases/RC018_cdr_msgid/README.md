# RC018 CDR MsgID：主路由 msgId，备份通道 msgId-{ssid}，IN_DELI_REP 总是 msgId

**对应用例清单**：P-01 P-02 P-03 P-04 P-09（`features/01_路由重构/路由重构测试用例清单.md`）

**目的**：逐级改道时每个通道自己的 OUT_SUBMIT 带自己的后缀，之后的 DR 类 CDR 带最后一个通道的后缀，IN_DELI_REP 是 msgId；没改道的 MT 全部是 msgId；失败 DR 的 CDR 带最后发送的通道。

## 配置

`routing/RTCUST_routingInfo.cfg`：

```text
CCBSSID:9801
A2PMainRouting:(Priority=10,PhonePrefix=86,Relay=RTV1);(Priority=10,PhonePrefix=64,Relay=RTV4);(Priority=10,PhonePrefix=44,Relay=RTV5)
A2PBackupRouting:(Priority=10,PhonePrefix=86,Relay=RTV2);(Priority=10,PhonePrefix=44,Relay=RTVNR)
A2PBackup2Routing:(Priority=10,PhonePrefix=86,Relay=RTV3)
```

通道（测试工具）的行为：

| 通道 | 行为 |
|---|---|
| rtv1（ssid 9811） | submitStatus=0x45 |
| rtv2（ssid 9812） | submitStatus=0x45 |
| rtv5（ssid 9815） | submitStatus=0x45 |
| rtvnr（ssid 9830） | submitStatus=0x45 |

（submitStatus：submit_sm_resp 的 command_status，0x45 → 内部 2300、0x0B → 2120、0x58 → 2600；submitStatusSeq：同一条 MT 第几次到这个通道就取第几个，用完后一直用最后一个；dr：DR 的 stat，none = 不回 DR）

## 步骤和预期

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|
| 1 | OTP → 8615000001801 | rtv1>rtv2>rtv3 | `[MT-REROUTE] level=2 from=9812 to=9813` | DELIVRD | P-02、P-03、P-04：OUT_SUBMIT 依次 msgId、msgId-9812、msgId-9813；之后的 OUT_DELI_REP msgId-9813；IN_DELI_REP msgId；CDR MsgID `IN_SUBMIT={id};OUT_SUBMIT={id};OUT_SUBMIT={id}-9812;OUT_SUBMIT={id}-9813;OUT_DELI_REP={id}-9813;IN_DELI_REP={id}` |
| 2 | OTP → 64210001802 | rtv4 | `by main prefix=64 priority=10` | DELIVRD | P-01：没改道，全部 msgId；CDR MsgID `IN_SUBMIT={id};OUT_SUBMIT={id};OUT_DELI_REP={id};IN_DELI_REP={id}` |
| 3 | OTP → 44770001803 | rtv5>rtvnr | `status 2300 not in SMSOptionSubmitRetryCode of rssid 9830` | !DELIVRD | P-09：备份 RTVNR 失败、不再改道，失败 DR 的 CDR 带 -9830；CDR MsgID `OUT_SUBMIT={id};OUT_SUBMIT={id}-9830` |

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR、CDR、DB）；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/RC018_cdr_msgid`（换配置并重启模块）。
