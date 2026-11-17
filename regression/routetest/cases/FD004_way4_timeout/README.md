# FD004 方式 4（通道 10 秒不回 DR 就自动回）：超时前真实 DR 到、超时回成功、超时后真实 DR 才到只记 CDR、状态 fail

**对应用例清单**：FN-08 FN-09（`features/03_假DR/假DR逻辑重新设计.md` 第 11 节）

**目的**：客户 RTCF9：SMSOptionFakeDRTimeoutSec=10（success）；RTCF10：同样 10 秒，状态 fail。

## 配置

`routing/RTSVCF_default_routingInfo.cfg`：

```text
A2PMainRouting:(Priority=10,PhonePrefix=8613,Relay=RTVF1);(Priority=10,PhonePrefix=8615,Relay=RTVF1);(Priority=10,PhonePrefix=8616,Relay=RTVF2);(Priority=10,PhonePrefix=8617,Relay=RTVF3);(Priority=10,PhonePrefix=852,Relay=RTVF1);(Priority=10,PhonePrefix=85269,Relay=RTVF4);(Priority=10,PhonePrefix=8619,Relay=RTVF5)
```

`cm.extra.cfg`（追加到 base/cm.cfg）：RTVF1, RTVF2, RTVF3, RTVF4, RTVF5, RTVF6, RTCF1, RTCF2, RTCF3, RTCF4, RTCF5, RTCF6, RTCF7, RTCF8, RTCF9, RTCF10, RTCF11。

通道（测试工具）的行为：

| 通道 | 行为 |
|---|---|
| rtvf4（ssid 9914） | submitStatus=0x45 |
| rtvf5（ssid 9915） | submitStatus=0x0B |

（submitStatus：submit_sm_resp 的 command_status，0x45 → 内部 2300、0x0B → 2120、0x58 → 2600；submitStatusSeq：同一条 MT 第几次到这个通道就取第几个，用完后一直用最后一个；dr：DR 的 stat，none = 不回 DR）

## 步骤和预期

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|
| 1 | OTP → 8613800004301 | rtvf1 | `![FAKE-DR]` | UNDELIV | FN-08：真实 DR 0.5 秒就到，转发，计时取消，10 秒后不再回；客户 `rtcf9`；通道 DR 的 stat `UNDELIV`；客户收到的 DR 条数 `1`；DR 到达（ms，发出后） `0,5000` |
| 2 | OTP → 8613800004302 | rtvf1 | `[FAKE-DR] mode=TIMEOUT ossid=9899` | DELIVRD | FN-09：通道不回 DR，约 10 秒后回成功；客户 `rtcf9`；通道 DR 的 stat `none`；客户收到的 DR 条数 `1`；DR 到达（ms，发出后） `9000,16000` |
| 3 | OTP → 8613800004303 | rtvf1 | `[FAKE-DR] mode=TIMEOUT`<br>`[DR-SKIP] outMsgId=` | DELIVRD | FN-09：真实 DR 15 秒后才到：10 秒时回了成功，真实 UNDELIV 只记 OUT_DELI_REP；客户 `rtcf9`；通道 DR 的 stat `UNDELIV`；通道多久回 DR（ms） `15000`；客户收到的 DR 条数 `1`；DR 到达（ms，发出后） `9000,14000`；CDR MsgID `OUT_DELI_REP={id}@10400` |
| 4 | OTP → 8613800004304 | rtvf1 | `[FAKE-DR] mode=TIMEOUT ossid=9900 rssid=9911 status=10400` | UNDELIV | FN-09：SMSOptionFakeDRTimeoutStatus=fail；客户 `rtcf10`；通道 DR 的 stat `none`；客户收到的 DR 条数 `1`；DR 到达（ms，发出后） `9000,16000` |

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR、CDR、DB）；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致。

## 注意

所有 FD 用例共用这些账户：通道 RTVF1～RTVF6（9911～9916），客户 RTCF1～RTCF11（9891～9901，各自的假 DR 配置见 cm.extra.cfg），路由都在 `RTSVCF_default`（客户的 CustomerServiceType 是 RTSVCF）。`drCount` 的 MT 要等满 `waitMs`，确认之后没有第二条 DR。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/FD004_way4_timeout`（换配置并重启模块）。
