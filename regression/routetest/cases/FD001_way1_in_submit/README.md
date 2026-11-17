# FD001 方式 1（收到客户消息就回）：全部 / 按前缀、延迟、之后的真实 DR 和外发失败只记 CDR、前缀外的失败照常回（DEF-C8）、比例 0 和 90±5、路由失败仍是失败 DR

**对应用例清单**：FN-01 FN-02 FN-03 FN-04（`features/03_假DR/假DR逻辑重新设计.md` 第 11 节）

**目的**：方式 1 在路由成功后、发给 Client 之前回假 DR，消息照常外发；之后通道的真实 DR、外发失败都只记 CDR。

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
| 1 | OTP → 8613800004001 | rtvf1 | `[FAKE-DR] mode=IN_SUBMIT ossid=9891 rssid=9911 status=10000`<br>`[DR-SKIP] ossid=9891 status=10000` | DELIVRD | FN-02：全部号码，假 DR 成功；通道真实 DR 被跳过；只有 1 条；客户 `rtcf1`；客户收到的 DR 条数 `1`；CDR MsgID `OUT_SUBMIT={id};OUT_DELI_REP={id}@10000` |
| 2 | OTP → 8613800004002 | rtvf1 | `[FAKE-DR] mode=IN_SUBMIT`<br>`[DR-SKIP] ossid=9891 status=10400` | DELIVRD | FN-03：真实 DR 是 UNDELIV，客户仍只有那条成功的假 DR，OUT_DELI_REP 记真实的 10400；客户 `rtcf1`；通道 DR 的 stat `UNDELIV`；客户收到的 DR 条数 `1`；CDR MsgID `OUT_DELI_REP={id}@10400` |
| 3 | OTP → 8619000004003 | rtvf5 | `[FAKE-DR] mode=IN_SUBMIT` | DELIVRD | FN-03：外发失败（0x0B，不重试），客户只有假 DR；客户 `rtcf1`；客户收到的 DR 条数 `1` |
| 4 | OTP → 8613800004004 | rtvf1 | `[FAKE-DR] mode=IN_SUBMIT ossid=9892`<br>`delaySec=` | DELIVRD | FN-02：按前缀 8613，延迟 2～4 秒；客户 `rtcf2`；客户收到的 DR 条数 `1`；DR 到达（ms，发出后） `1500,7000` |
| 5 | OTP → 85268000405 | rtvf1 | `![FAKE-DR]` | UNDELIV | FN-02：852 不在前缀里，不回假 DR，真实 DR 转发；客户 `rtcf2`；通道 DR 的 stat `UNDELIV`；客户收到的 DR 条数 `1` |
| 6 | OTP → 85269000406 | rtvf4 | `![FAKE-DR]` | !DELIVRD | FN-03、DEF-C8：前缀外的号码，可重试的失败（0x45 → 2300）重试到最后一次（策略只有 1 个点）：客户收到失败 DR；客户 `rtcf2`；客户收到的 DR 条数 `1` |
| 7 | OTP → 8613800004007 | rtvf1 | `[FAKE-DR] mode=IN_SUBMIT ossid=9893 rssid=9911 status=10400` | UNDELIV | FN-04：SMSOptionFakeDRSuccessRatio=0，假 DR 全是失败（真实 DR 是成功也不转发）；客户 `rtcf3`；客户收到的 DR 条数 `1` |
| 8 | OTP → 8613800004008 | rtvf1 | `[FAKE-DR] mode=IN_SUBMIT ossid=9894` | DELIVRD | FN-04：比例 90、浮动 5，100 条里成功的约 85%～95%（容差按 10 个百分点，留出抽样误差）；客户 `rtcf4`；条数 `100`；DR 的比例 `DELIVRD:90,tolerance=10` |
| 9 | OTP → 447700004009 | none | `no backup route either`<br>`![FAKE-DR]` | !DELIVRD | FN-01：路由失败的消息到不了方式 1，仍是失败 DR；客户 `rtcf1`；客户收到的 DR 条数 `1` |

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR、CDR、DB）；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致。

## 注意

所有 FD 用例共用这些账户：通道 RTVF1～RTVF6（9911～9916），客户 RTCF1～RTCF11（9891～9901，各自的假 DR 配置见 cm.extra.cfg），路由都在 `RTSVCF_default`（客户的 CustomerServiceType 是 RTSVCF）。`drCount` 的 MT 要等满 `waitMs`，确认之后没有第二条 DR。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/FD001_way1_in_submit`（换配置并重启模块）。
