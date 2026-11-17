# FD002 方式 2（通道 submit_sm_resp 成功就回）：通道开、客户拒绝、通道的前缀和延迟、外发失败、之后的真实 DR（含 EXPIRED，DEF-C9）只记 CDR

**对应用例清单**：FN-05 FN-06（`features/03_假DR/假DR逻辑重新设计.md` 第 11 节）

**目的**：方式 2 配在通道上：RTVF2 全部号码，RTVF3 只对 86170 开头、延迟 3 秒；客户 RTCF6 配了 SMSOptionFakeDRDisable 不接受。

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
| 1 | OTP → 8616000004101 | rtvf2 | `[FAKE-DR] mode=OUT_SUBMIT ossid=9895 rssid=9912`<br>`[DR-SKIP] ossid=9895 status=10400` | DELIVRD | FN-05：外发成功回假 DR；真实 UNDELIV 只记 OUT_DELI_REP；客户 `rtcf5`；通道 DR 的 stat `UNDELIV`；客户收到的 DR 条数 `1`；CDR MsgID `OUT_DELI_REP={id}@10400` |
| 2 | OTP → 8616000004102 | rtvf2 | `![FAKE-DR]` | UNDELIV | FN-05：客户 SMSOptionFakeDRDisable=true，不回假 DR，真实 DR 转发；客户 `rtcf6`；通道 DR 的 stat `UNDELIV`；客户收到的 DR 条数 `1` |
| 3 | OTP → 8617000004103 | rtvf3 | `[FAKE-DR] mode=OUT_SUBMIT ossid=9895 rssid=9913`<br>`delaySec=3` | DELIVRD | FN-05：通道前缀 86170 内，通道延迟 3 秒（`SMSOptionDRDelayTimeMinMaxInSec:3,3`；只写一个数 `3` 是 0～3 秒随机，原有的写法）；客户 `rtcf5`；通道 DR 的 stat `UNDELIV`；客户收到的 DR 条数 `1`；DR 到达（ms，发出后） `2500,9000` |
| 4 | OTP → 8617100004104 | rtvf3 | `![FAKE-DR]` | UNDELIV | FN-05：86171 不在通道前缀里，真实 DR 转发；客户 `rtcf5`；通道 DR 的 stat `UNDELIV`；客户收到的 DR 条数 `1` |
| 5 | OTP → 8616000004105 | rtvf2 | `![FAKE-DR]` | !DELIVRD | FN-05：外发失败（这条 MT 通道回 0x0B，不在重试码里）不回假 DR，回失败 DR；客户 `rtcf5`；通道的 submit_sm_resp `0x0B`；客户收到的 DR 条数 `1` |
| 6 | OTP → 8616000004106 | rtvf2 | `[FAKE-DR] mode=OUT_SUBMIT` | DELIVRD | FN-06、DEF-C9：之后通道回 EXPIRED，只记 OUT_DELI_REP，不回第二条；客户 `rtcf5`；通道 DR 的 stat `EXPIRED`；客户收到的 DR 条数 `1`；CDR MsgID `OUT_DELI_REP={id}` |

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR、CDR、DB）；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致。

## 注意

所有 FD 用例共用这些账户：通道 RTVF1～RTVF6（9911～9916），客户 RTCF1～RTCF11（9891～9901，各自的假 DR 配置见 cm.extra.cfg），路由都在 `RTSVCF_default`（客户的 CustomerServiceType 是 RTSVCF）。`drCount` 的 MT 要等满 `waitMs`，确认之后没有第二条 DR。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/FD002_way2_out_submit`（换配置并重启模块）。
