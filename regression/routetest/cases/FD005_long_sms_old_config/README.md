# FD005 长短信 3 段 + 方式 1 / 方式 2：每段一条 DR、不重复；旧配置 SMSOptionSendFakeDR、SMSOptionSendDRBeforeOutSubmitInDC 不生效并 WARN

**对应用例清单**：FN-13 FN-14（`features/03_假DR/假DR逻辑重新设计.md` 第 11 节）

**目的**：3 段的长短信（UDH），客户收到的 DR 按段算（每段一条）；配了旧配置的客户 / 通道不回假 DR，加载时打 WARN。

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
| 1 | OTP → 8613800004401 | rtvf1 | `[FAKE-DR] mode=IN_SUBMIT` | DELIVRD | FN-13：方式 1 + 长短信：3 条 DR，不重复；客户 `rtcf1`；段数 `3`；通道收到的段 `rtvf1:3`；客户收到的 DR 条数 `3` |
| 2 | OTP → 8616000004402 | rtvf2 | `[FAKE-DR] mode=OUT_SUBMIT` | DELIVRD | FN-13：方式 2 + 长短信：3 条 DR，不重复；客户 `rtcf5`；段数 `3`；通道收到的段 `rtvf2:3`；客户收到的 DR 条数 `3` |
| 3 | OTP → 8613800004403 | rtvf1 | `![FAKE-DR]` | UNDELIV | FN-14：客户配了旧的 SMSOptionSendFakeDR=true，不回假 DR，真实 DR 转发；客户 `rtcf11`；通道 DR 的 stat `UNDELIV`；客户收到的 DR 条数 `1` |

**用例级检查**（这次运行期间）：

- `CoreEngine1` 日志里有一行同时包含 `RTCF11: SMSOptionSendFakeDR is no longer used`
- `CoreEngine1` 日志里有一行同时包含 `RTVF6: SMSOptionSendDRBeforeOutSubmitInDC is no longer supported`

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR），用例级检查全部通过；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致。

## 注意

所有 FD 用例共用这些账户：通道 RTVF1～RTVF6（9911～9916），客户 RTCF1～RTCF11（9891～9901，各自的假 DR 配置见 cm.extra.cfg），路由都在 `RTSVCF_default`（客户的 CustomerServiceType 是 RTSVCF）。`drCount` 的 MT 要等满 `waitMs`，确认之后没有第二条 DR。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/FD005_long_sms_old_config`（换配置并重启模块）。
