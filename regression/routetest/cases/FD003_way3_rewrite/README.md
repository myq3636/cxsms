# FD003 方式 3（真实 DR 不是 DELIVERED 时按比例改成成功）：比例 100 按前缀、UNDELIV / REJECTD 改写、前缀外和 DELIVRD 不改、外发失败不改、比例 70

**对应用例清单**：FN-07（`features/03_假DR/假DR逻辑重新设计.md` 第 11 节）

**目的**：客户 RTCF7：SMSOptionDRSuccessRatio=100、只对 8613；RTCF8：比例 70、全部号码。给客户的 DR 和 IN_DELI_REP 改成 DELIVERED，OUT_DELI_REP 记真实状态。

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
| 1 | OTP → 8613800004201 | rtvf1 | `[DR-REWRITE] ossid=9897 from=10400 to=10000` | DELIVRD | FN-07：UNDELIV 改成 DELIVRD；OUT_DELI_REP 10400、IN_DELI_REP 10000；客户 `rtcf7`；通道 DR 的 stat `UNDELIV`；客户收到的 DR 条数 `1`；CDR MsgID `OUT_DELI_REP={id}@10400;IN_DELI_REP={id}@10000` |
| 2 | OTP → 8613800004202 | rtvf1 | `[DR-REWRITE] ossid=9897 from=` | DELIVRD | FN-07：REJECTD 也改写；客户 `rtcf7`；通道 DR 的 stat `REJECTD`；客户收到的 DR 条数 `1` |
| 3 | OTP → 8615000004203 | rtvf1 | `![DR-REWRITE]` | UNDELIV | FN-07：8615 不在 SMSOptionDRSuccessRatioPrefix 里，不改；客户 `rtcf7`；通道 DR 的 stat `UNDELIV`；客户收到的 DR 条数 `1` |
| 4 | OTP → 8613800004204 | rtvf1 | `![DR-REWRITE]` | DELIVRD | 真实 DR 本来就是 DELIVRD，不用改；客户 `rtcf7`；客户收到的 DR 条数 `1` |
| 5 | OTP → 8619000004205 | rtvf5 | `![DR-REWRITE]` | !DELIVRD | 第 2 节：外发失败的 DR 不是真实 DR，不改写；客户 `rtcf7`；客户收到的 DR 条数 `1` |
| 6 | OTP → 8613800004206 | rtvf1 |  | DELIVRD | FN-07：比例 70，100 条 UNDELIV 里约 70 条改成 DELIVRD（容差 12 个百分点）；改写的那些有 `[DR-REWRITE]`，没改的没有，所以不按条检查日志；客户 `rtcf8`；通道 DR 的 stat `UNDELIV`；条数 `100`；DR 的比例 `DELIVRD:70,tolerance=12` |

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR、CDR、DB）；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致。

## 注意

所有 FD 用例共用这些账户：通道 RTVF1～RTVF6（9911～9916），客户 RTCF1～RTCF11（9891～9901，各自的假 DR 配置见 cm.extra.cfg），路由都在 `RTSVCF_default`（客户的 CustomerServiceType 是 RTSVCF）。`drCount` 的 MT 要等满 `waitMs`，确认之后没有第二条 DR。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/FD003_way3_rewrite`（换配置并重启模块）。
