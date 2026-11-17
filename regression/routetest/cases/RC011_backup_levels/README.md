# RC011 逐级备份：每级按自己的规则选、每级只用一次、级别不连续、跳过已经发过的通道

**对应用例清单**：J-01 J-02 J-03 J-04 J-05 J-06 J-07 J-08 G-09（`features/01_路由重构/路由重构测试用例清单.md`）

**目的**：submit 失败（码在通道的 SMSOptionSubmitRetryCode 里）逐级进备份：每级的规则按前缀、优先级、发送方选；V4:50,V5:50 只选一个，失败就进下一级；某级没有命中直接看下一级；Relay 里发过的通道不再选。

## 配置

`routing/RTCUST_routingInfo.cfg`：

```text
CCBSSID:9801
A2PMainRouting:(Priority=10,PhonePrefix=86,Relay=RTV1);(Priority=10,PhonePrefix=44,Relay=RTV7);(Priority=10,PhonePrefix=64,Relay=RTV10);(Priority=10,PhonePrefix=27,Relay=RTV1)
A2PBackupRouting:(Priority=10,PhonePrefix=27,ExcludeSender=BANK,Relay=RTV6);(Priority=20,PhonePrefix=27,Relay=RTV9);(Priority=10,PhonePrefix=86,Sender=BANK,Relay=RTV2);(Priority=20,PhonePrefix=86,Relay=RTV3);(Priority=10,PhonePrefix=44,Sender=PAY,Relay=RTV8);(Priority=10,PhonePrefix=64,Relay=RTV10)
A2PBackup2Routing:(Priority=10,PhonePrefix=86,Relay=RTV4:50,RTV5:50);(Priority=10,PhonePrefix=64,Relay=RTV10:50,RTV11:50)
A2PBackup3Routing:(Priority=10,PhonePrefix=all,Relay=RTV6);(Priority=10,PhonePrefix=44,Relay=RTV9)
```

通道（测试工具）的行为：

| 通道 | 行为 |
|---|---|
| rtv1（ssid 9811） | submitStatus=0x45 |
| rtv2（ssid 9812） | submitStatus=0x45 |
| rtv3（ssid 9813） | submitStatus=0x45 |
| rtv4（ssid 9814） | submitStatus=0x45 |
| rtv5（ssid 9815） | submitStatus=0x45 |
| rtv7（ssid 9817） | submitStatus=0x45 |
| rtv10（ssid 9820） | submitStatus=0x45 |

（submitStatus：submit_sm_resp 的 command_status，0x45 → 内部 2300、0x0B → 2120、0x58 → 2600；submitStatusSeq：同一条 MT 第几次到这个通道就取第几个，用完后一直用最后一个；dr：DR 的 stat，none = 不回 DR）

## 步骤和预期

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|
| 1 | OTP → 8615000001101 | rtv1>rtv3>(rtv4 或 rtv5)>rtv6 | `[MT-REROUTE] level=1 from=9811 to=9813`<br>`level=3`<br>`by main backup3 prefix=all priority=10` | DELIVRD | J-01、J-03、J-04：主 RTV1 失败 → 备份 1 Priority 20（OTP）RTV3 → 备份 2 RTV4 或 RTV5（只选一个）→ 备份 3 RTV6 |
| 2 | BANK → 8615000001102 | rtv1>rtv2>(rtv4 或 rtv5)>rtv6 | `[MT-REROUTE] level=1 from=9811 to=9812` | DELIVRD | J-02：BANK 在备份 1 走 Priority 10 的 RTV2 |
| 3 | OTP → 44770001103 | rtv7>rtv9 | `[MT-REROUTE] level=3 from=9817 to=9819` | DELIVRD | J-05、J-06：备份 1 只有 PAY 的规则不命中、备份 2 没有 44，直接到备份 3 的 44 组 |
| 4 | OTP → 64210001104 | rtv10>rtv11 | `[MT-REROUTE] level=2 from=9820 to=9821` | DELIVRD | J-07、J-08：备份 1 是发过的 RTV10，跳过；备份 2 只会选没发过的 RTV11 |
| 5 | PAY → 27820001105 | rtv1>rtv6 | `[MT-REROUTE] level=1 from=9811 to=9816` | DELIVRD | G-09：备份 1 的排除：PAY 不被排除，走 Priority 10 的 RTV6 |
| 6 | BANK → 27820001106 | rtv1>rtv9 | `[MT-REROUTE] level=1 from=9811 to=9819` | DELIVRD | G-09：BANK 被排除，走 Priority 20 的 RTV9 |

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR）；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/RC011_backup_levels`（换配置并重启模块）。
