# RC012 备份的通道不存在跳到下一级；走满 10 级；SMSOptionMaxReroute

**对应用例清单**：J-09 J-10 J-15 J-16 J-17 K-09（`features/01_路由重构/路由重构测试用例清单.md`）

**目的**：备份某一级的通道不存在，跳到下一级；10 级备份都能用到（默认 SMSOptionMaxReroute=10）；客户 SMSOptionMaxReroute=1 时改道一次后就按重试处理（重试主路由），重试次数用完回失败 DR。

## 配置

`routing/RTCUST_routingInfo.cfg`：

```text
CCBSSID:9801
A2PMainRouting:(Priority=10,PhonePrefix=86,Relay=RTV1);(Priority=10,PhonePrefix=64,Relay=RTV3);(Priority=10,PhonePrefix=27,Relay=RTV1)
A2PBackupRouting:(Priority=10,PhonePrefix=86,Relay=RTVXX);(Priority=10,PhonePrefix=64,Relay=RTV4);(Priority=10,PhonePrefix=27,Relay=RTVLEN)
A2PBackup2Routing:(Priority=10,PhonePrefix=86,Relay=RTV2);(Priority=10,PhonePrefix=64,Relay=RTV5);(Priority=10,PhonePrefix=27,Relay=RTV2)
A2PBackup3Routing:(Priority=10,PhonePrefix=64,Relay=RTV6)
A2PBackup4Routing:(Priority=10,PhonePrefix=64,Relay=RTV7)
A2PBackup5Routing:(Priority=10,PhonePrefix=64,Relay=RTV8)
A2PBackup6Routing:(Priority=10,PhonePrefix=64,Relay=RTV9)
A2PBackup7Routing:(Priority=10,PhonePrefix=64,Relay=RTV10)
A2PBackup8Routing:(Priority=10,PhonePrefix=64,Relay=RTV11)
A2PBackup9Routing:(Priority=10,PhonePrefix=64,Relay=RTV12)
A2PBackup10Routing:(Priority=10,PhonePrefix=64,Relay=RTV2)
```

`routing/RTCUST5_routingInfo.cfg`：

```text
CCBSSID:9805
A2PMainRouting:(Priority=10,PhonePrefix=86,Relay=RTV1)
A2PBackupRouting:(Priority=10,PhonePrefix=86,Relay=RTV3)
A2PBackup2Routing:(Priority=10,PhonePrefix=86,Relay=RTV2)
```

`cm.extra.cfg`（追加到 base/cm.cfg）：RTCUST5, RTVLEN。

通道（测试工具）的行为：

| 通道 | 行为 |
|---|---|
| rtv1（ssid 9811） | submitStatus=0x45 |
| rtv3（ssid 9813） | submitStatus=0x45 |
| rtv4（ssid 9814） | submitStatus=0x45 |
| rtv5（ssid 9815） | submitStatus=0x45 |
| rtv6（ssid 9816） | submitStatus=0x45 |
| rtv7（ssid 9817） | submitStatus=0x45 |
| rtv8（ssid 9818） | submitStatus=0x45 |
| rtv9（ssid 9819） | submitStatus=0x45 |
| rtv10（ssid 9820） | submitStatus=0x45 |
| rtv11（ssid 9821） | submitStatus=0x45 |
| rtv12（ssid 9822） | submitStatus=0x45 |

（submitStatus：submit_sm_resp 的 command_status，0x45 → 内部 2300、0x0B → 2120、0x58 → 2600；submitStatusSeq：同一条 MT 第几次到这个通道就取第几个，用完后一直用最后一个；dr：DR 的 stat，none = 不回 DR）

## 步骤和预期

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|
| 1 | OTP → 8615000001201 | rtv1>rtv2 | `channel -1 not in cm.cfg, next level`<br>`[MT-REROUTE] level=2 from=9811 to=9812` | DELIVRD | J-09：备份 1 的 RTVXX 不存在，跳到备份 2 的 RTV2 |
| 2 | OTP → 64210001202 | rtv3>rtv4>rtv5>rtv6>rtv7>rtv8>rtv9>rtv10>rtv11>rtv12>rtv2 | `[MT-REROUTE] level=10 from=9822 to=9812` | DELIVRD | J-16、J-17：主路由 + 10 级备份，前 10 个都失败，第 10 级 RTV2 成功（默认 SMSOptionMaxReroute=10，第 10 次改道允许） |
| 3 | OTP → 8615000001203 | rtv1>rtv3>rtv1 | `backup levels used up (level 1, rssid 9813): retried on the main route rssid 9811`<br>`rerouted 1 times already, SMSOptionMaxReroute=1` | !DELIVRD | J-15、K-09：RTCUST5 只能改道 1 次：RTV1 → RTV3；RTV3 失败后不再改道，按重试策略（1M,1M 两个点 = 一共发两轮）1 分钟后重试主路由 RTV1；RTV1 再失败，改道次数已满、重试用完，回失败 DR；客户 `rtcust5`；最多等（ms） `240000` |
| 4 | OTP → 27820001204 | rtv1>rtv2 | `rssid=9840: content length not allowed by channel 9840, next level`<br>`[MT-REROUTE] level=2 from=9811 to=9812` | DELIVRD | J-10：备份 1 的 RTVLEN 不允许这么长的内容，跳到备份 2；内容 `this content is longer than the fifty bytes RTVLEN allows` |

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR）；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/RC012_backup_unusable_max_10levels`（换配置并重启模块）。
