# RC007 默认路由：没有自己路由文件的客户、客户规则都不命中时、默认路由的备份

**对应用例清单**：E-08 E-12 G-04 I-06 J-14 A-19 B-01（`features/01_路由重构/路由重构测试用例清单.md`）

**目的**：客户的四层都不命中时看 {业务类型}_default 文件（同样的规则格式）；默认路由也有备份；客户自己的规则被排除后也会落到默认路由。

## 配置

`routing/RTSVC_default_routingInfo.cfg`：

```text
A2PMainRouting:(Priority=10,PhonePrefix=86,Relay=RTV1);(Priority=10,PhonePrefix=64,Relay=RTV2);(Priority=10,PhonePrefix=8615,Relay=RTV6)
A2PBackupRouting:(Priority=10,PhonePrefix=8615,Relay=RTV7)
```

`routing/RTCUST4_routingInfo.cfg`：

```text
CCBSSID:9804
A2PMainRouting:(Priority=10,PhonePrefix=8613,ExcludeSender=BANK,Relay=RTV4);(Priority=10,PhonePrefix=44,Relay=RTV5)
```

`cm.extra.cfg`（追加到 base/cm.cfg）：RTCUST3, RTCUST4。

通道（测试工具）的行为：

| 通道 | 行为 |
|---|---|
| rtv6（ssid 9816） | submitStatus=0x45 |

（submitStatus：submit_sm_resp 的 command_status，0x45 → 内部 2300、0x0B → 2120、0x58 → 2600；submitStatusSeq：同一条 MT 第几次到这个通道就取第几个，用完后一直用最后一个；dr：DR 的 stat，none = 不回 DR）

## 步骤和预期

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|
| 1 | OTP → 8613800000701 | rtv1 | `by main prefix=86 priority=10 file=RTSVC_default` | DELIVRD | RR-05：RTCUST3 没有自己的路由文件，用 RTSVC_default；客户 `rtcust3` |
| 2 | OTP → 64210000702 | rtv2 | `by main prefix=64 priority=10 file=RTSVC_default` | DELIVRD | 默认路由的 64；客户 `rtcust3` |
| 3 | OTP → 44770000703 | none | `no backup route either` | !DELIVRD | I-06：默认路由也没有 44，备份也没有：路由失败，失败 DR；客户 `rtcust3` |
| 4 | OTP → 8613800000704 | rtv4 | `by main prefix=8613 priority=10 file=RTCUST4` | DELIVRD | RTCUST4 用自己的文件；客户 `rtcust4` |
| 5 | BANK → 8613800000705 | rtv1 | `by main prefix=86 priority=10 file=RTSVC_default` | DELIVRD | G-04、E-12：自己的规则排除了 BANK、自己的四层都不命中，看默认路由；客户 `rtcust4` |
| 6 | OTP → 8615000000707 | rtv6>rtv7 | `[MT-REROUTE] level=1 from=9816 to=9817`<br>`by main backup1 prefix=8615 priority=10 file=RTSVC_default` | DELIVRD | J-14：默认路由的通道 RTV6 回 0x45（2300，在重试码里），用默认路由的备份 1；客户 `rtcust3` |

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR）；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/RC007_default_routing`（换配置并重启模块）。
