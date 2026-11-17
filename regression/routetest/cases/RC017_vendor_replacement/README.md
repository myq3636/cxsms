# RC017 系统路由替换：选通道时替换；“发过”和改道码看替换后的通道；客户级替换优先；只换一次；替换成不存在的通道

**对应用例清单**：M-01 M-02 M-03 M-04 M-05 M-06 M-07 M-08 M-09 P-05 P-06（`features/01_路由重构/路由重构测试用例清单.md`）

**目的**：vendorRoutingReplacement.cfg（前缀:ossid:原通道:替换通道）在选通道时做：主路由和每一级备份都替换；替换后的通道发过了就算发过；客户级（ossid=9801）的替换优先于 -1；V→W 后不再查 W 的替换；替换成不存在的通道时主路由从备份找、备份跳到下一级。

## 配置

`routing/RTCUST_routingInfo.cfg`：

```text
CCBSSID:9801
A2PMainRouting:(Priority=10,PhonePrefix=8613,Relay=RTV1);(Priority=10,PhonePrefix=8615,Relay=RTV3);(Priority=10,PhonePrefix=64,Relay=RTV7)
A2PBackupRouting:(Priority=10,PhonePrefix=8613,Relay=RTV1);(Priority=10,PhonePrefix=64,Relay=RTV8)
A2PBackup2Routing:(Priority=10,PhonePrefix=8613,Relay=RTV3)
```

`conf/vendorRoutingReplacement.cfg`（运行时替换 conf 下的文件，结束后恢复）：

```text
86:-1:9811:9812
86:9801:9813:9814
86:-1:9813:9816
86:-1:9814:9815
64:-1:9817:9999
```

通道（测试工具）的行为：

| 通道 | 行为 |
|---|---|
| rtv2（ssid 9812） | submitStatus=0x45 |

（submitStatus：submit_sm_resp 的 command_status，0x45 → 内部 2300、0x0B → 2120、0x58 → 2600；submitStatusSeq：同一条 MT 第几次到这个通道就取第几个，用完后一直用最后一个；dr：DR 的 stat，none = 不回 DR）

## 步骤和预期

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|
| 1 | OTP → 8613800001701 | rtv2>rtv4 | `rule=9811 replaced=9812`<br>`[MT-REROUTE] level=2 from=9812 to=9814` | DELIVRD | M-01、M-02、M-03：主路由 RTV1 被替换成 RTV2 发出；RTV2 失败（看 RTV2 的重试码）；备份 1 的 RTV1 又被替换成发过的 RTV2，跳过；备份 2 的 RTV3 按客户级替换成 RTV4（M-09，不是 -1 的 RTV6），而且不再按 9814:9815 换（M-08）；CDR MsgID `OUT_SUBMIT={id};OUT_SUBMIT={id}-9814` |
| 2 | OTP → 8615000001702 | rtv4 | `rule=9813 replaced=9814` | DELIVRD | M-09：客户级替换优先 |
| 3 | OTP → 64210001703 | rtv8 | `channel 9999 not in cm.cfg`<br>`by main backup1 prefix=64 priority=10` | DELIVRD | M-06：主路由 RTV7 被替换成不存在的 9999：选路时从备份 1 找 |

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR、CDR、DB）；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致。

## 注意

设计稿 2.8 里“替换表出现链条时加载 WARN”当前代码没有实现，本用例只验证“只换一次”的行为。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/RC017_vendor_replacement`（换配置并重启模块）。
