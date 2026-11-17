# RC022 路由文件的加载和校验：属性、备份各级配置名、不生效的配置名 WARN、每种写错的文件只有它自己加载失败

**对应用例清单**：A-01 A-03 A-04 A-05 A-06 A-07 A-09 A-10 A-11 A-12 A-13 A-14 A-16（`features/01_路由重构/路由重构测试用例清单.md`）

**目的**：模块启动时加载 8 个路由文件：RTCA01 写了所有属性、备份 1～10 级、运营商层和二次层的备份、第 11 级和旧配置名；其余 7 个各有一种错误。不发消息，只看 Core 日志和 Redis 里实际在用的规则。

## 配置

`routing/RTCA01_routingInfo.cfg`：

```text
CCBSSID:9861
A2PMainRouting:(Priority=10,PhonePrefix=86,Sender=BANK;PAY,ExcludeSender=OTP,KeywordId=RTK1,ExcludeKeywordId=RTK2,ExcludePhonePrefix=8615,Relay=RTV1);(Priority=20,Relay=RTV2);(Priority=30,PhonePrefix=all,Relay=RTV3);(Priority=10,PhonePrefix=44,Relay=RTV4:100,RTV5:0)
A2PBackupRouting:(Priority=10,PhonePrefix=86,Relay=RTV6)
A2PBackup2Routing:(Priority=10,PhonePrefix=86,Relay=RTV7)
A2PBackup3Routing:(Priority=10,PhonePrefix=86,Relay=RTV7)
A2PBackup4Routing:(Priority=10,PhonePrefix=86,Relay=RTV7)
A2PBackup5Routing:(Priority=10,PhonePrefix=86,Relay=RTV7)
A2PBackup6Routing:(Priority=10,PhonePrefix=86,Relay=RTV7)
A2PBackup7Routing:(Priority=10,PhonePrefix=86,Relay=RTV7)
A2PBackup8Routing:(Priority=10,PhonePrefix=86,Relay=RTV7)
A2PBackup9Routing:(Priority=10,PhonePrefix=86,Relay=RTV7)
A2PBackup10Routing:(Priority=10,PhonePrefix=86,Relay=RTV7)
A2POPBackup3Routing:(Priority=10,Opid=4601,Relay=RTV8)
A2PSecBackup2Routing:(Priority=10,PhonePrefix=86,Relay=RTV9)
A2PBackup11Routing:(Priority=10,PhonePrefix=86,Relay=RTV9)
A2PServiceTypeIDRoutingRelay:(PhonePrefix=86,Relay=RTV9)
A2PContentRoutingRelay:(PhonePrefix=86,Relay=RTV9)
A2PSenderAddrRoutingRelay:(PhonePrefix=86,Relay=RTV9)
A2PContentRouting:(PhonePrefix=86,Relay=RTV9)
```

`routing/RTCA05_routingInfo.cfg`：

```text
CCBSSID:9862
A2PMainRouting:(PhonePrefix=86,Relay=RTV1)
```

`routing/RTCA06A_routingInfo.cfg`：

```text
CCBSSID:9863
A2PMainRouting:(Priority=0,PhonePrefix=86,Relay=RTV1)
```

`routing/RTCA06B_routingInfo.cfg`：

```text
CCBSSID:9864
A2PMainRouting:(Priority=101,PhonePrefix=86,Relay=RTV1)
```

`routing/RTCA06C_routingInfo.cfg`：

```text
CCBSSID:9865
A2PMainRouting:(Priority=abc,PhonePrefix=86,Relay=RTV1)
```

`routing/RTCA07_routingInfo.cfg`：

```text
CCBSSID:9866
A2PMainRouting:(Priority=10,PhonePrefix=86)
```

`routing/RTCA11_routingInfo.cfg`：

```text
CCBSSID:9867
A2PMainRouting:(Priority=10,PhonePrefix=86,Sender=BANK,ExcludeSender=bank,Relay=RTV1)
```

`routing/RTCA12_routingInfo.cfg`：

```text
CCBSSID:9868
A2PMainRouting:(Priority=10,PhonePrefix=86,ExcludePhonePrefix=44,Relay=RTV1)
```

`cm.extra.cfg`（追加到 base/cm.cfg）：RTCA01, RTCA05, RTCA06A, RTCA06B, RTCA06C, RTCA07, RTCA11, RTCA12。

## 步骤和预期

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|

**用例级检查**（这次运行期间）：

- `CoreEngine1` 日志里有一行同时包含 `routing file RTCA05`、`A2PMainRouting #1: no Priority (1-100)`
- `CoreEngine1` 日志里有一行同时包含 `routing file RTCA06A`、`A2PMainRouting #1: Priority=0 is not 1-100`
- `CoreEngine1` 日志里有一行同时包含 `routing file RTCA06B`、`A2PMainRouting #1: Priority=101 is not 1-100`
- `CoreEngine1` 日志里有一行同时包含 `routing file RTCA06C`、`A2PMainRouting #1: Priority=abc is not a number (1-100)`
- `CoreEngine1` 日志里有一行同时包含 `routing file RTCA07`、`A2PMainRouting #1: no Relay`
- `CoreEngine1` 日志里有一行同时包含 `routing file RTCA11`、`bank is both in Sender and in ExcludeSender`
- `CoreEngine1` 日志里有一行同时包含 `routing file RTCA12`、`ExcludePhonePrefix=44 is not a part of PhonePrefix`
- Redis `HGET routingInfoCoreEngine1.Rules:RTCA01 main:0:86:10` 包含 `"ExcludePhonePrefix":"8615"`
- Redis `HGET routingInfoCoreEngine1.Rules:RTCA01 main:0:86:10` 包含 `"ExcludeSender":"otp"`
- Redis `HGET routingInfoCoreEngine1.Rules:RTCA01 main:0:86:10` 包含 `"Sender":"bank;pay"`
- Redis `HGET routingInfoCoreEngine1.Rules:RTCA01 main:0:86:10` 包含 `"Relay":"9811"`
- Redis `HGET routingInfoCoreEngine1.Rules:RTCA01 main:0:all:20` 包含 `"Relay":"9812"`
- Redis `HGET routingInfoCoreEngine1.Rules:RTCA01 main:0:all:30` 包含 `"Relay":"9813"`
- Redis `HGET routingInfoCoreEngine1.Rules:RTCA01 main:0:44:10` 包含 `"Relay":"9814:100,9815:0"`
- Redis `HGET routingInfoCoreEngine1.Rules:RTCA01 main:1:86:10` 包含 `"Relay":"9816"`
- Redis `HGET routingInfoCoreEngine1.Rules:RTCA01 main:2:86:10` 包含 `"Relay":"9817"`
- Redis `HGET routingInfoCoreEngine1.Rules:RTCA01 main:10:86:10` 包含 `"Relay":"9817"`
- Redis `HGET routingInfoCoreEngine1.Rules:RTCA01 op:3:4601:10` 包含 `"Relay":"9818"`
- Redis `HGET routingInfoCoreEngine1.Rules:RTCA01 second:2:86:10` 包含 `"Relay":"9819"`
- Redis `HGET routingInfoCoreEngine1.Rules:RTCA01 _warnings` 包含 `A2PBackup11Routing: backup level 11 is not supported`
- Redis `HGET routingInfoCoreEngine1.Rules:RTCA01 _warnings` 包含 `A2PServiceTypeIDRoutingRelay: the old routing config name is not used any more`
- Redis `HGET routingInfoCoreEngine1.Rules:RTCA01 _warnings` 包含 `A2PContentRoutingRelay: the old routing config name is not used any more`
- Redis `HGET routingInfoCoreEngine1.Rules:RTCA01 _warnings` 包含 `A2PSenderAddrRoutingRelay: the old routing config name is not used any more`
- Redis `HGET routingInfoCoreEngine1.Rules:RTCA01 _warnings` 包含 `A2PContentRouting: the old routing config name is not used any more`
- Redis 里没有 `routingInfoCoreEngine1.Rules:RTCA05`（这个文件没有加载成功）
- Redis 里没有 `routingInfoCoreEngine1.Rules:RTCA06A`（这个文件没有加载成功）
- Redis 里没有 `routingInfoCoreEngine1.Rules:RTCA06B`（这个文件没有加载成功）
- Redis 里没有 `routingInfoCoreEngine1.Rules:RTCA06C`（这个文件没有加载成功）
- Redis 里没有 `routingInfoCoreEngine1.Rules:RTCA07`（这个文件没有加载成功）
- Redis 里没有 `routingInfoCoreEngine1.Rules:RTCA11`（这个文件没有加载成功）
- Redis 里没有 `routingInfoCoreEngine1.Rules:RTCA12`（这个文件没有加载成功）

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR），用例级检查全部通过；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致；用例级检查的结果（实际取到的值、命令输出）在 `summary.txt` 的 `case checks` 下。

## 注意

每个路由文件对应 cm.extra.cfg 里同名的客户（RTCA01 9861，RTCA05～RTCA12 9862～9868）。RTCA05～RTCA12 这些文件在启动时加载失败，客户没有路由（启动时没有“原来的路由”可以保留）。错误的原文出自 `RuleSet.parse`；如果日志里的文件名写法不同（例如带 `_routingInfo.cfg`），第一次执行时按 evidence 修正检查文本。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/RC022_load_validation`（换配置并重启模块）。
