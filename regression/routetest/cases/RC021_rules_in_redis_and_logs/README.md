# RC021 Redis 里的实际规则（Rules:{文件}）和选路 / 改道日志的格式

**对应用例清单**：Q-01 Q-02 Q-03 Q-05 C-05 A-02 A-15（`features/01_路由重构/路由重构测试用例清单.md`）

**目的**：Core 启动后 routingInfo{Core}.Rules:{文件} 是内存里实际在用的规则（Relay 为 ssid，_file、_warnings）；[MT-ROUTE] / [MT-REROUTE] / 不改道的日志格式和《路由配置说明》第 8 节一致；MT 的 routingRule 字段。

## 配置

`routing/RTCUST_routingInfo.cfg`：

```text
CCBSSID:9801
A2PNumberRouting:(Priority=1,Number=8613800002199,Relay=RTV3)
A2PMainRouting:(Priority=10,PhonePrefix=86;852,Sender=BANK,Relay=RTV1);(Priority=50,PhonePrefix=86,Relay=RTV1:70,RTV2:30)
A2PBackupRouting:(Priority=10,PhonePrefix=all,Relay=RTV4)
A2PRoutingRelay:(PhonePrefix=86,Relay=RTV9)
```

通道（测试工具）的行为：

| 通道 | 行为 |
|---|---|
| rtv3（ssid 9813） | submitStatus=0x45 |

（submitStatus：submit_sm_resp 的 command_status，0x45 → 内部 2300、0x0B → 2120、0x58 → 2600；submitStatusSeq：同一条 MT 第几次到这个通道就取第几个，用完后一直用最后一个；dr：DR 的 stat，none = 不回 DR）

## 步骤和预期

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|
| 1 | BANK → 8613800002101 | rtv1 | `[MT-ROUTE] ossid=9801 rssid=9811 by main prefix=86 priority=10 file=RTCUST` | DELIVRD | Q-01：选路日志 |
| 2 | OTP → 8613800002199 | rtv3>rtv4 | `by number number=8613800002199 priority=1`<br>`[MT-REROUTE] level=1 from=9813 to=9814 code=2300 attempt=1 by main backup1 prefix=all priority=10 file=RTCUST` | DELIVRD | Q-02：号码层的通道失败，用普通层的备份；改道日志 |

**用例级检查**（这次运行期间）：

- Redis `HGET routingInfoCoreEngine1.Rules:RTCUST _file` 包含 `CCBSSID=9801`
- Redis `HGET routingInfoCoreEngine1.Rules:RTCUST _warnings` 包含 `A2PRoutingRelay`
- Redis `HGET routingInfoCoreEngine1.Rules:RTCUST main:0:852:10` 包含 `"Relay":"9811"`
- Redis `HGET routingInfoCoreEngine1.Rules:RTCUST main:0:86:50` 包含 `"Relay":"9811:70,9812:30"`
- Redis `HGET routingInfoCoreEngine1.Rules:RTCUST main:1:all:10` 包含 `"Source":"RTCUST A2PBackupRouting #1"`
- Redis `HGET routingInfoCoreEngine1.Rules:RTCUST number:0:8613800002199:1` 包含 `"Relay":"9813"`

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR），用例级检查全部通过；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致；用例级检查的结果（实际取到的值、命令输出）在 `summary.txt` 的 `case checks` 下。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/RC021_rules_in_redis_and_logs`（换配置并重启模块）。
