# RC020 RELOAD_ROUTING：一个文件写错只有它失败、保留原来的路由，其他文件照常生效；旧配置名 WARN 不生效

**对应用例清单**：A-08 A-15 A-17 A-18 A-20（`features/01_路由重构/路由重构测试用例清单.md`）

**目的**：第 1 阶段用正确的文件；第 2 阶段把 RTCUST 的文件改成同一前缀两条 Priority 10（写错），新增客户 RTCUST7 的文件（带一行旧配置名），发 RELOAD_ROUTING：RTCUST 保留原来的路由，RTCUST7 的新文件生效、旧配置名只 WARN。

## 配置

`routing/RTCUST_routingInfo.cfg`：

```text
CCBSSID:9801
A2PMainRouting:(Priority=10,PhonePrefix=86,Relay=RTV1)
```

`cm.extra.cfg`（追加到 base/cm.cfg）：RTCUST7。

## 步骤和预期

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|
| 1 | OTP → 8613800002001 | rtv1 | `by main prefix=86 priority=10 file=RTCUST` | DELIVRD | 第 1 阶段：RTCUST 的正确文件 |

**第 2 阶段**：第 1 阶段的 MT 发完 0 秒后，换成 `phase2/routing/` 的路由文件、发 `RELOAD_ROUTING`，再发：

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|
| 1 | OTP → 8613800002002 | rtv1 | `by main prefix=86 priority=10 file=RTCUST` | DELIVRD | A-18：RTCUST 的新文件有错（同一前缀两条 Priority 10），这个文件失败、保留原来的路由 RTV1（不是 RTV3） |
| 2 | OTP → 8613800002003 | rtv2 | `by main prefix=86 priority=10 file=RTCUST7` | DELIVRD | A-17、A-15：RTCUST7 的新文件生效；旧的 A2PRoutingRelay 不生效（不走 RTV5）；客户 `rtcust7` |

**用例级检查**（这次运行期间）：

- `CoreEngine1` 日志里有一行同时包含 `routing file RTCUST not loaded, it keeps its previous routing`
- `CoreEngine1` 日志里有一行同时包含 `have the same Priority=10 for 86`
- `CoreEngine1` 日志里有一行同时包含 `A2PRoutingRelay: the old routing config name is not used any more`
- Redis `HGET routingInfoCoreEngine1.Rules:RTCUST main:0:86:10` 包含 `"Relay":"9811"`
- Redis `HGET routingInfoCoreEngine1.Rules:RTCUST7 main:0:86:10` 包含 `"Relay":"9812"`
- Redis `HGET routingInfoCoreEngine1.Rules:RTCUST7 _warnings` 包含 `A2PRoutingRelay`

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR），用例级检查全部通过；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致；用例级检查的结果（实际取到的值、命令输出）在 `summary.txt` 的 `case checks` 下。

## 注意

第 2 阶段的 RELOAD_ROUTING 由 run_case.sh 发到 state Redis 的 `channel:gmms:control`（和 telnet 7000 的 smsui 发的是同一个命令），不重启模块。RELOAD_ROUTING 只加载有变化的文件。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/RC020_reload_file_failure`（换配置并重启模块）。
