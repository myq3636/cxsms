# RC009 主路由选不出能用的通道时，选路时就从备份找：长度不允许、通道不存在、没有主路由规则；备份也没有才路由失败

**对应用例清单**：I-01 I-02 I-03 I-06 Q-04（`features/01_路由重构/路由重构测试用例清单.md`）

**目的**：主路由命中了但通道不存在 / 内容长度不允许，或者主路由一条规则都没命中：在选路时从备份 1 级找；备份也没有：路由失败，回失败 DR。

## 配置

`routing/RTCUST_routingInfo.cfg`：

```text
CCBSSID:9801
A2PMainRouting:(Priority=10,PhonePrefix=8613,Relay=RTVLEN);(Priority=10,PhonePrefix=8615,Relay=RTVXX);(Priority=10,PhonePrefix=44,Relay=RTV1);(Priority=10,PhonePrefix=852,Relay=RTV4:50,RTVXX:50)
A2PBackupRouting:(Priority=10,PhonePrefix=86,Relay=RTV2);(Priority=10,PhonePrefix=64,Relay=RTV3);(Priority=10,PhonePrefix=852,Relay=RTV5)
```

`cm.extra.cfg`（追加到 base/cm.cfg）：RTVLEN。

## 步骤和预期

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|
| 1 | OTP → 8613800000901 | rtvlen | `by main prefix=8613 priority=10` | DELIVRD | 短内容，主路由的 RTVLEN 允许；内容 `hi` |
| 2 | OTP → 8613800000902 | rtv2 | `content length not allowed by channel 9840`<br>`by main backup1 prefix=86 priority=10` | DELIVRD | I-02：内容超过 50 字节，RTVLEN 不允许，选路时用备份 1，routingLevel=1；内容 `this content is longer than the fifty bytes RTVLEN allows` |
| 3 | OTP → 8615000000903 | rtv2 | `channel -1 not in cm.cfg`<br>`by main backup1 prefix=86 priority=10` | DELIVRD | I-01：主路由的通道 RTVXX 不在 cm.cfg，用备份 1（WARN） |
| 4 | OTP → 64210000904 | rtv3 | `no main routing rule matched`<br>`by main backup1 prefix=64 priority=10` | DELIVRD | I-06：没有 64 的主路由规则，用备份 1 |
| 5 | OTP → 27820000905 | none | `no backup route either` | !DELIVRD | 没有主路由，也没有备份：路由失败，失败 DR |
| 6 | OTP → 85268000906 | rtv4 或 rtv5 |  | DELIVRD | I-03：40 条，Relay=RTV4:50,RTVXX:50，抽到不存在的 RTVXX 时不换 RTV4，而是用备份 RTV5：两个都收到，各一半左右；条数 `40`；比例 `rtv4:50,rtv5:50,tolerance=20` |

**用例级检查**（这次运行期间）：

- `CoreEngine1` 日志里有一行同时包含 `Relay channel RTVXX is not in cm.cfg`

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR），用例级检查全部通过；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致；用例级检查的结果（实际取到的值、命令输出）在 `summary.txt` 的 `case checks` 下。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/RC009_main_unusable_backup`（换配置并重启模块）。
