# LS002 长短信没集齐：CsmIntegrityCacheTimeout 到期后过期扫描给已收到的每段回失败 DR，不外发，合并缓冲删掉

**对应用例清单**：流程说明第 4 节（没集齐就过期）；配置项说明 CsmIntegrityCacheTimeout、CoreCsmExpirySweep（`features/08_长短信/长短信处理流程说明.md` 第 8 节、`长短信CDR与DR测试报告.md`）

**目的**：3 段的长短信只发第 1、3 段：30 秒后过期扫描给这 2 段各回一条失败 DR（客户支持过期 DR 时 EXPIRED，否则 UNDELIV），通道收不到；`csm:` 合并缓冲删掉。同时发一条完整的 3 段作对照，确认 30 秒的设置不影响正常的长短信。

## 配置

`routing/RTCUST_routingInfo.cfg`：

```text
CCBSSID:9801
A2PMainRouting:(Priority=10,PhonePrefix=8613,Relay=RTVL1);(Priority=10,PhonePrefix=8615,Relay=RTVL2);(Priority=10,PhonePrefix=8616,Relay=RTVL3)
```

`cm.extra.cfg`（追加到 base/cm.cfg）：RTVL1, RTVL2, RTVL3。

`gmms.append/GmmsConfig.properties`（运行时追加到 Gmms/GmmsConfig.properties（后写的生效），结束后恢复）：

```text
CsmIntegrityCacheTimeout=30000
CoreCsmExpirySweep=true
CoreCsmExpiryIntervalSec=5
```

## 步骤和预期

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|
| 1 | OTP → 8613800009101 | none |  | !DELIVRD | 只发第 1、3 段：过期，2 条失败 DR；段数 `3`；dropSeg `2`；客户收到的 DR 条数 `2`；drEachSeg `true`；最多等（ms） `120000` |
| 2 | OTP → 8613800009102 | rtvl1 |  | DELIVRD | 对照：完整的 3 段照常外发；段数 `3`；通道收到的段 `rtvl1:3`；客户收到的 DR 条数 `3`；drEachSeg `true`；最多等（ms） `120000` |

**用例级检查**（这次运行期间）：

- 命令 `$RTREDIS --scan --pattern 'csm:9801:OTP:8613800009101:*' | wc -l` 的输出等于 `0`
- 命令 `$RTREDIS --scan --pattern 'csm:ids:9801:OTP:8613800009101:*' | wc -l` 的输出等于 `0`

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR），用例级检查全部通过；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致；用例级检查的结果（实际取到的值、命令输出）在 `summary.txt` 的 `case checks` 下。

## 注意

客户 RTCUST 把一条 368 个 ASCII 字符的消息分成 3 段（每段最多 134 字节）发：默认用 UDH（esm_class 0x40），`sar` 时用 SAR 可选参数。通道 RTVL1 按 ASCII 160 拆（带 UDH 每段 153 字符，3 段），RTVL2 按 100 拆（4 段），RTVL3 用 SAR 可选参数（`ChlUdhConcatenated:no`）。测试工具扮演通道，按 UDH 或 SAR 参数收段、拼起来和原文比较；每段外发都回一个成功的 DR。用例把 `CsmIntegrityCacheTimeout` 改成 30 秒、过期扫描间隔 5 秒（追加到 GmmsConfig，结束后恢复）。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/LS002_long_mt_expiry`（换配置并重启模块）。
