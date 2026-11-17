# LS001 长短信：UDH / SAR 两种发法、通道不同的拆法（3 段、4 段、SAR）、每种 CDR 的条数、客户每段一条 DR

**对应用例清单**：流程说明第 8 节；测试报告第 2 节 A、B、C；第 3 节的修复（`features/08_长短信/长短信处理流程说明.md` 第 8 节、`长短信CDR与DR测试报告.md`）

**目的**：一条长短信从 Server 到 DR 回到客户：Core 按段写 IN_SUBMIT_PARTIAL、IN_SUBMIT，拼好后按通道重新拆分，每条外发一条 OUT_SUBMIT、每个通道 DR 一条 OUT_DELI_REP；通道拆过的只有第 1 条的 DR 回客户，按客户的 3 段拆成 3 条，每段的 InMsgID 各一条（第 3 节修复前只有第 1 段）。

## 配置

`routing/RTCUST_routingInfo.cfg`：

```text
CCBSSID:9801
A2PMainRouting:(Priority=10,PhonePrefix=8613,Relay=RTVL1);(Priority=10,PhonePrefix=8615,Relay=RTVL2);(Priority=10,PhonePrefix=8616,Relay=RTVL3)
```

`cm.extra.cfg`（追加到 base/cm.cfg）：RTVL1, RTVL2, RTVL3。

## 步骤和预期

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|
| 1 | OTP → 8613800009001 | rtvl1 | `[MT-ROUTE] ossid=9801 rssid=9981` | DELIVRD | 3 段 UDH，通道按 160 拆回 3 段；段数 `3`；通道收到的段 `rtvl1:3`；客户收到的 DR 条数 `3`；drEachSeg `true`；cdrCount `IN_SUBMIT_PARTIAL:3,IN_SUBMIT:3,OUT_SUBMIT:3,OUT_DELI_REP:3,IN_DELI_REP:3` |
| 2 | OTP → 8613800009002 | rtvl1 | `[MT-ROUTE] ossid=9801 rssid=9981` | DELIVRD | 3 段 SAR 可选参数（Server 解析出参考号、总段数、段号），结果同上；段数 `3`；sar `true`；通道收到的段 `rtvl1:3`；客户收到的 DR 条数 `3`；drEachSeg `true`；cdrCount `IN_SUBMIT_PARTIAL:3,IN_SUBMIT:3,OUT_SUBMIT:3,OUT_DELI_REP:3,IN_DELI_REP:3` |
| 3 | OTP → 8615000009003 | rtvl2 | `[MT-ROUTE] ossid=9801 rssid=9982` | DELIVRD | 测试报告 B：通道按 100 拆成 4 段（每段 100 字节含 6 字节 UDH：94、94、94、86）：OUT_SUBMIT、OUT_DELI_REP 各 4 条，客户仍是 3 条 DR；段数 `3`；通道收到的段 `rtvl2:4`；segLens `94,94,94,86`；客户收到的 DR 条数 `3`；drEachSeg `true`；cdrCount `IN_SUBMIT_PARTIAL:3,IN_SUBMIT:3,OUT_SUBMIT:4,OUT_DELI_REP:4,IN_DELI_REP:3` |
| 4 | OTP → 8616000009004 | rtvl3 | `[MT-ROUTE] ossid=9801 rssid=9983` | DELIVRD | 通道 ChlUdhConcatenated:no：外发用 SAR 可选参数；段数 `3`；通道收到的段 `rtvl3:3`；客户收到的 DR 条数 `3`；drEachSeg `true`；cdrCount `IN_SUBMIT_PARTIAL:3,IN_SUBMIT:3,OUT_SUBMIT:3,OUT_DELI_REP:3,IN_DELI_REP:3` |
| 5 | OTP → 8613800009005 | rtvl1 | `[MT-ROUTE] ossid=9801 rssid=9981` | DELIVRD | 测试报告 C（对照）：单条短信；客户收到的 DR 条数 `1`；cdrCount `IN_SUBMIT_PARTIAL:0,IN_SUBMIT:1,OUT_SUBMIT:1,OUT_DELI_REP:1,IN_DELI_REP:1` |

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR）；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致。

## 注意

客户 RTCUST 把一条 368 个 ASCII 字符的消息分成 3 段（每段最多 134 字节）发：默认用 UDH（esm_class 0x40），`sar` 时用 SAR 可选参数。通道 RTVL1 按 ASCII 160 拆（带 UDH 每段 153 字符，3 段），RTVL2 按 100 拆（4 段），RTVL3 用 SAR 可选参数（`ChlUdhConcatenated:no`）。测试工具扮演通道，按 UDH 或 SAR 参数收段、拼起来和原文比较；每段外发都回一个成功的 DR。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/LS001_long_mt_flow`（换配置并重启模块）。
