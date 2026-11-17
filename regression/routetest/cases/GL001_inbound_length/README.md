# GL001 Core 入向长度检查：GSM 7-bit 按 GSM 字符数（≤160），ISO 按字节（≤140），NeedCheckMsgSize:false 不检查，模式 1 里 >0x7F 的字节打 WARN

**对应用例清单**：G7-01 G7-02 G7-03 G7-04 G7-05 G7-05b G7-05c G7-05d G7-06 G7-11 G7-12（`features/07_GSM7与8bit长度/GSM7bit消息长度计算优化设计.md` 第 5、7、8、9 节）

**目的**：真实客户经 Server、Core 到通道的完整链路（回归里是在 Core 这一层构造消息）。通过的 MT 都发往 RTVG1（7-bit 模式 1），通道收到 1 段、data_coding 0、GSM 编码、文本和原文一样；超长的 MT 由 Core 拒绝（OUT_SUBMIT 状态 2250），客户收到失败 DR。

## 配置

`routing/RTSVCG_default_routingInfo.cfg`：

```text
A2PMainRouting:(Priority=10,PhonePrefix=8613,Relay=RTVG1);(Priority=10,PhonePrefix=8615,Relay=RTVG2);(Priority=10,PhonePrefix=8616,Relay=RTVG3);(Priority=10,PhonePrefix=8617,Relay=RTVG4);(Priority=10,PhonePrefix=8618,Relay=RTVG5);(Priority=10,PhonePrefix=8619,Relay=RTVG6)
```

`routing/RTCUST_routingInfo.cfg`：

```text
CCBSSID:9801
A2PMainRouting:(Priority=10,PhonePrefix=8613,Relay=RTVG1);(Priority=10,PhonePrefix=8615,Relay=RTVG2);(Priority=10,PhonePrefix=8616,Relay=RTVG3);(Priority=10,PhonePrefix=8617,Relay=RTVG4);(Priority=10,PhonePrefix=8618,Relay=RTVG5);(Priority=10,PhonePrefix=8619,Relay=RTVG6)
```

`cm.extra.cfg`（追加到 base/cm.cfg）：RTVG1, RTVG2, RTVG3, RTVG4, RTVG5, RTVG6, RTCG1, RTCG2, RTCG3。

## 步骤和预期

| MT | 发送方 → 号码 | 预期通道（顺序） | Core 日志里要有 | 客户收到的 DR | 说明 |
|---|---|---|---|---|---|
| 1 | OTP → 8613800008001 | rtvg1 |  | DELIVRD | G7-01：160 个 ASCII 字符，模式 1；客户 `rtcg1`；enc `gsm7`；内容 `hello`；length `160`；segLens `160`；vendorDc `0`；vendorEnc `gsm7`；vendorText `same` |
| 2 | OTP → 8613800008002 | rtvg1 |  | DELIVRD | G7-02、G7-12：带 é，160 个 GSM 字符，模式 1：通过（改前 2250）；IN_SUBMIT、发往 7-bit 通道的 OUT_SUBMIT 字符集都是 ASCII；客户 `rtcg1`；enc `gsm7`；内容 `Café été`；length `160`；segLens `160`；vendorDc `0`；vendorEnc `gsm7`；vendorText `same`；CDR MsgID `IN_SUBMIT={id}~"ASCII";OUT_SUBMIT={id}~"ASCII"` |
| 3 | OTP → 8613800008003 | rtvg1 |  | DELIVRD | G7-03：带 é，160 个字符，模式 2（压缩，140 字节）；客户 `rtcg2`；enc `gsm7packed`；内容 `Café été`；length `160`；segLens `160`；vendorDc `0`；vendorEnc `gsm7`；vendorText `same` |
| 4 | OTP → 8613800008004 | rtvg1 |  | DELIVRD | G7-04：带 Δ（UCS2 字符），160 个字符，模式 1：通过（改前 2250）；客户 `rtcg1`；enc `gsm7`；内容 `Δelta`；length `160`；segLens `160`；vendorDc `0`；vendorEnc `gsm7`；vendorText `same` |
| 5 | OTP → 8613800008005 | none |  | !DELIVRD | G7-05：带 é，161 个字符：2250；客户 `rtcg1`；enc `gsm7`；内容 `Café`；length `161`；CDR MsgID `OUT_SUBMIT={id}@2250` |
| 6 | OTP → 8613800008006 | rtvg1 |  | DELIVRD | G7-06a：é 加 5 个 `{`（扩展字符算 2 个），一共 160 个 GSM 字符：通过；客户 `rtcg1`；enc `gsm7`；内容 `Café {{{{{`；length `160`；vendorEnc `gsm7`；vendorText `same` |
| 7 | OTP → 8613800008007 | none |  | !DELIVRD | G7-06b：同上，161 个 GSM 字符：2250；客户 `rtcg1`；enc `gsm7`；内容 `Café {{{{{`；length `161`；CDR MsgID `OUT_SUBMIT={id}@2250` |
| 8 | OTP → 8613800008008 | none |  | !DELIVRD | G7-05b：data_coding 3，160 字节 ISO：2250（和改前一样）；客户 `rtcust`；enc `latin1`；内容 `Café`；length `160`；CDR MsgID `OUT_SUBMIT={id}@2250` |
| 9 | OTP → 8613800008009 | rtvg1 |  | DELIVRD | G7-05c：data_coding 3，140 字节 ISO：通过；发往 7-bit 通道仍是 ISO（不是 7-bit），data_coding 3；客户 `rtcust`；enc `latin1`；内容 `Café`；length `140`；segLens `140`；vendorDc `3`；vendorText `same` |
| 10 | OTP → 8613800008010 | rtvg1 |  | DELIVRD | G7-11：NeedCheckMsgSize:false，161 个字符也不检查；客户 `rtcg3`；enc `gsm7`；内容 `Café`；length `161`；vendorMatch `dest` |
| 11 | OTP → 8613800008011 | rtvg1 | `{MultiSmppServer}[GSM7] ossid=9961 SMSOptionSupportIncomingGSM7bit=1: 1 byte(s) above 0x7F` | DELIVRD | G7-05d：模式 1 里出现 0xE9：解析成 `?`（不改），Server 打一条 WARN；客户 `rtcg1`；enc `gsm7`；hex `436166E9`；vendorEnc `gsm7`；vendorText `Caf? {text}` |

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR、CDR、DB）；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致。

## 注意

`segLens` 是通道收到的每段去掉 UDH 后的字节数：`MsgSplitLen:140` 是每段 short_message 的总长度（含 6 字节 UDH），所以拆出来是 134 + 26（设计稿第 9 节的 `[140, 20]` 是回归在 Core 里按拆分前的内容统计的）。客户 RTCG1 是入向模式 1（GSM 7-bit 每字符一字节），RTCG2 模式 2（压缩），RTCG3 模式 1 并且 `NeedCheckMsgSize:false`，RTCUST 默认（按 ASCII 解析，用 data_coding 3 发 ISO）。通道 RTVG1 外发 7-bit 模式 1、RTVG2 模式 2、RTVG3 默认 0 没配 MsgSplitLen、RTVG4 只配 `MsgSplitLen:140`、RTVG5 配 140 加 `SMSOptionAsciiMsgSplitLen:160`、RTVG6 默认 0 加 `SMSOptionForceTrans2ASCII`。内容用 `a` 补到 `length` 个 GSM 字符（扩展字符 `{` 算 2 个；ISO 按字节），最后是用例标记。通道收到的每段 data_coding、字节数和按编码解出的文本在 evidence 的 `vendor got` 一行。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/GL001_inbound_length`（换配置并重启模块）。
