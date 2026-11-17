# GL002 外发的编码和拆分：7-bit 通道模式 1 / 2、默认通道按 ISO 140 拆、ForceTrans2ASCII、只配 MsgSplitLen 140 / 加 AsciiMsgSplitLen 160、data_coding 跟内容（DEF-C6）

**对应用例清单**：G7-07 G7-08 G7-10 G7-12 G7-13 G7-16 G7-17 G7-18 PD-02a PD-02h（`features/07_GSM7与8bit长度/GSM7bit消息长度计算优化设计.md` 第 5、7、8、9 节）

**目的**：同一种消息（模式 1 客户带 é 的 160 个字符，或 ISO / ASCII / UCS2）发往配置不同的通道，看通道收到几段、每段多少字节、data_coding、内容。

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
| 1 | OTP → 8613800008101 | rtvg1 |  | DELIVRD | G7-07、G7-12：7-bit 模式 1 的通道：1 段 160 字节，data_coding 0，GSM 编码；OUT_SUBMIT 字符集 ASCII；客户 `rtcg1`；enc `gsm7`；内容 `Café été`；length `160`；segLens `160`；vendorDc `0`；vendorEnc `gsm7`；vendorText `same`；CDR MsgID `OUT_SUBMIT={id}~"ASCII"` |
| 2 | OTP → 8615000008102 | rtvg2 |  | DELIVRD | G7-08：7-bit 模式 2 的通道：1 段，压缩成 140 字节；客户 `rtcg1`；enc `gsm7`；内容 `Café été`；length `160`；segLens `140`；vendorDc `0`；vendorEnc `gsm7packed`；vendorText `same` |
| 3 | OTP → 8616000008103 | rtvg3 |  | DELIVRD | G7-16、PD-02h、G7-12：默认通道（没配 7-bit、没配 MsgSplitLen）：去掉 7-bit 按 ISO 发，134 + 26 字节（每段 140 含 UDH），data_coding 3（改前是 0）；OUT_SUBMIT 字符集 ISO-8859-1；客户 `rtcg1`；enc `gsm7`；内容 `Café été`；length `160`；segLens `134,26`；vendorDc `3`；vendorText `same`；CDR MsgID `OUT_SUBMIT={id}~"ISO-8859-1"` |
| 4 | OTP → 8619000008104 | rtvg6 |  | DELIVRD | G7-10：ForceTrans2ASCII：按 ASCII 编码，é 变成 `?`（不是 e），1 段 160；客户 `rtcg1`；enc `gsm7`；内容 `Café été`；length `160`；segLens `160`；vendorText `ascii?` |
| 5 | OTP → 8616000008105 | rtvg3 |  | DELIVRD | G7-13b：data_coding 3，140 字节 ISO，默认通道：1 段；客户 `rtcust`；enc `latin1`；内容 `Café`；length `140`；segLens `140`；vendorDc `3`；vendorText `same` |
| 6 | OTP → 8616000008106 | rtvg3 |  | DELIVRD | G7-13c：ASCII 160，默认通道：1 段，data_coding 0；客户 `rtcust`；enc `ascii`；内容 `hello`；length `160`；segLens `160`；vendorDc `0`；vendorText `same` |
| 7 | OTP → 8616000008107 | rtvg3 |  | DELIVRD | G7-13a：data_coding 3，160 字节 ISO（客户不检查长度），默认通道没配 MsgSplitLen：按 140 拆（改前 160 一段）；客户 `rtcg3`；enc `latin1`；内容 `Café`；length `160`；segLens `134,26`；vendorDc `3`；vendorText `same` |
| 8 | OTP → 8617000008108 | rtvg4 |  | DELIVRD | G7-17：只配 MsgSplitLen 140：ASCII 160 也拆成 134 + 26（每段 140 字节含 6 字节 UDH）；客户 `rtcg1`；enc `gsm7`；内容 `hello`；length `160`；segLens `134,26` |
| 9 | OTP → 8617000008109 | rtvg4 |  | DELIVRD | G7-17：只配 MsgSplitLen 140：7-bit 带 é 160 拆成 134 + 26（每段 140 字节含 6 字节 UDH）；客户 `rtcg1`；enc `gsm7`；内容 `Café été`；length `160`；segLens `134,26`；vendorEnc `gsm7`；vendorText `same` |
| 10 | OTP → 8618000008110 | rtvg5 |  | DELIVRD | G7-18：再配 SMSOptionAsciiMsgSplitLen 160：ASCII 160 一段；客户 `rtcg1`；enc `gsm7`；内容 `hello`；length `160`；segLens `160` |
| 11 | OTP → 8618000008111 | rtvg5 |  | DELIVRD | G7-18：同上，7-bit 带 é 160 一段；客户 `rtcg1`；enc `gsm7`；内容 `Café été`；length `160`；segLens `160`；vendorEnc `gsm7`；vendorText `same` |
| 12 | OTP → 8618000008112 | rtvg5 |  | DELIVRD | G7-18：ISO 160（data_coding 3）仍按 MsgSplitLen 140 拆；客户 `rtcg3`；enc `latin1`；内容 `Café`；length `160`；segLens `134,26`；vendorDc `3` |
| 13 | OTP → 8616000008113 | rtvg3 |  | DELIVRD | PD-02a（DEF-C6）：UCS2 发往默认通道：data_coding 8（改前是 0）；客户 `rtcust`；enc `ucs2`；内容 `你好`；vendorDc `8`；vendorText `same` |

## 验收标准

- `result/{时间}/summary.txt` 最后一行 `RESULT: PASS`，每条 MT 都是 PASS（通道顺序、日志、DR、CDR、DB）；
- 每条 MT 的 `evidence/M{n}_{msgId}.log` 能看到它经过的每个通道、Core 的选路 / 改道日志和 CDR，和上表的“说明”一致。

## 注意

`segLens` 是通道收到的每段去掉 UDH 后的字节数：`MsgSplitLen:140` 是每段 short_message 的总长度（含 6 字节 UDH），所以拆出来是 134 + 26（设计稿第 9 节的 `[140, 20]` 是回归在 Core 里按拆分前的内容统计的）。客户 RTCG1 是入向模式 1（GSM 7-bit 每字符一字节），RTCG2 模式 2（压缩），RTCG3 模式 1 并且 `NeedCheckMsgSize:false`，RTCUST 默认（按 ASCII 解析，用 data_coding 3 发 ISO）。通道 RTVG1 外发 7-bit 模式 1、RTVG2 模式 2、RTVG3 默认 0 没配 MsgSplitLen、RTVG4 外发 7-bit 模式 1 只配 `MsgSplitLen:140`、RTVG5 外发 7-bit 模式 1 配 140 加 `SMSOptionAsciiMsgSplitLen:160`（和回归 Gsm7LengthIT 的 VENDOR65、VENDOR66 一样）、RTVG6 默认 0 加 `SMSOptionForceTrans2ASCII`。内容用 `a` 补到 `length` 个 GSM 字符（扩展字符 `{` 算 2 个；ISO 按字节），最后是用例标记。通道收到的每段 data_coding、字节数和按编码解出的文本在 evidence 的 `vendor got` 一行。

## 运行

`/usr/local/a2p/routetest/bin/run_case.sh /usr/local/a2p/routetest/cases/GL002_outbound_coding_split`（换配置并重启模块）。
