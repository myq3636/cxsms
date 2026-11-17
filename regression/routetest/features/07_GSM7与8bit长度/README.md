# 07_GSM7与8bit长度

GSM 7-bit 按 GSM 字符检查长度、data_coding 跟内容；8-bit ISO 没配 MsgSplitLen 时按 140 拆（提交 15ad63b、468731a）。

| 文件 | 内容 |
|---|---|
| `GSM7bit消息长度计算优化设计.md` | 仓库 `docs/GSM7bit消息长度计算优化设计.md` 的副本 |
| `Client模块功能与测试用例清单.md` | 仓库 `docs/Client模块功能与测试用例清单.md` 的副本 |
| `regression.txt` | 这个功能的回归套件（`newsms/regression/run.sh` 的套件名）：Gsm7LengthIT、ClientPduIT |
| `cases.txt` | 这个功能在服务器上的消息级用例（`cases/` 下的目录名）：GL001、GL002 |

运行：`/usr/local/a2p/routetest/bin/run_feature.sh 07_GSM7与8bit长度`，结果在本目录的 `result/{时间}/`。

## 消息级用例

| 用例 | 内容 |
|---|---|
| `GL001_inbound_length` | Core 入向长度检查，11 条 MT：7-bit 模式 1 / 2 的 ASCII、带 é、带 Δ 的 160 个字符通过；161 个字符、é 加 5 个 `{` 共 161 个 GSM 字符被拒（OUT_SUBMIT 2250、失败 DR）；data_coding 3 的 ISO 160 字节被拒、140 字节通过；`NeedCheckMsgSize:false` 不检查；模式 1 里出现 0xE9 解析成 `?` 并且 Server 打 `[GSM7]` WARN；IN_SUBMIT、OUT_SUBMIT 的字符集列 |
| `GL002_outbound_coding_split` | 外发的编码和拆分，13 条 MT：7-bit 通道模式 1（160 字节）、模式 2（压缩 140 字节）；默认通道按 ISO 拆成 140 + 20、data_coding 3（DEF-C6 已修）；ForceTrans2ASCII 把 é 变成 e；只配 `MsgSplitLen:140` 时 ASCII 和 7-bit 也拆成 140 + 20；再配 `SMSOptionAsciiMsgSplitLen:160` 时一段；UCS2 发往默认通道 data_coding 8 |

和回归的区别：回归 `Gsm7LengthIT` 在 Core 这一层构造“Server 解析后”的消息，`ClientPduIT` 直接往 Client 的外发 stream 写消息；这里是真实 SMPP 客户按 GSM 7-bit（每字符一字节或压缩）、ISO、UCS2 发，经 Server 解析、Core 检查和处理、Client 拆分编码，测试工具扮演供应商收 PDU，按编码解出文本和原文比较。用例由 `bin/gen_gsm7_cases.py` 生成。

**设计稿第 5 节、`ClientPduIT` 相关项的验证方式：**

| 编号 | 方式 |
|---|---|
| G7-01～G7-05d、G7-06、G7-11、G7-12 | 服务器 GL001 |
| G7-07、G7-08、G7-10、G7-13a～c、G7-16～G7-18、PD-02a、PD-02h | 服务器 GL002 |
| G7-09（默认通道配 `MsgSplitLen:140`） | 和 G7-17 同一种通道（RTVG4），GL002 M9 |
| G7-05e～g（同一客户 10 分钟内第二次不再打 WARN） | 回归 Gsm7LengthIT |
| PD-02b～g（通道 1 / 2 的 ISO、UCS2、二进制） | 回归 ClientPduIT（二进制要 data_coding 4 的客户消息） |

状态：已写好，未执行。
