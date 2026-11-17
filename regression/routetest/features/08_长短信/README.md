# 08_长短信

长短信：从 Server 到 DR 回来的流程；每一段都重新给 DR（提交 2934318、f2a1821）。

| 文件 | 内容 |
|---|---|
| `长短信处理流程说明.md` | 仓库 `docs/长短信处理流程说明.md` 的副本 |
| `长短信CDR与DR测试报告.md` | 仓库 `docs/长短信CDR与DR测试报告.md` 的副本 |
| `regression.txt` | 这个功能的回归套件（`newsms/regression/run.sh` 的套件名）：CsmCdrIT |
| `cases.txt` | 这个功能在服务器上的消息级用例（`cases/` 下的目录名）：LS001、LS002 |

运行：`/usr/local/a2p/routetest/bin/run_feature.sh 08_长短信`，结果在本目录的 `result/{时间}/`。

## 消息级用例

| 用例 | 内容 |
|---|---|
| `LS001_long_mt_flow` | 5 条 MT：<br>• 3 段 UDH、3 段 SAR 可选参数，发往按 160 拆的通道（3 段）<br>• 发往按 100 拆的通道（4 段，100、100、100、68）<br>• 发往用 SAR 外发的通道<br>• 单条短信对照<br>每条检查：通道收到的段数和拼起来的内容；`IN_SUBMIT_PARTIAL`、`IN_SUBMIT`、`OUT_SUBMIT`、`OUT_DELI_REP`、`IN_DELI_REP` 各几条；客户每段各收到一条 DR（第 3 节的修复） |
| `LS002_long_mt_expiry` | 3 段只发第 1、3 段：`CsmIntegrityCacheTimeout`（用例里改成 30 秒）到期后，已收到的 2 段各回一条失败 DR，通道收不到，`csm:` 合并缓冲删掉；完整的 3 段对照照常外发 |

用例由 `bin/gen_csm_cases.py` 生成。

**文档里各项的验证方式：**

| 内容 | 方式 |
|---|---|
| 第 8 节 CDR 汇总、测试报告 A / B / C、分段 DR 的修复 | 服务器 LS001 |
| UDH、SAR 两种入向 | 服务器 LS001 |
| 通道用 UDH / SAR 外发 | 服务器 LS001 |
| 没集齐就过期 | 服务器 LS002 |
| 长短信改道（第 1 段失败整条改道） | 服务器 RC019（01_路由重构） |
| 长短信 + 假 DR | 服务器 FD005（03_假DR） |
| 先发后删、BUSY（另一个 Core 正在外发）、重投的旧段只确认 | 回归 CsmCdrIT（要在外发前停掉 Core，服务器上手工做） |
| 24 小时发送上限按整条算 | 要补（配 `SMSOptionRecipientMaxSendCountIn24H` 的客户，可以做成消息级用例） |
| 通道限速时先进 `stream:core:hold`、HTTP 通道 | 回归 |
| 旧模式 `CsmReassembleMode=legacy` | 回归 CsmCdrIT |

状态：已写好，未执行。
