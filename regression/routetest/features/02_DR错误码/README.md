# 02_DR错误码

DR 错误码：通道错误码映射到内部 3 位码、再映射给客户；DB 列 VendorErrorCode、DrErrorCode（提交 ef603b5）。

| 文件 | 内容 |
|---|---|
| `DR错误码设计.md` | 仓库 `docs/DR错误码设计.md` 的副本 |
| `2026-11-11_dr_error_code.sql` | 仓库 `docs/sql/2026-11-11_dr_error_code.sql` 的副本 |
| `regression.txt` | 这个功能的回归套件（`newsms/regression/run.sh` 的套件名）：DrErrorIT、ServerDrPduIT、ClientResultIT、CoreDrBizIT |
| `cases.txt` | 这个功能在服务器上的消息级用例（`cases/` 下的目录名）：DE001～DE005 |

运行：`/usr/local/a2p/routetest/bin/run_feature.sh 02_DR错误码`，结果在本目录的 `result/{时间}/`。

## 消息级用例

| 用例 | 内容 | 设计稿第 8 节 |
|---|---|---|
| `DE001_vendor_err_to_code` | 通道 DR → 内部错误码：stat+err、范围、TLV 优先、只按状态、DELIVRD 不看 err、只按 err、[DEFAULT] 块、对不上、欠费码优先 | DE-01 DE-02 DE-03 DE-04 DE-11 DE-13 |
| `DE002_customer_err` | 内部码 → 客户：没有客户块（legacy）、passthrough、CustomerErr 精确和范围、vendor、000、DRErrTLV、旧配置 SMSOptionSmppMapErrCod4DR | DE-05 DE-06 DE-13 |
| `DE003_submit_err` | submit 失败的码：SubmitErr 精确、Status=*；客户的失败 DR 带这个码 | DE-08 |
| `DE004_reload` | RELOAD_DRERROR：改了对照表，下一条 DR 按新表 | DE-12 |
| `DE005_reload_bad_file` | RELOAD_DRERROR：对照表写错（码不在码表里）整次拒绝，继续用原来的表 | DE-12 |

所有 DE 用例共用一份 `conf/drErrorMapping.cfg`（运行时替换服务器上的文件，结束后恢复；服务器上原来没有这个文件，结束后删掉）和同样的账户：通道 RTVE1～RTVE5（9871～9875，都没有改道码），客户 RTCE1～RTCE6（9881～9886，各自的客户块见 `conf/drErrorMapping.cfg`）。通道回什么 DR 由每条 MT 指定（`vendorDr`、`vendorDrErr`、`vendorTlv`），客户收到的 `err:` 和 `network_error_code` TLV 用 `drErr`、`drTlv` 检查。用例由 `bin/gen_drerr_cases.py` 生成。

**设计稿第 8 节每一条的验证方式：**

| 编号 | 方式 |
|---|---|
| DE-01～DE-06、DE-08、DE-12、DE-13 | 消息级（上表） |
| DE-07 HTTP 通道 / 客户 | 回归 DrErrorIT（服务器上的测试工具只有 SMPP） |
| DE-09 假 DR、方式 3、Core 拒绝、长短信过期 | 假 DR 部分放到 03_假DR 的消息级用例；Core 拒绝、过期：回归 DrErrorIT、ServerDrPduIT |
| DE-10 Stream 里的两个字段 | 回归 DrErrorIT |
| DE-11 CDR 不变、`[DR-ERR]` 日志 | 消息级：每条 MT 都检查 `[DR-ERR]` 行；CDR 格式由回归 CsmCdrIT |
| 第 12 节 DB 列、RDQ 重试恢复状态 | 回归 DrErrorIT、ServerDrPduIT；真实库上的写入：手工（客户不回 deliver_sm_resp 让 DR 进 RDQ，查 `rdq` 的 `VendorErrorCode`、`DrErrorCode`） |

状态：已写好，未执行。
