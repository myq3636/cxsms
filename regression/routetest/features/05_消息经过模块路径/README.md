# 05_消息经过模块路径

modulePath：消息经过的模块，写进日志和 CDR 的 DeliveryChannel（提交 a0b3359）。

| 文件 | 内容 |
|---|---|
| `消息经过模块路径设计.md` | 仓库 `docs/消息经过模块路径设计.md` 的副本 |
| `regression.txt` | 这个功能的回归套件（`newsms/regression/run.sh` 的套件名）：PathChainIT、CsmCdrIT |
| `cases.txt` | 这个功能在服务器上的消息级用例（`cases/` 下的目录名）：PP001 |

运行：`/usr/local/a2p/routetest/bin/run_feature.sh 05_消息经过模块路径`，结果在本目录的 `result/{时间}/`。

## 消息级用例

| 用例 | 内容 |
|---|---|
| `PP001_module_path` | 5 条 MT：<br>• 经过 MultiSmppServer1 和经过 MultiSmppServer2 的 MT 和 DR：Core `[MT-TO-CLIENT]`、Client `[MT-OUT]`、Core `[DR-TO-SERVER]`、Server `[DR-OUT]` 的 `path=`，`IN_SUBMIT`、`OUT_SUBMIT`、`OUT_DELI_REP`、`IN_DELI_REP` 的 DeliveryChannel 列<br>• 改道后第二次外发的路径接着第一次<br>• 路由失败时 Core 生成的失败 DR 带着 MT 的路径<br>• 长短信每段的 `[MT-OUT]` |

这台机器有 2 个 Server、3 个 Core、2 个 Client，每条 MT 走哪个 Core、哪个 Client 不固定，所以用正则检查（例如 `path=MultiSmppServer1>CoreEngine[1-3]>MultiSmppClient[12]`），实际经过的模块在 evidence 里。用例由 `bin/gen_path_cases.py` 生成。

**设计稿每一条的验证方式：**

| 内容 | 方式 |
|---|---|
| 第 4 节的 4 处日志、4 种 CDR | 服务器 PP001 |
| 两个 Server：DR 回到客户 bind 的那个 Server | 服务器 PP001（M2） |
| Stream 第 57 段，新版读旧数据、旧版读新数据 | 回归 PathChainIT（PC-00a/b） |
| 超过 255 个字符时前面用 `..` | 回归 PathChainIT（要反复重试很多次才会出现） |
| 分片迁移、超时被别的 Core 认领时路径里有两个 Core | 手工：处理中停掉一个 Core，看接管的 Core 的日志 |
| DB 兜底、SDQ 之后路径重新开始 | 回归 CsmCdrIT、PathChainIT |
| HTTP 外发、HTTP Server 收消息 | 服务器上没有 HTTP 通道 / 客户，回归 |

状态：已写好，未执行。
