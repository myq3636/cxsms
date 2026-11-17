# 04_性能监控与去掉邮件

性能监控：默认可见、延迟指标、JSON、Redis 快照、A2pCtl metrics、日志告警；去掉告警邮件（提交 b97ae27）。

| 文件 | 内容 |
|---|---|
| `性能监控埋点评估与设计.md` | 仓库 `docs/性能监控埋点评估与设计.md` 的副本 |
| `性能监控指标说明.md` | 仓库 `docs/性能监控指标说明.md` 的副本 |
| `regression.txt` | 这个功能的回归套件（`newsms/regression/run.sh` 的套件名）：MetricsIT、GmmsReloadIT、ConfigReloadIT |
| `cases.txt` | 这个功能在服务器上的消息级用例（`cases/` 下的目录名）：PM001、PM002 |

运行：`/usr/local/a2p/routetest/bin/run_feature.sh 04_性能监控与去掉邮件`，结果在本目录的 `result/{时间}/`。

## 服务器用例

| 用例 | 内容 | 检查项 |
|---|---|---|
| `PM001_metrics_output` | 指标输出：供应商响应、DR 延迟、端到端、Stream 排队、业务计数、消费者耗时、瞬时值、按 ssid / bind、告警、Redis 快照、A2pCtl metrics、没有告警邮件 | 43 项用例级检查 |
| `PM002_reload_format_alert` | 热加载：RELOAD_GMMS_CONFIG 后 Metrics.Format 从 json 换成 kv、告警阈值生效 | 4 项用例级检查 |

指标不是按条 MT 看的：用例发一批 MT（通道 RTVM1 晚 300 ms 回 submit_sm_resp、再过 2 秒回 DR），等两个窗口（70 秒），再用用例级检查看指标日志里的值（`check.metric`：按名字取 p95、count 或每秒次数，判断在不在范围里）、告警日志（`check.log`）、Redis 快照和 `A2pCtl metrics` 的输出（`check.cmd`）。

**这台机器的配置和文档的默认值不一样**：`Gmms/GmmsConfig.properties` 里 `Metrics.LogLevel=DEBUG`（log4j2 的 `metrics.logger` 是 info，所以指标一行都不写，`log/metricsname.log` 从 11-11 起是空的）、`Metrics.PrintGauge=false`、`DB.PoolStats.Enable=false`、`RedisStreamMonitor.Enable=false`。用例运行时在 GmmsConfig 后面追加这几项的默认值（`gmms.append/`），结束后恢复。线上如果也是这样配的，指标同样看不到。

**设计稿第 5 节、指标说明各部分的验证方式：**

| 内容 | 方式 |
|---|---|
| 供应商响应、DR 延迟、端到端、Stream 排队、业务计数、消费者耗时、瞬时值、按 ssid / bind、`vendor_rtt` 告警、Redis 快照、`A2pCtl metrics` | 服务器 PM001 |
| JSON / kv、告警阈值的热加载 | 服务器 PM002 |
| 去掉告警邮件 | 服务器 PM001（a2p.jar 里没有 MailSender、日志里没有发邮件）；原来发邮件的地方打 `[ALERT]` / `[MGT-ALERT]`：回归 GmmsReloadIT、ConfigReloadIT（服务器上要让 Redis、DB 挂掉才会出现） |
| 区间和分位数估算、最大值、并发计数、名字缓存、JSON 分行 | 回归 MetricsIT |
| `stream_lag`、`db_pool_waiting`、`consumer_fail_ratio` 告警，`[STREAM-BACKLOG]`、门铃丢失 | 回归 MetricsIT（服务器上要造积压、连接池等待，排在后面） |
| HTTP 通道 / 客户的指标 | 没做（指标说明第 5 节“不统计的情况”） |
| 开销（1% 以内） | 手工：MemSoakIT 600 TPS 指标全关 / 全开比较 |

状态：已写好，未执行。用例由 `bin/gen_metrics_cases.py` 生成。
