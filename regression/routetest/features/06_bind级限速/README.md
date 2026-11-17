# 06_bind级限速

SMPP 外发每个 bind 的速率 SMSOptionOutgoingThrottlingNumPerBind（提交 9cba8ab）。

| 文件 | 内容 |
|---|---|
| `SMPP外发bind级限速设计.md` | 仓库 `docs/SMPP外发bind级限速设计.md` 的副本 |
| `SMPP外发bind分布实测报告.md` | 仓库 `docs/SMPP外发bind分布实测报告.md` 的副本 |
| `regression.txt` | 这个功能的回归套件（`newsms/regression/run.sh` 的套件名）：BindRateIT、BindSpreadIT |
| `cases.txt` | 这个功能在服务器上的消息级用例（`cases/` 下的目录名）：BT001～BT006 |

运行：`/usr/local/a2p/routetest/bin/run_feature.sh 06_bind级限速`，结果在本目录的 `result/{时间}/`。

## 服务器用例

| 用例 | 内容 | 设计稿第 6 节 |
|---|---|---|
| `BT001_per_bind_rate` | 每个 bind 10 TPS：2 个 bind 各自不超过 10（任意 1 秒 ≤ 11、任意 100 ms ≤ 2），总量约 20 TPS，不丢不重 | BR-02 BR-08 BR-10 |
| `BT002_per_bind_and_account` | 每个 bind 20 + 账号级 30：总量约 30 TPS，每个 bind 不超过 20 | BR-03 |
| `BT003_account_only` | 对照：只配账号级 100，两个 bind 都用上，约 2 秒发完 | BR-01 |
| `BT004_slow_bind_skipped` | 配了 bind 级：一个 bind 回响应慢（窗口 5 很快满），改用同一连接里的另一个 bind，100 条约 2 秒发完 | BR-09a |
| `BT005_slow_bind_no_per_bind` | 对照：没配 bind 级，同样一个 bind 慢：轮到它时整批推迟 100 ms，发得慢很多（原有行为） | BR-09b |
| `BT006_reload_rate` | RELOAD_CUSTOMER 把每个 bind 从 20 改成 5：之后每个 bind 任意 1 秒 ≤ 6，不重启 | BR-06 |

测试工具按 bind 记下每条 submit_sm 的到达时间，按一批 MT（`count`）检查：每个 bind 任意 1 秒 / 100 ms 内最多几条（`bindMax1s`、`bindMax100ms`）、第一条到最后一条用了多久（`spanMs`）、用了几个 bind（`bindsUsed`）。同一个号码的一批 MT 由同一个 Client 发（外发按 rssid + 发送方 + 号码分片），所以看的是这个 Client 的 bind；`SMSOptionOutgoingThrottlingNum` 是按 Client 节点算的，同样只看这一个节点。用例由 `bin/gen_bindrate_cases.py` 生成。

**设计稿第 6 节每一条的验证方式：**

| 编号 | 方式 |
|---|---|
| BR-01、BR-02、BR-03、BR-06、BR-08、BR-09a、BR-09b、BR-10 | 服务器（上表） |
| BR-04 一个 bind 断开 | 回归 BindRateIT（服务器上的测试工具要能按 bind 断开，排在后面） |
| BR-05 两条连接（两个账号） | 回归 BindRateIT |
| BR-07 长短信 3 段各算一条 | 回归 BindRateIT（服务器上长短信的段可能落在不同 bind，间隔不好判断） |
| BR-11 HTTP 通道 | 没写用例（HTTP 外发没有读这个配置） |
| 线上 bind 不平均的原因（分布实测报告） | 手工 BindSpreadIT |

状态：已写好，未执行。
