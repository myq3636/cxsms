# 03_假DR

假 DR 重新设计：四种方式，每条 MT 只有一个最终 DR（提交 69d376f、99f314c）。

| 文件 | 内容 |
|---|---|
| `假DR逻辑重新设计.md` | 仓库 `docs/假DR逻辑重新设计.md` 的副本 |
| `假DR可疑点测试报告.md` | 仓库 `docs/假DR可疑点测试报告.md` 的副本 |
| `regression.txt` | 这个功能的回归套件（`newsms/regression/run.sh` 的套件名）：FakeDrIT、CoreDrBizIT |
| `cases.txt` | 这个功能在服务器上的消息级用例（`cases/` 下的目录名）：FD001～FD005 |

运行：`/usr/local/a2p/routetest/bin/run_feature.sh 03_假DR`，结果在本目录的 `result/{时间}/`。

## 消息级用例

| 用例 | 内容 | 设计稿第 11 节 |
|---|---|---|
| `FD001_way1_in_submit` | 方式 1（收到客户消息就回）：全部 / 按前缀、延迟、之后的真实 DR 和外发失败只记 CDR、前缀外的失败照常回（DEF-C8）、比例 0 和 90±5、路由失败仍是失败 DR | FN-01 FN-02 FN-03 FN-04 |
| `FD002_way2_out_submit` | 方式 2（通道 submit_sm_resp 成功就回）：通道开、客户拒绝、通道的前缀和延迟、外发失败、之后的真实 DR（含 EXPIRED，DEF-C9）只记 CDR | FN-05 FN-06 |
| `FD003_way3_rewrite` | 方式 3（真实 DR 不是 DELIVERED 时按比例改成成功）：比例 100 按前缀、UNDELIV / REJECTD 改写、前缀外和 DELIVRD 不改、外发失败不改、比例 70 | FN-07 |
| `FD004_way4_timeout` | 方式 4（通道 10 秒不回 DR 就自动回）：超时前真实 DR 到、超时回成功、超时后真实 DR 才到只记 CDR、状态 fail | FN-08 FN-09 |
| `FD005_long_sms_old_config` | 长短信 3 段 + 方式 1 / 方式 2：每段一条 DR、不重复；旧配置 SMSOptionSendFakeDR、SMSOptionSendDRBeforeOutSubmitInDC 不生效并 WARN | FN-13 FN-14 |

所有 FD 用例共用同样的账户：通道 RTVF1～RTVF6（9911～9916；RTVF2、RTVF3 开了方式 2，RTVF4 是可重试的失败、只有 1 个重试点，RTVF5 是不重试的失败，RTVF6 只用来看旧配置的 WARN），客户 RTCF1～RTCF11（9891～9901，每个客户一种假 DR 配置，见各用例的 `cm.extra.cfg`）。“每条 MT 只有一条最终 DR”用 `drCount` 检查：这种 MT 等满 `waitMs`（20～25 秒），确认之后没有第二条。用例由 `bin/gen_fakedr_cases.py` 生成。

**设计稿第 11 节每一条的验证方式：**

| 编号 | 方式 |
|---|---|
| FN-01～FN-09、FN-13、FN-14 | 消息级（上表）；FN-01 只做了路由失败，Core 检查不通过、长短信过期由回归 FakeDrIT |
| FN-07 里的 EXPIRED | 回归 FakeDrIT（真实 DR 是 EXPIRED、MT 还没过期时消息进 WDQ，消息级要等过期） |
| FN-10 到点和真实 DR 同时（两个 Core） | 回归 FakeDrIT（消息级很难让两件事刚好同时） |
| FN-11 方式 4 + DR 失败重路由 | 要补：消息级可以做（通道开 NeedDRReroute），排在 01 的 DR 改道用例跑通之后 |
| FN-12 Core 重启、分片迁移 | 手工：计时中重启一个 Core，看 `zset:fakedr:timeout:*` 由别的 Core 接着处理 |
| FN-15 旧 Redis 延迟键迁移 | 回归 FakeDrIT；服务器上已经没有旧键 |
| FN-16 性能 | 手工 / 压测 |

状态：已写好，未执行。
