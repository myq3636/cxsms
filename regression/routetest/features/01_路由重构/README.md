# 01_路由重构

MT 路由重构：4 层规则、组内优先级、排除条件、10 级备份、改道码、系统路由替换、CDR MsgID（提交 c9c19d3）。

| 文件 | 内容 |
|---|---|
| `路由重构设计.md` | 仓库 `docs/路由重构设计.md` 的副本 |
| `路由配置说明.md` | 仓库 `docs/路由配置说明.md` 的副本 |
| `路由重构测试用例清单.md` | 仓库 `docs/路由重构测试用例清单.md` 的副本 |
| `路由服务器测试说明.md` | 仓库 `docs/路由服务器测试说明.md` 的副本 |
| `路由重构用例覆盖.md` | 仓库 `docs/路由重构用例覆盖.md` 的副本：清单的 209 条每条怎么验证 |
| `2026-11-13_routing_level.sql` | 仓库 `docs/sql/2026-11-13_routing_level.sql` 的副本 |
| `regression.txt` | 这个功能的回归套件（`newsms/regression/run.sh` 的套件名）：RoutingRuleIT、CoreRouteIT、CoreRetryIT、CoreDrBizIT、IncrementalRoutingIT、RapidReloadCore、ConfigFileIT、CoreContentIT |
| `cases.txt` | 这个功能在服务器上的消息级用例（`cases/` 下的目录名）：RC001～RC022 |

运行：`/usr/local/a2p/routetest/bin/run_feature.sh 01_路由重构`，结果在本目录的 `result/{时间}/`。

## 消息级用例

| 用例 | 内容 |
|---|---|
| `RC001_prefix_priority_sender` | main route: prefix groups longest first, Priority in a group, Sender, all |
| `RC002_prefix_groups_more` | 前缀分组：三级前缀、宽泛规则优先级更小、长前缀不命中退到短前缀、多前缀规则、数字发送方完全相等 |
| `RC003_keyword_conditions` | 内容关键字：KeywordId、ExcludeKeywordId、发送方+关键字（并且） |
| `RC004_exclude_sender_prefix` | 排除条件：ExcludeSender（不区分大小写）、退到短前缀和 all、ExcludePhonePrefix、排除的 DEBUG 日志 |
| `RC005_number_layer` | 号码层：完全相等、多个号码、同一号码按优先级、号码层先于前缀、号码带 + |
| `RC006_second_routing` | 二次路由：同一号码短时间内第二次发走二次层；二次层不命中往下看 |
| `RC007_default_routing` | 默认路由：没有自己路由文件的客户、客户规则都不命中时、默认路由的备份 |
| `RC008_relay_weight_turn` | Relay：权重 70/30、轮流、; 后面不生效 |
| `RC009_main_unusable_backup` | 主路由选不出能用的通道时，选路时就从备份找：长度不允许、通道不存在、没有主路由规则；备份也没有才路由失败 |
| `RC010_rop_failed_smq` | ROP 失败模式：进 SMQ 等通道恢复，不走备份；恢复后发原来的通道 |
| `RC011_backup_levels` | 逐级备份：每级按自己的规则选、每级只用一次、级别不连续、跳过已经发过的通道 |
| `RC012_backup_unusable_max_10levels` | 备份的通道不存在跳到下一级；走满 10 级；SMSOptionMaxReroute |
| `RC013_retry_code_decides` | 改道码：失败码不在这次发送通道的 SMSOptionSubmitRetryCode 里，不改道、不重试，直接失败 DR |
| `RC014_levels_used_up_retry_main` | 级别用完：按重试策略重试主路由的通道；主路由再失败不再走备份；RoutingLevel 写进 DB；CDR MsgID |
| `RC015_dr_reroute` | DR 失败改道：要通道开 NeedDRReroute；逐级；没开、超时、级别用完：失败 DR 回客户；submit 改道后 DR 再改道 |
| `RC016_dr_reroute_codes` | DR 改道码：配了 SMSOptionDRRerouteCode 只有列表里的码改道；按细分后的内部状态判断；NotPaid 不改道 |
| `RC017_vendor_replacement` | 系统路由替换：选通道时替换；“发过”和改道码看替换后的通道；客户级替换优先；只换一次；替换成不存在的通道 |
| `RC018_cdr_msgid` | CDR MsgID：主路由 msgId，备份通道 msgId-{ssid}，IN_DELI_REP 总是 msgId |
| `RC019_long_sms_reroute` | 长短信：第 1 段失败改道，整条按新通道重新拆分；后面的段跟第 1 段的通道和级别 |
| `RC020_reload_file_failure` | RELOAD_ROUTING：一个文件写错只有它失败、保留原来的路由，其他文件照常生效；旧配置名 WARN 不生效 |
| `RC021_rules_in_redis_and_logs` | Redis 里的实际规则（Rules:{文件}）和选路 / 改道日志的格式 |
| `RC022_load_validation` | 路由文件的加载和校验：属性、备份各级配置名、不生效的配置名 WARN、每种写错的文件只有它自己加载失败 |

RC001 已执行通过；RC002～RC022 只写好了，没有执行。它们用的是 case.properties 第 2 版（《路由服务器测试说明》4.2），要先按 4.3 扩展 `RouteCase` 和 `run_case.sh` 才能运行。每个用例目录的 `README.md` 写了配置、每条 MT 的预期和原因、验收标准。
