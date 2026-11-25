# gw-metrics-agent

网关指标采集工具，部署在每台网关机器上，以常驻进程运行。

- 跟读各模块的 metrics 日志 `{模块}_metrics.log`，以及主日志里的 `[METRICS-ALERT]` 行。
- 把读到的窗口批量推送给 Portal 的网关性能监控。
- 只读日志，不改网关的任何东西。

设计：[`docs/网关指标采集工具设计.md`](../../docs/网关指标采集工具设计.md)。依赖的日志格式和 log4j2 配置见 `docs/性能监控指标说明.md` 6.1、8.3 节。

> 状态：设计稿，代码未写。

## 目录

| 路径 | 内容 |
| --- | --- |
| `src/com/king/tools/metricsagent/` | 源码（JDK 17） |
| `build.sh` | 构建：`javac --release 17`，把 `../../jar/fastjson-1.2.47.jar` 放进同一个 jar，产物为 `build/gw-metrics-agent.jar`（不提交） |
| `agent.properties.sample` | 配置样例 |
| `gw-metrics-agent.service` | systemd 服务文件 |

测试是 `regression/test/com/king/tools/metricsagent/*Check.java`，登记在 `regression/suites.txt` 里，属于 `unit` 套件，随全量回归一起跑。
