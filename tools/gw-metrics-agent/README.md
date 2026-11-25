# gw-metrics-agent

网关指标采集工具，部署在每台网关机器上，以常驻进程运行。

- 跟读各模块的 metrics 日志 `{模块}_metrics.log`，以及主日志里的 `[METRICS-ALERT]` 行。
- 把读到的窗口批量推送给 Portal 的网关性能监控。
- 只读日志，不改网关的任何东西。

设计：[`docs/网关指标采集工具设计.md`](../../docs/网关指标采集工具设计.md)；部署和配置：[`docs/网关指标采集工具部署配置说明.md`](../../docs/网关指标采集工具部署配置说明.md)。依赖的日志格式和 log4j2 配置见 `docs/性能监控指标说明.md` 6.1、8.3 节。

> 状态：1.0.0 已实现。测试 24 项，也作为全量回归的 `GwMetricsAgent` 套件运行（`regression/run.sh`）。

## 技术栈

- JDK 17。
- Spring Boot 3.5.16，不启 Web 服务器。
- Maven：用工程自带的 Maven Wrapper `mvnw`（固定 Maven 3.9.16），不用另外装 Maven。

## 构建

```
cd tools/gw-metrics-agent
./mvnw -B package            # 产物 target/gw-metrics-agent-1.0.0.jar（约 18 MB）；测试随 package 一起跑
```

构建机需要 JDK 17，并能访问 Maven 中央仓库或内部镜像。不想用 `~/.m2` 时，加 `-Dmaven.repo.local=...`；Maven 本身下载到 `MAVEN_USER_HOME`（默认 `~/.m2`）。

## 部署与启动

```
/usr/local/a2p/metrics-agent/
  gw-metrics-agent.jar
  config/application.yml       # 由 deploy/application.yml.sample 改名，至少填 agent.portal.url
  agent.secret                 # 600
```

```
cd /usr/local/a2p/metrics-agent
java -jar gw-metrics-agent.jar --check     # 检查配置、文件和 Portal 连通性
systemctl enable --now gw-metrics-agent    # 常驻运行（deploy/gw-metrics-agent.service）
```

其他命令：

```
java -jar gw-metrics-agent.jar --dry-run --limit=5                       # 从当前位置解析，打印将要发送的窗口
java -jar gw-metrics-agent.jar --replay='CoreEngine1_metrics.log.2026-11-2*' --from='2026-11-25 10:00'   # 补发旧文件
java -jar gw-metrics-agent.jar --reset=CoreEngine1_metrics.log --to=end   # 先停服务
```

任何配置都可以临时用 `--agent.xxx=值` 覆盖。说明见设计文档 Decisions 8。

## 目录

| 路径 | 内容 |
| --- | --- |
| `pom.xml`、`mvnw`、`.mvn/wrapper/` | Maven 工程和 Maven Wrapper |
| `src/main/java/com/king/tools/metricsagent/` | 源码 |
| `src/main/resources/application.yml` | 默认配置 |
| `src/test/java/com/king/tools/metricsagent/` | 测试（JUnit 5），也作为 `mvn` 类型的套件进全量回归 |
| `deploy/` | `application.yml.sample`、`gw-metrics-agent.service` |
