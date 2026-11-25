package com.king.tools.metricsagent;

import java.time.Duration;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.unit.DataSize;
import org.springframework.validation.annotation.Validated;

/**
 * agent.* of application.yml (docs/网关指标采集工具设计.md, Decisions 11). Relative paths are against the working
 * directory (the install directory under systemd).
 */
@ConfigurationProperties("agent")
@Validated
public class AgentProperties {
    /** daemon, or command when the arguments hold a command (set by GwMetricsAgentApplication) */
    private String mode = "daemon";
    @Valid
    private Portal portal = new Portal();
    /** the name in the heartbeat and X-Agent-Host; empty: the host name */
    private String host = "";
    private String secretFile = "agent.secret";
    private String stateDir = "state";
    @NotBlank
    private String logDir = "/usr/local/a2p/log";
    @Valid
    private Metrics metrics = new Metrics();
    @Valid
    private Alert alert = new Alert();
    @Valid
    private Rolled rolled = new Rolled();
    @Valid
    private Start start = new Start();
    private Duration sendInterval = Duration.ofSeconds(10);
    private Duration heartbeatInterval = Duration.ofSeconds(30);
    @Valid
    private Batch batch = new Batch();
    @Valid
    private Outbox outbox = new Outbox();
    private Duration assembleIdle = Duration.ofSeconds(5);
    @Valid
    private Read read = new Read();
    private Duration lagWarn = Duration.ofDays(5);

    public static class Portal {
        @NotBlank(message = "agent.portal.url is required (the ingest URL of Portal)")
        private String url;
        private Duration timeout = Duration.ofSeconds(10);
        private String trustStore;
        private String trustStorePassword;

        public String getUrl() { return url; }
        public void setUrl(String url) { this.url = url; }
        public Duration getTimeout() { return timeout; }
        public void setTimeout(Duration timeout) { this.timeout = timeout; }
        public String getTrustStore() { return trustStore; }
        public void setTrustStore(String trustStore) { this.trustStore = trustStore; }
        public String getTrustStorePassword() { return trustStorePassword; }
        public void setTrustStorePassword(String trustStorePassword) { this.trustStorePassword = trustStorePassword; }
    }

    public static class Metrics {
        @NotBlank
        private String include = "*_metrics.log";

        public String getInclude() { return include; }
        public void setInclude(String include) { this.include = include; }
    }

    public static class Alert {
        /** empty: no alerts collected */
        private String include = "*.log";
        private String exclude = "*_metrics.log,*_gc.log*,*_smpp_pdu.log,*.out";
        /** the time zone of the main log's time stamps; empty: the system's */
        private String zone;

        public String getInclude() { return include; }
        public void setInclude(String include) { this.include = include; }
        public String getExclude() { return exclude; }
        public void setExclude(String exclude) { this.exclude = exclude; }
        public String getZone() { return zone; }
        public void setZone(String zone) { this.zone = zone; }
    }

    public static class Rolled {
        @NotBlank
        private String dir = "/usr/local/a2p/log/previous";
        /** {name}: the live file name, {date}: yyyy-MM-dd, {i}: the index; .gz optional on disk while log4j compresses */
        @NotBlank
        private String pattern = "{name}.{date}-{i}.gz";
        private Duration maxHold = Duration.ofSeconds(60);

        public String getDir() { return dir; }
        public void setDir(String dir) { this.dir = dir; }
        public String getPattern() { return pattern; }
        public void setPattern(String pattern) { this.pattern = pattern; }
        public Duration getMaxHold() { return maxHold; }
        public void setMaxHold(Duration maxHold) { this.maxHold = maxHold; }
    }

    public static class Start {
        /** beginning or end: where a file without a position starts */
        private String metrics = "beginning";
        private String alert = "end";

        public String getMetrics() { return metrics; }
        public void setMetrics(String metrics) { this.metrics = metrics; }
        public String getAlert() { return alert; }
        public void setAlert(String alert) { this.alert = alert; }
    }

    public static class Batch {
        @Min(1)
        private int maxWindows = 500;
        private DataSize maxSize = DataSize.ofMegabytes(8);

        public int getMaxWindows() { return maxWindows; }
        public void setMaxWindows(int maxWindows) { this.maxWindows = maxWindows; }
        public DataSize getMaxSize() { return maxSize; }
        public void setMaxSize(DataSize maxSize) { this.maxSize = maxSize; }
    }

    public static class Outbox {
        @Min(1)
        private int maxWindows = 20000;
        private DataSize maxSize = DataSize.ofMegabytes(50);

        public int getMaxWindows() { return maxWindows; }
        public void setMaxWindows(int maxWindows) { this.maxWindows = maxWindows; }
        public DataSize getMaxSize() { return maxSize; }
        public void setMaxSize(DataSize maxSize) { this.maxSize = maxSize; }
    }

    public static class Read {
        /** MB a second read from rolled files and far behind the live end (before decompression); 0: no limit */
        @Min(0)
        private int backfillMbps = 20;

        public int getBackfillMbps() { return backfillMbps; }
        public void setBackfillMbps(int backfillMbps) { this.backfillMbps = backfillMbps; }
    }

    public String getMode() { return mode; }
    public void setMode(String mode) { this.mode = mode; }
    public Portal getPortal() { return portal; }
    public void setPortal(Portal portal) { this.portal = portal; }
    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }
    public String getSecretFile() { return secretFile; }
    public void setSecretFile(String secretFile) { this.secretFile = secretFile; }
    public String getStateDir() { return stateDir; }
    public void setStateDir(String stateDir) { this.stateDir = stateDir; }
    public String getLogDir() { return logDir; }
    public void setLogDir(String logDir) { this.logDir = logDir; }
    public Metrics getMetrics() { return metrics; }
    public void setMetrics(Metrics metrics) { this.metrics = metrics; }
    public Alert getAlert() { return alert; }
    public void setAlert(Alert alert) { this.alert = alert; }
    public Rolled getRolled() { return rolled; }
    public void setRolled(Rolled rolled) { this.rolled = rolled; }
    public Start getStart() { return start; }
    public void setStart(Start start) { this.start = start; }
    public Duration getSendInterval() { return sendInterval; }
    public void setSendInterval(Duration sendInterval) { this.sendInterval = sendInterval; }
    public Duration getHeartbeatInterval() { return heartbeatInterval; }
    public void setHeartbeatInterval(Duration heartbeatInterval) { this.heartbeatInterval = heartbeatInterval; }
    public Batch getBatch() { return batch; }
    public void setBatch(Batch batch) { this.batch = batch; }
    public Outbox getOutbox() { return outbox; }
    public void setOutbox(Outbox outbox) { this.outbox = outbox; }
    public Duration getAssembleIdle() { return assembleIdle; }
    public void setAssembleIdle(Duration assembleIdle) { this.assembleIdle = assembleIdle; }
    public Read getRead() { return read; }
    public void setRead(Read read) { this.read = read; }
    public Duration getLagWarn() { return lagWarn; }
    public void setLagWarn(Duration lagWarn) { this.lagWarn = lagWarn; }
}
