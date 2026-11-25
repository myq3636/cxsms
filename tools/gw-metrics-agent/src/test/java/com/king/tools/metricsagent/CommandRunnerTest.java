package com.king.tools.metricsagent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/** the configuration binding and the commands, in the Spring context as java -jar runs them */
class CommandRunnerTest {
    @TempDir
    Path root;

    @Test
    void portalUrlIsRequired() {
        assertThatThrownBy(() -> new SpringApplicationBuilder(GwMetricsAgentApplication.class)
                .properties("agent.mode=command", "logging.file.name=" + root.resolve("agent.log"))
                .run("--dry-run"))
                .hasStackTraceContaining("agent.portal.url");
    }

    @Test
    void commandArgumentsAreTold() {
        assertThat(GwMetricsAgentApplication.isCommand(new String[] { "--check" })).isTrue();
        assertThat(GwMetricsAgentApplication.isCommand(new String[] { "--replay=x*.gz" })).isTrue();
        assertThat(GwMetricsAgentApplication.isCommand(new String[] { "--agent.host=x" })).isFalse();
        assertThat(CommandRunner.baseName(Path.of("CoreEngine1_metrics.log.2026-11-25-3.gz"))).isEqualTo("CoreEngine1_metrics.log");
        assertThat(CommandRunner.baseName(Path.of("CoreEngine1.log"))).isEqualTo("CoreEngine1.log");
    }

    @Test
    void dryRunPrintsTheWindowsAndChangesNothing() throws Exception {
        LogFixture fx = new LogFixture(root);
        fx.append("CoreEngine1_metrics.log", LogFixture.window("coreengine1", 1795500030000L));
        fx.append("CoreEngine1_metrics.log", LogFixture.window("coreengine1", 1795500060000L));
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try (ConfigurableApplicationContext ctx = new SpringApplicationBuilder(GwMetricsAgentApplication.class)
                .properties("agent.mode=command")
                .run("--agent.portal.url=http://127.0.0.1:1/x", "--agent.log-dir=" + fx.logDir,
                        "--agent.rolled.dir=" + fx.rolledDir, "--agent.state-dir=" + root.resolve("state"),
                        "--agent.assemble-idle=100ms", "--logging.file.name=" + root.resolve("agent.log"))) {
            assertThat(ctx.getBeansOfType(AgentDaemon.class)).as("no daemon in command mode").isEmpty();
            CommandRunner r = ctx.getBean(CommandRunner.class);
            r.out(new PrintStream(buf, true, StandardCharsets.UTF_8));
            r.dryRun(20);
            assertThat(SpringApplication.exit(ctx)).isZero();
        }
        String out = buf.toString(StandardCharsets.UTF_8);
        assertThat(out).contains("\"ts\":1795500030000").contains("\"ts\":1795500060000").contains("dry run: 2 windows");
        assertThat(Files.exists(root.resolve("state/positions.json"))).as("positions unchanged").isFalse();
    }
}
