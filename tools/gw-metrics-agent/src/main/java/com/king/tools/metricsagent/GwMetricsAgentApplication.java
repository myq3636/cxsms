package com.king.tools.metricsagent;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.info.BuildProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

/**
 * gw-metrics-agent (docs/网关指标采集工具设计.md): java -jar gw-metrics-agent.jar runs the daemon (AgentDaemon);
 * with --check, --dry-run, --replay=... or --reset=... it runs that command (CommandRunner) and exits with its code.
 * Configuration: agent.* of config/application.yml next to the jar.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class GwMetricsAgentApplication {
    static final List<String> COMMANDS = List.of("check", "dry-run", "replay", "reset");

    public static void main(String[] args) {
        boolean command = isCommand(args);
        SpringApplication app = new SpringApplication(GwMetricsAgentApplication.class);
        app.setDefaultProperties(Map.of("agent.mode", command ? "command" : "daemon"));
        ConfigurableApplicationContext ctx = app.run(args);
        if (command) {
            System.exit(SpringApplication.exit(ctx));
        }
    }

    static boolean isCommand(String[] args) {
        for (String a : args) {
            for (String c : COMMANDS) {
                if (a.equals("--" + c) || a.startsWith("--" + c + "=")) {
                    return true;
                }
            }
        }
        return false;
    }

    @Bean
    AgentStatus agentStatus(ObjectMapper om) {
        return new AgentStatus(om);
    }

    @Bean
    AgentInfo agentInfo(AgentProperties p, ObjectProvider<BuildProperties> build) {
        String host = p.getHost();
        if (host == null || host.isBlank()) {
            try {
                host = InetAddress.getLocalHost().getHostName();
            } catch (Exception e) {
                host = "unknown";
            }
        }
        BuildProperties b = build.getIfAvailable();
        return new AgentInfo(host, b == null ? "dev" : b.getVersion());
    }

    /** the host name sent to Portal and the agent's version (from the build) */
    record AgentInfo(String host, String version) {
    }
}
