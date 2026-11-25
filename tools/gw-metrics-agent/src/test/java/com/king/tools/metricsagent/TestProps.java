package com.king.tools.metricsagent;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

/** AgentProperties over a LogFixture, short intervals, the secret written to root/agent.secret */
final class TestProps {
    private TestProps() {
    }

    static AgentProperties of(Path root, LogFixture fx, String url, String secret) throws IOException {
        AgentProperties p = new AgentProperties();
        p.getPortal().setUrl(url);
        p.getPortal().setTimeout(Duration.ofSeconds(5));
        p.setHost("test-host");
        Path sf = root.resolve("agent.secret");
        Files.writeString(sf, secret + "\n");
        p.setSecretFile(sf.toString());
        p.setStateDir(root.resolve("state").toString());
        p.setLogDir(fx.logDir.toString());
        p.getRolled().setDir(fx.rolledDir.toString());
        p.setSendInterval(Duration.ofMillis(100));
        p.setHeartbeatInterval(Duration.ofSeconds(30));
        p.setAssembleIdle(Duration.ofMillis(200));
        p.getRead().setBackfillMbps(0);
        p.getBatch().setMaxWindows(50);
        return p;
    }
}
