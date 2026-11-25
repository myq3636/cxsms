package com.king.tools.metricsagent;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Two modules write windows; their files roll over (compressed or not) while the agent reads; Portal fails every 7th
 * request; the agent is killed (no drain, no last save) and started again, twice; at the end Portal has exactly the
 * windows written, each whole, and dedupes what was read again.
 */
class AgentEndToEndTest {
    @TempDir
    Path root;
    final ObjectMapper om = new ObjectMapper();

    final class Agent {
        final PositionStore store;
        final Outbox outbox;
        final Engine engine;
        final Sender sender;

        Agent(AgentProperties p) throws Exception {
            store = new PositionStore(Path.of(p.getStateDir()), om);
            store.load();
            AgentStatus status = new AgentStatus(om);
            outbox = new Outbox(200, 1 << 30);
            engine = new Engine(p, store, outbox, status, om);
            sender = new Sender(p, outbox, PortalClient.create(p, "test-host"), engine, store, status, om, "test-host", "t");
            sender.backoff(20, 100, 20);
            engine.start();
            sender.start();
        }

        void kill() {
            engine.stop();
            sender.stop();
            engine.close();
        }

        void stop() {
            engine.stop();
            sender.drain(java.time.Duration.ofSeconds(10));
            sender.stop();
            try {
                store.save(engine.committed());
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
            engine.close();
        }
    }

    @Test
    void everyWindowExactlyOnceThroughRolloversRetriesAndKills() throws Exception {
        LogFixture fx = new LogFixture(root);
        try (FakePortal portal = new FakePortal("k")) {
            AtomicInteger n = new AtomicInteger();
            portal.status = () -> n.incrementAndGet() % 7 == 0 ? 503 : 200;
            AgentProperties p = TestProps.of(root, fx, portal.url(), "k");
            String[] modules = { "coreengine1", "multismppclient1" };
            Set<String> written = new HashSet<>();
            int[] rolls = { 0, 0 };
            // the files are there before the agent starts, as on a gateway host
            fx.appendRaw("CoreEngine1_metrics.log", "");
            fx.appendRaw("MultiSmppClient1_metrics.log", "");
            Agent agent = new Agent(p);
            Thread.sleep(1500);
            long ts = 1795500000000L;
            for (int w = 1; w <= 600; w++) {
                ts += 30000;
                for (int m = 0; m < modules.length; m++) {
                    String file = (m == 0 ? "CoreEngine1" : "MultiSmppClient1") + "_metrics.log";
                    fx.append(file, LogFixture.window(modules[m], ts));
                    fx.append(file, "261125 10:00:00,000 some other line");
                    written.add(modules[m] + "|0|" + ts);
                    if (w % (37 + m * 11) == 0) {
                        rolls[m]++;
                        fx.roll(file, "2026-11-25", rolls[m], rolls[m] % 2 == 0);
                    }
                }
                if (w % 50 == 0) {
                    Thread.sleep(60);
                }
                if (w == 200 || w == 420) {
                    Thread.sleep(400);
                    agent.kill();
                    agent = new Agent(p);
                }
            }
            for (int i = 0; i < 300 && !portal.windows.containsAll(written); i++) {
                Thread.sleep(100);
            }
            agent.stop();
            assertThat(portal.badSignatures).hasValue(0);
            assertThat(portal.windows).as("every window, nothing else").isEqualTo(written);
            for (JsonNode wj : portal.windowJson.values()) {
                assertThat(wj.path("tps").size()).as("both parts of the TPS line").isEqualTo(2);
                assertThat(wj.path("ssid").size()).isEqualTo(1);
                assertThat(wj.path("gauge").size()).isEqualTo(1);
                assertThat(wj.path("timer").size()).isEqualTo(1);
            }
        }
    }
}
