package com.king.tools.metricsagent;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** the batches to Portal: signature, ack, positions, retries, 413, 401 */
class SenderTest {
    @TempDir
    Path root;
    final ObjectMapper om = new ObjectMapper();
    FakePortal portal;
    AgentProperties p;
    Outbox outbox;
    PositionStore store;
    AgentStatus status;
    Engine engine;
    Ledger ledger;

    @BeforeEach
    void setUp() throws Exception {
        portal = new FakePortal("s3cret");
        LogFixture fx = new LogFixture(root);
        p = TestProps.of(root, fx, portal.url(), "s3cret");
        outbox = new Outbox(1000, 1 << 30);
        store = new PositionStore(Path.of(p.getStateDir()), om);
        status = new AgentStatus(om);
        engine = new Engine(p, store, outbox, status, om);
        ledger = new Ledger(null);
    }

    @AfterEach
    void tearDown() {
        portal.close();
    }

    Sender sender() throws Exception {
        Sender s = new Sender(p, outbox, PortalClient.create(p, "test-host"), engine, store, status, om, "test-host", "t");
        s.backoff(10, 40, 10);
        return s;
    }

    List<Ledger.Unit> windows(int n) {
        List<Ledger.Unit> units = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            Ledger.Unit u = ledger.hold(new Position("f", 1, 0, 0, i, 0));
            units.add(u);
            outbox.add(new Outbox.Item(om.createObjectNode().put("module", "m").put("node", "0").put("ts", 1000 + i), false, 100, List.of(u)));
        }
        return units;
    }

    @Test
    void portalTakesTheBatchSignedAndThePositionsAreSaved() throws Exception {
        List<Ledger.Unit> units = windows(3);
        Sender s = sender();
        assertThat(s.sendOnce(System.currentTimeMillis())).isTrue();
        assertThat(portal.badSignatures).hasValue(0);
        assertThat(portal.windows).hasSize(3);
        assertThat(units).allMatch(u -> u.done);
        assertThat(outbox.size()).isZero();
        assertThat(store.batchId()).isEqualTo(1);
        assertThat(Files.exists(Path.of(p.getStateDir(), "positions.json"))).isTrue();
        JsonNode b = portal.bodies().get(0);
        assertThat(b.path("host").asText()).isEqualTo("test-host");
        assertThat(b.path("batchId").asLong()).isEqualTo(1);
        assertThat(b.path("heartbeat").has("files")).isTrue();
    }

    @Test
    void wrongSecretIs401AndNothingIsTaken() throws Exception {
        windows(1);
        Files.writeString(Path.of(p.getSecretFile()), "other");
        Sender s = sender();
        assertThat(s.sendOnce(System.currentTimeMillis())).isFalse();
        assertThat(status.lastError).startsWith("401");
        assertThat(outbox.size()).isEqualTo(1);
    }

    @Test
    void serverErrorsBackOffAndRetryUntilTaken() throws Exception {
        windows(2);
        AtomicInteger n = new AtomicInteger();
        portal.status = () -> n.incrementAndGet() <= 2 ? 500 : 200;
        Sender s = sender();
        long now = System.currentTimeMillis();
        assertThat(s.sendOnce(now)).isFalse();
        assertThat(status.retrying).isTrue();
        s.tick(now + 1);
        assertThat(portal.requests).as("backing off").hasValue(1);
        for (int i = 0; i < 20 && outbox.size() > 0; i++) {
            now += 50;
            s.tick(now);
        }
        assertThat(outbox.size()).isZero();
        assertThat(status.retrying).isFalse();
        assertThat(portal.windows).hasSize(2);
    }

    @Test
    void tooLargeHalvesTheBatch() throws Exception {
        windows(8);
        AtomicInteger n = new AtomicInteger();
        portal.status = () -> n.incrementAndGet() == 1 ? 413 : 200;
        Sender s = sender();
        long now = System.currentTimeMillis();
        assertThat(s.sendOnce(now)).isFalse();
        assertThat(s.sendOnce(now)).isTrue();
        assertThat(portal.bodies().get(0).path("windows").size()).isEqualTo(4);
        assertThat(outbox.size()).isEqualTo(4);
    }

    @Test
    void idleSendsAHeartbeatAlone() throws Exception {
        p.setHeartbeatInterval(java.time.Duration.ofMillis(100));
        Sender s = sender();
        s.tick(System.currentTimeMillis() + 1000);
        assertThat(portal.bodies()).hasSize(1);
        assertThat(portal.bodies().get(0).path("windows").size()).isZero();
    }
}
