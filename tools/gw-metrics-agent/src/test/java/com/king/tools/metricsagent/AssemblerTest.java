package com.king.tools.metricsagent;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Test;

/** parsing MetricsReporter's lines and assembling the windows */
class AssemblerTest {
    private final ObjectMapper om = new ObjectMapper();
    private final LineParser parser = new LineParser(om, ZoneId.of("Asia/Shanghai"));

    @Test
    void parsesTheFourTagsAndTellsKvAndBadLines() {
        String[] w = LogFixture.window("coreengine1", 1795599990123L);
        LineParser.Parsed tps = parser.parseMetrics(w[0]);
        assertThat(tps.type()).isEqualTo(LineParser.Type.WINDOW);
        assertThat(tps.module()).isEqualTo("coreengine1");
        assertThat(tps.ts()).isEqualTo(1795599990123L);
        assertThat(tps.window()).isEqualTo(30);
        assertThat(tps.field()).isEqualTo("tps");
        assertThat(parser.parseMetrics(w[2]).field()).isEqualTo("ssid");
        assertThat(parser.parseMetrics(w[3]).field()).isEqualTo("gauge");
        assertThat(parser.parseMetrics(w[4]).field()).isEqualTo("timer");
        assertThat(parser.parseMetrics("261125 10:00:00,000 [METRICS-TPS] module=coreengine1 node=0 window=30s tps=\"a=1.5/s\"").type())
                .isEqualTo(LineParser.Type.KV);
        assertThat(parser.parseMetrics("261125 10:00:00,000 [METRICS-TPS] {\"module\":\"x\",\"ts\":").type())
                .isEqualTo(LineParser.Type.BAD);
        assertThat(parser.parseMetrics("261125 10:00:00,000 [METRICS-TPS] {\"module\":\"x\",\"tps\":{}}").type())
                .as("no ts").isEqualTo(LineParser.Type.BAD);
        assertThat(parser.parseMetrics("INFO something else").type()).isEqualTo(LineParser.Type.OTHER);
    }

    @Test
    void alertTakesTheTimeOfItsLogLine() {
        String line = "WARN  261125 07:41:51,968 metrics-report   [METRICS-ALERT] {\"module\":\"multismppclient1\",\"alert\":\"vendor_rtt\","
                + "\"metric\":\"multismppclient1.client.vendor.submit_rtt.81\",\"value\":250.85,\"threshold\":100.00,\"count\":3,\"windows\":1} ";
        ObjectNode a = parser.parseAlert(line);
        long expect = LocalDateTime.of(2026, 11, 25, 7, 41, 51, 968_000_000).atZone(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli();
        assertThat(a.path("ts").asLong()).isEqualTo(expect);
        assertThat(a.path("alert").asText()).isEqualTo("vendor_rtt");
        assertThat(parser.parseAlert("WARN 261125 07:41:51,968 [METRICS-ALERT] not json")).isNull();
    }

    @Test
    void windowIsItsLinesMergedWithoutTheModulePrefixCompleteAtTheNextTs() {
        List<Outbox.Item> out = new ArrayList<>();
        WindowAssembler asm = new WindowAssembler(sink(out), om, 5000);
        Tailer t = new Tailer("m_metrics.log", Tailer.Kind.METRICS, Path.of("m_metrics.log"), null, 0, null);
        for (String l : LogFixture.window("m", 30000)) {
            asm.add(t, parser.parseMetrics(l), t.ledger.hold(null), 1);
        }
        assertThat(out).isEmpty();
        asm.add(t, parser.parseMetrics(LogFixture.window("m", 60000)[0]), t.ledger.hold(null), 2);
        assertThat(out).hasSize(1);
        ObjectNode w = out.get(0).json();
        assertThat(w.path("module").asText()).isEqualTo("m");
        assertThat(w.path("ts").asLong()).isEqualTo(30000);
        assertThat(w.path("window").asInt()).isEqualTo(30);
        assertThat(w.path("tps").path("business.a").asDouble()).as("part 1").isEqualTo(1.5);
        assertThat(w.path("tps").path("business.b").asDouble()).as("part 2").isEqualTo(2.5);
        assertThat(w.path("ssid").path("ssid.9995.insubmit.received").asDouble()).isEqualTo(3.1);
        assertThat(w.path("gauge").path("redis.stream.server_to_core.xlen").asLong()).isEqualTo(7);
        assertThat(w.path("timer").path("e2e.submit").path("p95").asDouble()).isEqualTo(2.0);
        assertThat(out.get(0).units()).hasSize(5);
        asm.flushIdle(2 + 4999);
        assertThat(out).hasSize(1);
        asm.flushIdle(2 + 5000);
        assertThat(out).hasSize(2);
        assertThat(asm.pending()).isZero();
    }

    @Test
    void modulesInterleavedInOneFileKeepTheirWindowsWhole() {
        List<Outbox.Item> out = new ArrayList<>();
        WindowAssembler asm = new WindowAssembler(sink(out), om, 5000);
        Tailer t = new Tailer("metricsname.log", Tailer.Kind.METRICS, Path.of("metricsname.log"), null, 0, null);
        String[] a = LogFixture.window("a", 30000), b = LogFixture.window("b", 30500);
        // a's first lines, then b (a later ts) in between, then the rest of a's window
        for (String l : new String[] { a[0], a[1], b[0], b[1], a[2], a[3], a[4], b[2], b[3], b[4] }) {
            asm.add(t, parser.parseMetrics(l), t.ledger.hold(null), 1);
        }
        assertThat(out).as("nothing ended by the other module").isEmpty();
        asm.add(t, parser.parseMetrics(LogFixture.window("a", 60000)[0]), t.ledger.hold(null), 2);
        assertThat(out).hasSize(1);
        assertThat(out.get(0).json().path("module").asText()).isEqualTo("a");
        assertThat(out.get(0).units()).as("all five lines of a's window").hasSize(5);
        asm.flushAll();
        assertThat(out).extracting(i -> i.json().path("module").asText() + i.json().path("ts").asLong())
                .containsExactly("a30000", "b30500", "a60000");
    }

    @Test
    void ledgerMovesOnlyPastWhatPortalTook() {
        Ledger l = new Ledger(null);
        Position p0 = new Position("f", 1, 0, 0, 0, 0);
        l.read(p0.at(100, 0));
        assertThat(l.committed().offset()).isEqualTo(100);
        Ledger.Unit a = l.hold(p0.at(100, 0));
        Ledger.Unit b = l.hold(p0.at(200, 0));
        l.read(p0.at(300, 0));
        assertThat(l.committed().offset()).isEqualTo(100);
        b.done = true;
        assertThat(l.committed().offset()).as("a still pending").isEqualTo(100);
        a.done = true;
        assertThat(l.committed().offset()).isEqualTo(300);
    }

    @Test
    void outboxAcksTheHeadAndSaysWhenFull() {
        Outbox o = new Outbox(2, 1_000_000);
        Ledger l = new Ledger(null);
        Outbox.Item w1 = new Outbox.Item(om.createObjectNode(), false, 10, List.of(l.hold(null)));
        Outbox.Item al = new Outbox.Item(om.createObjectNode(), true, 10, List.of(l.hold(null)));
        Outbox.Item w2 = new Outbox.Item(om.createObjectNode(), false, 10, List.of(l.hold(null)));
        o.add(w1);
        o.add(al);
        assertThat(o.full()).isFalse();
        o.add(w2);
        assertThat(o.full()).isTrue();
        List<Outbox.Item> b = o.peek(1, 1_000_000);
        assertThat(b).containsExactly(w1, al);
        o.ack(b);
        assertThat(w1.units().get(0).done).isTrue();
        assertThat(o.size()).isEqualTo(1);
        assertThat(o.full()).isFalse();
    }

    static ItemSink sink(List<Outbox.Item> out) {
        return new ItemSink() {
            @Override
            public void add(Outbox.Item item) {
                out.add(item);
            }

            @Override
            public boolean full() {
                return false;
            }
        };
    }
}
