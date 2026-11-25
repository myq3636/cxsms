package com.king.tools.metricsagent;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;

/**
 * Portal's ingest for the tests: checks the signature of the gzipped body, dedupes the windows by (module, node, ts)
 * as Portal does (docs of Portal, Decisions 4), answers what the status supplier says.
 */
final class FakePortal implements AutoCloseable {
    final HttpServer server;
    final byte[] secret;
    final ObjectMapper om = new ObjectMapper();
    final Set<String> windows = Collections.synchronizedSet(new HashSet<>());
    final Map<String, JsonNode> windowJson = Collections.synchronizedMap(new HashMap<>());
    final AtomicInteger duplicates = new AtomicInteger();
    final AtomicInteger requests = new AtomicInteger();
    final AtomicInteger badSignatures = new AtomicInteger();
    final ConcurrentLinkedQueue<JsonNode> bodies = new ConcurrentLinkedQueue<>();
    volatile IntSupplier status = () -> 200;

    FakePortal(String secret) throws IOException {
        this.secret = secret.getBytes(StandardCharsets.UTF_8);
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/v1/gw/metrics/ingest", ex -> {
            requests.incrementAndGet();
            byte[] body = ex.getRequestBody().readAllBytes();
            int code;
            String reply = "";
            if (!PortalClient.hmac(this.secret, body).equals(ex.getRequestHeaders().getFirst("X-Signature"))) {
                badSignatures.incrementAndGet();
                code = 401;
            } else {
                code = status.getAsInt();
                if (code == 200) {
                    JsonNode root = om.readTree(PortalClient.gunzip(body));
                    bodies.add(root);
                    int accepted = 0, dup = 0;
                    for (JsonNode w : root.path("windows")) {
                        String key = w.path("module").asText() + "|" + w.path("node").asText() + "|" + w.path("ts").asLong();
                        if (windows.add(key)) {
                            windowJson.put(key, w);
                            accepted++;
                        } else {
                            duplicates.incrementAndGet();
                            dup++;
                        }
                    }
                    reply = "{\"accepted\":" + accepted + ",\"duplicated\":" + dup + "}";
                }
            }
            byte[] r = reply.getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(code, r.length == 0 ? -1 : r.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(r);
            }
        });
        server.start();
    }

    String url() {
        return "http://127.0.0.1:" + server.getAddress().getPort() + "/api/v1/gw/metrics/ingest";
    }

    List<JsonNode> bodies() {
        return new ArrayList<>(bodies);
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
