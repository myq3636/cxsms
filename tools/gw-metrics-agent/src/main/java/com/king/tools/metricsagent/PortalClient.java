package com.king.tools.metricsagent;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.time.Duration;
import java.util.HexFormat;
import java.util.zip.GZIPOutputStream;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * POST of a batch to Portal (Decisions 5): the JSON gzipped, X-Signature = HMAC-SHA256(secret, the gzipped body) in
 * hex, X-Agent-Host, X-Batch-Id. JDK HttpClient; the JDK's trust store, or agent.portal.trust-store for a self-signed
 * Portal (no switch to skip the certificate check).
 */
final class PortalClient {
    record Reply(int status, String body) {
    }

    private final HttpClient http;
    private final URI uri;
    private final byte[] secret;
    private final String host;
    private final Duration timeout;

    PortalClient(URI uri, byte[] secret, String host, Duration timeout, SSLContext ssl) {
        HttpClient.Builder b = HttpClient.newBuilder().connectTimeout(timeout).version(HttpClient.Version.HTTP_1_1);
        if (ssl != null) {
            b.sslContext(ssl);
        }
        this.http = b.build();
        this.uri = uri;
        this.secret = secret;
        this.host = host;
        this.timeout = timeout;
    }

    static PortalClient create(AgentProperties p, String host) throws IOException, GeneralSecurityException {
        Path sf = Paths.get(p.getSecretFile());
        if (!Files.isReadable(sf)) {
            throw new IOException("the secret file " + sf.toAbsolutePath() + " is missing (agent.secret-file)");
        }
        String s = Files.readString(sf, StandardCharsets.UTF_8).trim();
        if (s.isEmpty()) {
            throw new IOException("the secret file " + sf.toAbsolutePath() + " is empty");
        }
        SSLContext ssl = null;
        String ts = p.getPortal().getTrustStore();
        if (ts != null && !ts.isBlank()) {
            String pw = p.getPortal().getTrustStorePassword();
            KeyStore ks = KeyStore.getInstance(Paths.get(ts).toFile(), pw == null ? null : pw.toCharArray());
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ks);
            ssl = SSLContext.getInstance("TLS");
            ssl.init(null, tmf.getTrustManagers(), null);
        }
        return new PortalClient(URI.create(p.getPortal().getUrl()), s.getBytes(StandardCharsets.UTF_8), host,
                p.getPortal().getTimeout(), ssl);
    }

    Reply post(byte[] json, long batchId) throws IOException, InterruptedException {
        byte[] body = gzip(json);
        HttpRequest req = HttpRequest.newBuilder(uri)
                .timeout(timeout)
                .header("Content-Type", "application/json")
                .header("Content-Encoding", "gzip")
                .header("X-Agent-Host", host)
                .header("X-Batch-Id", String.valueOf(batchId))
                .header("X-Signature", hmac(secret, body))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
        HttpResponse<String> r = http.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        return new Reply(r.statusCode(), r.body());
    }

    static byte[] gzip(byte[] b) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream(Math.max(64, b.length / 4));
        try (GZIPOutputStream gz = new GZIPOutputStream(out)) {
            gz.write(b);
        }
        return out.toByteArray();
    }

    static byte[] gunzip(byte[] b) throws IOException {
        try (InputStream in = new java.util.zip.GZIPInputStream(new java.io.ByteArrayInputStream(b))) {
            return in.readAllBytes();
        }
    }

    static String hmac(byte[] key, byte[] data) {
        try {
            Mac m = Mac.getInstance("HmacSHA256");
            m.init(new SecretKeySpec(key, "HmacSHA256"));
            return HexFormat.of().formatHex(m.doFinal(data));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }
}
