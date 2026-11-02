package it;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Minimal SMPP 3.4 ESME for the local server tests: bind_transceiver, async submit_sm, deliver_sm handling. */
public class SmppTestClient {
	static final int BIND_TRX = 0x00000009, BIND_TRX_RESP = 0x80000009, SUBMIT = 0x00000004, SUBMIT_RESP = 0x80000004,
			DELIVER = 0x00000005, DELIVER_RESP = 0x80000005, ENQUIRE = 0x00000015, ENQUIRE_RESP = 0x80000015,
			UNBIND = 0x00000006, GENERIC_NACK = 0x80000000;

	final Socket socket;
	final OutputStream out;
	final DataInputStream in;
	final AtomicInteger seq = new AtomicInteger(1);
	final Map<Integer, Long> sentAt = new ConcurrentHashMap<Integer, Long>();
	final Map<Integer, Integer> respStatus = new ConcurrentHashMap<Integer, Integer>();
	final AtomicLong okResp = new AtomicLong(), errResp = new AtomicLong();
	final LatencyRecorder latency;
	final Semaphore window;
	volatile int bindStatus = -1;
	volatile boolean ackDeliver = true;
	/** a slow customer: wait this long before answering each deliver_sm */
	volatile long deliverDelayMs = 0;
	final Map<String, AtomicInteger> deliverByText = new ConcurrentHashMap<String, AtomicInteger>();
	final AtomicLong delivers = new AtomicLong();
	volatile long enquireRespAt = 0;
	final AtomicLong enquiresFromServer = new AtomicLong();
	volatile boolean answerEnquire = true;
	Thread reader;

	SmppTestClient(String host, int port, int window, LatencyRecorder latency) throws IOException {
		socket = new Socket(host, port);
		socket.setTcpNoDelay(true);
		out = socket.getOutputStream();
		in = new DataInputStream(socket.getInputStream());
		this.window = new Semaphore(window);
		this.latency = latency;
		reader = new Thread(this::readLoop, "smpp-test-reader");
		reader.setDaemon(true);
		reader.start();
	}

	static byte[] cstr(String s) {
		byte[] b = s.getBytes(StandardCharsets.ISO_8859_1);
		byte[] r = new byte[b.length + 1];
		System.arraycopy(b, 0, r, 0, b.length);
		return r;
	}

	synchronized void send(int cmd, int status, int sequence, byte[] body) throws IOException {
		ByteBuffer bb = ByteBuffer.allocate(16 + body.length);
		bb.putInt(16 + body.length).putInt(cmd).putInt(status).putInt(sequence).put(body);
		out.write(bb.array());
		out.flush();
	}

	boolean bind(String user, String pwd) throws Exception {
		ByteBuffer b = ByteBuffer.allocate(200);
		b.put(cstr(user)).put(cstr(pwd)).put(cstr("")).put((byte) 0x34).put((byte) 0).put((byte) 0).put(cstr(""));
		byte[] body = new byte[b.position()];
		b.flip();
		b.get(body);
		send(BIND_TRX, 0, seq.getAndIncrement(), body);
		long end = System.currentTimeMillis() + 5000;
		while (bindStatus < 0 && System.currentTimeMillis() < end) Thread.sleep(5);
		return bindStatus == 0;
	}

	/** async submit_sm; blocks while the client's own window is full */
	int submit(String src, String dst, String text) throws Exception {
		window.acquire();
		int s = seq.getAndIncrement();
		byte[] sm = text.getBytes(StandardCharsets.ISO_8859_1);
		ByteBuffer b = ByteBuffer.allocate(120 + sm.length);
		b.put(cstr("")).put((byte) 1).put((byte) 1).put(cstr(src)).put((byte) 1).put((byte) 1).put(cstr(dst))
				.put((byte) 0).put((byte) 0).put((byte) 0).put(cstr("")).put(cstr("")).put((byte) 1).put((byte) 0)
				.put((byte) 0).put((byte) 0).put((byte) sm.length).put(sm);
		byte[] body = new byte[b.position()];
		b.flip();
		b.get(body);
		sentAt.put(s, System.nanoTime());
		send(SUBMIT, 0, s, body);
		return s;
	}

	void enquire() throws IOException {
		send(ENQUIRE, 0, seq.getAndIncrement(), new byte[0]);
	}

	void readLoop() {
		try {
			while (true) {
				int len = in.readInt();
				int cmd = in.readInt();
				int status = in.readInt();
				int s = in.readInt();
				byte[] body = new byte[len - 16];
				in.readFully(body);
				if (cmd == BIND_TRX_RESP) {
					bindStatus = status;
				} else if (cmd == SUBMIT_RESP || cmd == GENERIC_NACK) {
					Long t = sentAt.remove(s);
					if (t != null) {
						if (latency != null) latency.record(System.nanoTime() - t);
						window.release();
					}
					respStatus.put(s, status);
					if (status == 0) okResp.incrementAndGet(); else errResp.incrementAndGet();
				} else if (cmd == DELIVER) {
					delivers.incrementAndGet();
					String text = shortMessage(body);
					deliverByText.computeIfAbsent(text, k -> new AtomicInteger()).incrementAndGet();
					if (deliverDelayMs > 0) Thread.sleep(deliverDelayMs);
					if (ackDeliver) send(DELIVER_RESP, 0, s, new byte[] { 0 });
				} else if (cmd == ENQUIRE) {
					enquiresFromServer.incrementAndGet();
					if (answerEnquire) send(ENQUIRE_RESP, 0, s, new byte[0]);
				} else if (cmd == ENQUIRE_RESP) {
					enquireRespAt = System.currentTimeMillis();
				}
			}
		} catch (Exception e) {
			// closed
		}
	}

	/** short_message of a deliver_sm body */
	static String shortMessage(byte[] b) {
		int i = 0;
		for (int field = 0; field < 1; field++) { while (b[i] != 0) i++; i++; }   // service_type
		i += 2; while (b[i] != 0) i++; i++;                                         // src ton/npi/addr
		i += 2; while (b[i] != 0) i++; i++;                                         // dst ton/npi/addr
		i += 3; while (b[i] != 0) i++; i++; while (b[i] != 0) i++; i++;             // esm..priority, schedule, validity
		i += 4;                                                                      // registered..default_msg_id
		int len = b[i] & 0xff; i++;
		return new String(b, i, len, StandardCharsets.ISO_8859_1);
	}

	/** the reader stops on EOF or a reset: the server closed the connection */
	boolean isClosedByServer() {
		return !reader.isAlive();
	}

	void close() {
		try { socket.close(); } catch (Exception e) { }
	}

	/** submit_sm_resp latency histogram (microseconds) */
	static class LatencyRecorder {
		final java.util.concurrent.ConcurrentLinkedQueue<Long> samples = new java.util.concurrent.ConcurrentLinkedQueue<Long>();
		void record(long nanos) { samples.add(nanos / 1000); }
		long percentile(double p) {
			long[] a = samples.stream().mapToLong(Long::longValue).sorted().toArray();
			if (a.length == 0) return -1;
			return a[Math.min(a.length - 1, (int) Math.ceil(p / 100.0 * a.length) - 1)];
		}
		int count() { return samples.size(); }
	}
}
