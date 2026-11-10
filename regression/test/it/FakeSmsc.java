package it;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** A vendor's SMSC for the Client tests. Its behaviour is set per system_id: reject binds, ignore enquire_link, hold submit_sm_resp, close connections. */
final class FakeSmsc implements Runnable {
	static final class Link {
		final Socket socket; final DataOutputStream out; final int bindCmd; volatile boolean open = true;
		Link(Socket s, DataOutputStream o, int cmd) { socket = s; out = o; bindCmd = cmd; }
		synchronized void pdu(int cmd, int status, int seq, byte[] body) throws IOException {
			out.writeInt(16 + body.length); out.writeInt(cmd); out.writeInt(status); out.writeInt(seq); out.write(body); out.flush();
		}
	}
	static final class Account {
		final String sys;
		volatile int bindStatus = 0;
		volatile boolean answerEnquire = true, answerSubmit = true;
		volatile int submitStatus = 0;
		/** message_id of the next submit_sm_resp; null: a generated one */
		volatile String submitMessageId = null;
		final Map<Integer, Integer> deliverResp = new ConcurrentHashMap<>();
		final AtomicInteger deliverSeq = new AtomicInteger(1000);
		final AtomicInteger binds = new AtomicInteger(), rejected = new AtomicInteger(), closed = new AtomicInteger(),
				enquires = new AtomicInteger(), enquireResps = new AtomicInteger();
		/** enquire_link received before the first close */
		volatile int enquiresAtFirstClose = -1;
		volatile long firstBindAt = 0, firstCloseAt = 0, nextBindAt = 0;
		volatile int bindVersion = 0;
		final List<String> texts = Collections.synchronizedList(new ArrayList<String>());
		final List<Submit> submits = Collections.synchronizedList(new ArrayList<Submit>());
		final List<Integer> bindCmds = Collections.synchronizedList(new ArrayList<Integer>());
		final List<Link> links = new CopyOnWriteArrayList<>();
		final Map<Link, AtomicInteger> perLink = new ConcurrentHashMap<>();
		final Queue<Object[]> held = new ConcurrentLinkedQueue<>();
		final AtomicInteger msgNo = new AtomicInteger();
		/** submit_sm_resp of the bind with this number (0: the first bound) is sent this many ms late */
		final Map<Integer, Long> respDelayMs = new ConcurrentHashMap<>();
		Account(String s) { sys = s; }
		List<Link> open() { List<Link> l = new ArrayList<>(); for (Link k : links) if (k.open) l.add(k); return l; }
		int count(String text) { synchronized (texts) { return Collections.frequency(texts, text); } }
		void closeAll() { for (Link k : open()) try { k.socket.close(); } catch (IOException e) { } }
		/** send a deliver_sm on the first open link; the status of its deliver_sm_resp lands in deliverResp under the returned sequence */
		int deliver(String source, String dest, int esm, byte[] sm, byte[] tlvs) throws IOException {
			int seq = deliverSeq.incrementAndGet();
			ByteArrayOutputStream b = new ByteArrayOutputStream();
			b.write(0);                                     // service_type
			b.write(1); b.write(1); b.write(source.getBytes("US-ASCII")); b.write(0);
			b.write(1); b.write(1); b.write(dest.getBytes("US-ASCII")); b.write(0);
			b.write(esm); b.write(0); b.write(0);           // esm, protocol, priority
			b.write(0); b.write(0);                         // schedule, validity
			b.write(0); b.write(0); b.write(0); b.write(0); // registered, replace, dcs, default
			b.write(sm.length); b.write(sm);
			if (tlvs != null) b.write(tlvs);
			open().get(0).pdu(5, 0, seq, b.toByteArray());
			return seq;
		}
		/** answer the submit_sm held so far */
		int release() throws IOException {
			int n = 0; Object[] h;
			while ((h = held.poll()) != null) { respond((Link) h[0], (Integer) h[1]); n++; }
			return n;
		}
		void respond(Link k, int seq) throws IOException {
			String id = submitMessageId != null ? submitMessageId : "V" + msgNo.incrementAndGet();
			k.pdu(0x80000004, submitStatus, seq, submitStatus == 0 ? (id + "\0").getBytes("US-ASCII") : new byte[] { 0 });
		}
	}
	static final ScheduledExecutorService LATE = Executors.newScheduledThreadPool(2, r -> { Thread t = new Thread(r, "FakeSmsc-late"); t.setDaemon(true); return t; });
	final Map<String, Account> accounts = new ConcurrentHashMap<>();
	final ServerSocket server;
	final int port;

	FakeSmsc(int port) throws IOException { this(new ServerSocket(port)); }
	FakeSmsc(ServerSocket socket) {
		this.port = socket.getLocalPort();
		server = socket;
		Thread t = new Thread(this, "FakeSmsc-" + port);
		t.setDaemon(true);
		t.start();
	}
	Account account(String sys) { return accounts.computeIfAbsent(sys, Account::new); }
	void close() { try { server.close(); } catch (IOException e) { } }

	public void run() {
		while (true) {
			try {
				final Socket s = server.accept();
				Thread t = new Thread(() -> serve(s));
				t.setDaemon(true);
				t.start();
			} catch (Exception e) { return; }
		}
	}

	private void serve(Socket s) {
		Account a = null; Link link = null;
		try {
			DataInputStream in = new DataInputStream(s.getInputStream());
			DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
			while (true) {
				int len = in.readInt(), cmd = in.readInt(), st = in.readInt(), seq = in.readInt();
				byte[] body = new byte[len - 16];
				in.readFully(body);
				if (cmd == 1 || cmd == 2 || cmd == 9) {
					int z = 0;
					while (body[z] != 0) z++;
					a = account(new String(body, 0, z, "US-ASCII"));
					int y = z + 1;
					while (body[y] != 0) y++;      // password
					y++;
					while (body[y] != 0) y++;      // system_type
					a.bindVersion = body[y + 1] & 0xff;
					Link k = new Link(s, out, cmd);
					if (a.bindStatus != 0) {
						a.rejected.incrementAndGet();
						k.pdu(cmd | 0x80000000, a.bindStatus, seq, new byte[] { 0 });
						continue;
					}
					link = k;
					if (a.firstCloseAt > 0 && a.nextBindAt == 0) a.nextBindAt = System.currentTimeMillis();
					if (a.firstBindAt == 0) a.firstBindAt = System.currentTimeMillis();
					a.binds.incrementAndGet();
					a.bindCmds.add(cmd);
					a.links.add(k);
					a.perLink.put(k, new AtomicInteger());
					k.pdu(cmd | 0x80000000, 0, seq, "smsc\0".getBytes("US-ASCII"));
				} else if (cmd == 0x15) {
					if (a != null) a.enquires.incrementAndGet();
					if (a != null && a.answerEnquire && link != null) link.pdu(0x80000015, 0, seq, new byte[0]);
				} else if (cmd == 0x80000005) {
					if (a != null) a.deliverResp.put(seq, st);
				} else if (cmd == 0x80000015) {
					if (a != null) a.enquireResps.incrementAndGet();
				} else if (cmd == 4 && a != null && link != null) {
					Submit parsed = parse(body);
					parsed.link = link;
					parsed.at = System.nanoTime();
					a.submits.add(parsed);
					a.texts.add(shortMessage(body));
					a.perLink.get(link).incrementAndGet();
					Long late = a.respDelayMs.get(a.links.indexOf(link));
					if (!a.answerSubmit) a.held.add(new Object[] { link, seq });
					else if (late != null && late > 0) {
						final Account fa = a; final Link fl = link; final int fs = seq;
						LATE.schedule(() -> { try { fa.respond(fl, fs); } catch (IOException e) { } }, late, TimeUnit.MILLISECONDS);
					} else a.respond(link, seq);
				}
			}
		} catch (Exception e) {
			if (link != null) {
				link.open = false;
				if (a.firstCloseAt == 0) { a.firstCloseAt = System.currentTimeMillis(); a.enquiresAtFirstClose = a.enquires.get(); }
				a.closed.incrementAndGet();
			}
		} finally {
			try { s.close(); } catch (IOException e) { }
		}
	}

	/** service_type, source, dest, esm, pid, priority, schedule, validity, registered, replace, dcs, default, length, message */
	static String shortMessage(byte[] b) throws UnsupportedEncodingException {
		int i = skip(b, 0);          // service_type
		i = skip(b, i + 2);          // source
		i = skip(b, i + 2);          // dest
		i = skip(b, i + 3);          // schedule
		i = skip(b, i);              // validity
		i += 4;
		int n = b[i++] & 0xff;
		return new String(b, i, n, "ISO-8859-1");
	}
	private static int skip(byte[] b, int i) { while (b[i] != 0) i++; return i + 1; }

	/** one TLV: tag, length, value */
	static byte[] tlv(int tag, byte[] value) throws IOException {
		ByteArrayOutputStream b = new ByteArrayOutputStream();
		DataOutputStream d = new DataOutputStream(b);
		d.writeShort(tag); d.writeShort(value.length); d.write(value);
		return b.toByteArray();
	}

	/** every field of a submit_sm */
	static final class Submit {
		Link link; long at; String service, source, dest, schedule, validity; int srcTon, srcNpi, dstTon, dstNpi, esm, protocol, priority, registered, replace, dcs;
		byte[] sm; final Map<Integer, byte[]> tlvs = new HashMap<>();
		String text() throws UnsupportedEncodingException { return new String(sm, "ISO-8859-1"); }
	}
	static Submit parse(byte[] b) throws UnsupportedEncodingException {
		Submit s = new Submit();
		int[] i = { 0 };
		s.service = cstr(b, i);
		s.srcTon = b[i[0]++] & 0xff; s.srcNpi = b[i[0]++] & 0xff; s.source = cstr(b, i);
		s.dstTon = b[i[0]++] & 0xff; s.dstNpi = b[i[0]++] & 0xff; s.dest = cstr(b, i);
		s.esm = b[i[0]++] & 0xff; s.protocol = b[i[0]++] & 0xff; s.priority = b[i[0]++] & 0xff;
		s.schedule = cstr(b, i); s.validity = cstr(b, i);
		s.registered = b[i[0]++] & 0xff; s.replace = b[i[0]++] & 0xff; s.dcs = b[i[0]++] & 0xff; i[0]++;
		int n = b[i[0]++] & 0xff;
		s.sm = Arrays.copyOfRange(b, i[0], i[0] + n);
		i[0] += n;
		while (i[0] + 4 <= b.length) {
			int tag = ((b[i[0]] & 0xff) << 8) | (b[i[0] + 1] & 0xff);
			int len = ((b[i[0] + 2] & 0xff) << 8) | (b[i[0] + 3] & 0xff);
			s.tlvs.put(tag, Arrays.copyOfRange(b, i[0] + 4, i[0] + 4 + len));
			i[0] += 4 + len;
		}
		return s;
	}
	private static String cstr(byte[] b, int[] i) throws UnsupportedEncodingException {
		int s = i[0];
		while (b[i[0]] != 0) i[0]++;
		String r = new String(b, s, i[0] - s, "ISO-8859-1");
		i[0]++;
		return r;
	}

	/** the SMSC asks: is the client alive */
	void enquire(Link k, int seq) throws IOException { k.pdu(0x15, 0, seq, new byte[0]); }
}
