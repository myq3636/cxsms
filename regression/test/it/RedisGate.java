package it;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

/** A TCP proxy in front of Redis that can be cut: the client gets real connection errors and Redis keeps its data. */
final class RedisGate implements Runnable {
	final ServerSocket server;
	final int target;
	volatile boolean open = true;
	final Set<Socket> sockets = ConcurrentHashMap.newKeySet();

	RedisGate(int port, int target) throws IOException {
		this.target = target;
		server = new ServerSocket(port);
		Thread t = new Thread(this, "RedisGate");
		t.setDaemon(true);
		t.start();
	}
	public void run() {
		while (true) {
			try {
				Socket c = server.accept();
				if (!open) { c.close(); continue; }
				Socket r = new Socket("127.0.0.1", target);
				c.setTcpNoDelay(true); r.setTcpNoDelay(true);
				sockets.add(c); sockets.add(r);
				pipe(c, r); pipe(r, c);
			} catch (IOException e) { if (server.isClosed()) return; }
		}
	}
	private void pipe(Socket from, Socket to) {
		Thread t = new Thread(() -> {
			byte[] b = new byte[65536];
			try {
				InputStream in = from.getInputStream(); OutputStream out = to.getOutputStream();
				int n;
				while ((n = in.read(b)) >= 0) { out.write(b, 0, n); out.flush(); }
			} catch (IOException e) { }
			close(from); close(to);
		});
		t.setDaemon(true);
		t.start();
	}
	private void close(Socket s) { try { s.close(); } catch (IOException e) { } sockets.remove(s); }
	/** every connection dropped, new ones refused */
	void cut() { open = false; for (Socket s : new ArrayList<>(sockets)) close(s); }
	void restore() { open = true; }
}
