package com.king.gmms.listener.smppserver;

import java.net.Socket;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;

/**
 * Connection limits of the SMPP server ports (plain and TLS): SmppServerMaxConnections in total
 * and SmppServerMaxConnectionsPerIp per client IP, 0 = no limit (default). A connection above a
 * limit is closed right after accept, before any session thread is created.
 *
 * <p>Connections are counted by their sockets; a socket closed by its session (stop, bind timeout,
 * silence) no longer counts.
 */
public final class SmppServerConnectionLimiter {
	private static final SystemLogger log = SystemLogger.getSystemLogger(SmppServerConnectionLimiter.class);
	private static final SmppServerConnectionLimiter instance = new SmppServerConnectionLimiter();

	private final Map<String, Set<Socket>> byIp = new ConcurrentHashMap<String, Set<Socket>>();

	public static SmppServerConnectionLimiter getInstance() {
		return instance;
	}

	/** Accept the socket, or close it when a limit is reached. Called on the accept thread only. */
	public boolean admit(Socket socket, String module) {
		String ip = socket.getInetAddress().getHostAddress();
		int perIp = intProperty("SmppServerMaxConnectionsPerIp");
		int total = intProperty("SmppServerMaxConnections");
		if (perIp > 0 || total > 0) {
			int ipCount = live(ip);
			int allCount = total > 0 ? liveTotal() : 0;
			String reason = null;
			if (perIp > 0 && ipCount >= perIp) {
				reason = "SmppServerMaxConnectionsPerIp=" + perIp + " reached by " + ip;
			} else if (total > 0 && allCount >= total) {
				reason = "SmppServerMaxConnections=" + total + " reached";
			}
			if (reason != null) {
				log.warn("{} refused a connection from {}: {}", module, ip, reason);
				close(socket);
				return false;
			}
		}
		Set<Socket> set = byIp.get(ip);
		if (set == null) {
			Set<Socket> created = ConcurrentHashMap.newKeySet();
			set = byIp.putIfAbsent(ip, created);
			if (set == null) {
				set = created;
			}
		}
		set.add(socket);
		return true;
	}

	/** The session could not be created: stop counting the socket and close it. */
	public void failed(Socket socket) {
		Set<Socket> set = byIp.get(socket.getInetAddress().getHostAddress());
		if (set != null) {
			set.remove(socket);
		}
		close(socket);
	}

	/** Open connections of an IP (closed sockets are dropped). */
	public int live(String ip) {
		Set<Socket> set = byIp.get(ip);
		if (set == null) {
			return 0;
		}
		for (Iterator<Socket> it = set.iterator(); it.hasNext();) {
			if (it.next().isClosed()) {
				it.remove();
			}
		}
		return set.size();
	}

	public int liveTotal() {
		int n = 0;
		for (String ip : byIp.keySet()) {
			n += live(ip);
		}
		return n;
	}

	private static void close(Socket socket) {
		try {
			socket.close();
		} catch (Exception ignored) {
		}
	}

	private static int intProperty(String key) {
		try {
			return Integer.parseInt(GmmsUtility.getInstance().getCommonProperty(key, "0").trim());
		} catch (Exception e) {
			return 0;
		}
	}
}
