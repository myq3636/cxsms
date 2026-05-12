/**
 * Standard Logged
 */

package com.king.gmms.listener;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import com.king.db.DatabaseStatus;
import com.king.framework.A2PService;
import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.ha.systemmanagement.pdu.ModuleRegisterAck;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class AbstractServer implements A2PService, Runnable {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(AbstractServer.class);
	protected GmmsUtility gmmsUtility;
	protected volatile boolean running;
	protected String module;
	protected ServerSocket server = null;
	protected ServerSocket serverSocket;// Init in run
	protected Thread serverThread;
	protected int port; // smpp server listener port
	protected ScheduledExecutorService heartbeatExecutor = null;


	public AbstractServer() {
		this.gmmsUtility = GmmsUtility.getInstance();
		running = true;
		module = System.getProperty("module");
		port = Integer.parseInt(gmmsUtility.getModuleProperty("Port").trim());
	}

	/**
	 * start the thread
	 */
	public void run() {
		try {
			if (serverSocket == null) {
				// int port =
				// Integer.parseInt(gmmsUtility.getModuleProperty("Port").trim());
				if (port <= 0) {
					throw new IOException("Port number is:" + port);
				}
				serverSocket = new ServerSocket(port);
				serverSocket.setReuseAddress(true);
				String info = module + " began to listen on port:" + port;
				log.info(info);
				serverSocket.setSoTimeout(10 * 1000);
			}
			while (isRunning()) {
				Socket nextClient = null;
				try {
					nextClient = serverSocket.accept();
					String host = nextClient.getInetAddress().getHostAddress();
					if (nextClient != null) {
						if (gmmsUtility.isAddressScreened(host)) {
							nextClient.close();
							continue;
						}
						log.info("{} accept a connection on port {} from host {}",
										module, host, port);
						createSession(nextClient);
					}
				} catch (IOException e) {
					// NO log needed here because the
					// java.net.SocketTimeoutException is regularly thrown
				}
			}
		} catch (IOException e) {
			log.error(e, e);
		} finally {
			running = false;
			try {
				if (serverSocket != null) {
					serverSocket.close();
					serverSocket = null;
				}
			} catch (IOException e1) {
				log.warn(e1, e1);
			}
		}
	}

	public boolean startService() {
		try {
			serverThread = new Thread(A2PThreadGroup.getInstance(), this,
					module);
			serverThread.start();
			String redisStatus = "M"; // V4.0 Default to Master, synchronized via Redis Pub/Sub
			
			gmmsUtility.initRedisClient(redisStatus);
			
			// V4.0 Redis Heartbeat Mechanism
			String nodeId = System.getProperty("NodeID", "0");
			final String statusKey = "module:status:" + module + ":" + nodeId;
			heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
			heartbeatExecutor.scheduleAtFixedRate(new Runnable() {
				@Override
				public void run() {
					try {
						gmmsUtility.getRedisClient().setString(statusKey, "ONLINE");
						gmmsUtility.getRedisClient().setExpire(statusKey, 30); // 30s TTL
					} catch (Exception e) {
						log.warn("Failed to update Redis heartbeat for " + module, e);
					}
				}
			}, 0, 10, TimeUnit.SECONDS); // Ping every 10 seconds

			log.info("{} starting...", module);
			return true;
		} catch (Exception ex) {
			log.fatal("serverThread initialize fail!", ex);
			System.exit(-1);
			return false;
		}
	}

	public boolean stopService() {
		running = false;
		try {
			beforeStop();
			if (heartbeatExecutor != null) {
				heartbeatExecutor.shutdownNow();
			}
			serverThread.join();
			if (serverSocket != null) {
				serverSocket.close();
				serverSocket = null;
			}
		} catch (Exception e) {
			log.warn(e, e);
		}
			log.info("{} stopped!", module);
		return true;
	}

	protected abstract void createSession(Socket clientSocket)
			throws IOException;

	public boolean isRunning() {
		return running;
	}

	/**
	 * send stop request
	 */
	public void beforeStop() {
		try {
			String nodeId = System.getProperty("NodeID", "0");
			String statusKey = "module:status:" + module + ":" + nodeId;
			gmmsUtility.getRedisClient().del(statusKey);
		} catch (Exception e) {
			log.warn("Failed to delete module status key on stop", e);
		}
	}
}
