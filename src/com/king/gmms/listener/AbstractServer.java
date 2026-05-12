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
import com.king.gmms.ha.ModuleStatusReporter;
import com.king.gmms.ha.systemmanagement.pdu.ModuleRegisterAck;

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
	protected ModuleStatusReporter statusReporter = null;


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
			String redisStatus = "M"; // V4.0 Default to Master, synchronized via Redis Pub/Sub
			
			log.info("{} initializing Redis client. redisStatus={}", module, redisStatus);
			gmmsUtility.initRedisClient(redisStatus);
			log.info("{} Redis client initialized.", module);

			serverThread = new Thread(A2PThreadGroup.getInstance(), this,
					module);
			log.info("{} starting server listen thread.", module);
			serverThread.start();
			log.info("{} server listen thread started.", module);

			try {
				log.info("{} starting ModuleStatusReporter.", module);
				statusReporter = ModuleStatusReporter.start(gmmsUtility, "server", module, gmmsUtility.getNodeId());
			} catch (Exception e) {
				log.warn(module + " failed to start ModuleStatusReporter, server listener keeps running.", e);
			}

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
			if (statusReporter != null) {
				statusReporter.stop();
				statusReporter = null;
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
	}
}
