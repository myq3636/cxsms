package com.king.gmms.listener;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.ConnectionInfo;
import com.king.gmms.domain.ModuleManager;

public abstract class AbstractInternalListener implements Runnable {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(AbstractInternalListener.class);
	private ServerSocket serverSocket = null;;// Init in run
	protected GmmsUtility gmmsUtility;
	protected volatile boolean running;
	protected String module;

	public AbstractInternalListener() {
		this.gmmsUtility = GmmsUtility.getInstance();
		running = true;
		module = System.getProperty("module");
	}

	/**
	 * start the thread
	 */
	public void run() {
		try {
			ModuleManager moduleManager = ModuleManager.getInstance();
			ConnectionInfo connectionInfo = moduleManager
					.getConnectionInfo(module);
			if (connectionInfo == null) {
				log.fatal("ConnectionInfo is null when moduleName={}", module);
			}
			int port = connectionInfo.getPort();
			if (serverSocket == null) {
				if (port <= 0) {
					throw new IOException("Port number is:" + port);
				}
				serverSocket = new ServerSocket(port);
				serverSocket.setReuseAddress(true);
				String info = module + " began to listen on port:" + port;
				log.info(info);
				serverSocket.setSoTimeout(10 * 1000);
				Thread.sleep(100);
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
						log.info("{} accepted an internal connection on port {} from {} ",
										module, port, host);
						createSession(nextClient);
					}
				} catch (IOException e) {
					// NO log needed here because the
					// java.net.SocketTimeoutException is regularly thrown
				}
			}
		} catch (IOException e) {
			log.error(e, e);
		} catch (Exception e) {
			log.fatal(e, e);
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

	public void start() {
		running = true;
		Thread monitor = new Thread(A2PThreadGroup.getInstance(), this,
				module);
		monitor.start();
		log.info("{} InternalListener thread start!", module);
	}

	public void stop() {
		running = false;
	}

	protected abstract void createSession(Socket clientSocket)
			throws IOException;

	public boolean isRunning() {
		return running;
	}
}
