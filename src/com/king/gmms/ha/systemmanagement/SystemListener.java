package com.king.gmms.ha.systemmanagement;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.ModuleConnectionInfo;
import com.king.gmms.domain.ModuleManager;

public class SystemListener implements Runnable {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(SystemListener.class);
	private ServerSocket serverSocket = null;;// Init in run
	protected GmmsUtility gmmsUtility;
	protected volatile boolean running = false;
	protected String selfModule;
	private static SystemListener instance = null;
	private static AtomicLong sessionCounter = new AtomicLong();

	private SystemListener() {
		this.gmmsUtility = GmmsUtility.getInstance();
		selfModule = System.getProperty("module");
	}

	/**
	 * 
	 * @return
	 */
	public static synchronized SystemListener getInstance() {
		if (instance == null) {
			instance = new SystemListener();
		}
		return instance;
	}

	/**
	 * create session
	 */
	protected void createSession(Socket clientSocket) throws IOException {
		InetAddress clientAddress = clientSocket.getLocalAddress();
		if (!isAuthorized(clientAddress)) {
			log.info("Refused the unauthorized connection from {}",
					clientAddress.getHostAddress());
			return;
		}
		if (clientSocket != null) {
			String listenerName = "sysLstn_" + sessionCounter.getAndIncrement();
			SystemSession session = new SystemSession(listenerName,
					clientSocket);
			session.start();
		}
	}

	/**
	 * 
	 * @param ip
	 * @return
	 */
	private boolean isAuthorized(InetAddress address) {
		String ip = address.getHostAddress();
		if (address.isLoopbackAddress()) {
			return true;
		}
		ModuleManager moduleManager = ModuleManager.getInstance();
		Set<String> moduleUrls = moduleManager.getModuleUrls();
		return moduleUrls.contains(ip);
	}

	/**
	 * start the thread
	 */
	public void run() {
		try {
			ModuleManager moduleManager = ModuleManager.getInstance();
			ModuleConnectionInfo connectionInfo = moduleManager
					.getConnectionInfo(selfModule);
			if (connectionInfo == null) {
				log.fatal("ConnectionInfo is null when moduleName={}",
						selfModule);
			}
			int port = connectionInfo.getSysPort();
			if (serverSocket == null) {
				if (port <= 0) {
					throw new IOException("Port number is:" + port);
				}
				serverSocket = new ServerSocket(port);
				serverSocket.setReuseAddress(true);
				String info = selfModule + " began to listen on port:" + port;
				log.info(info);
				serverSocket.setSoTimeout(10 * 1000);
				Thread.sleep(100);
			}
			while (isRunning()) {
				Socket nextClient = null;
				try {
					nextClient = serverSocket.accept();
					if (nextClient != null) {
						// if(gmmsUtility.isAddressScreened(nextClient.getInetAddress().getHostAddress()))
						// {
						// nextClient.close();
						// continue;
						// }
						log
								.info(
										"{} Listener accepted a connection on port:{} and then create a {} session",
										selfModule, port, selfModule);
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
				selfModule);
		monitor.start();
		log.info("{} SystemListener thread start!", selfModule);
	}

	public void stop() {
		running = false;
	}

	public boolean isRunning() {
		return running;
	}
}