package com.king.gmms.listener;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import com.king.framework.A2PService;
import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;

public abstract class AbstractSslServer implements A2PService, Runnable {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(AbstractSslServer.class);
	protected GmmsUtility gmmsUtility;
	protected volatile boolean running;
	protected String module;
	protected ServerSocket server = null;
	protected ServerSocket serverSocket;// Init in run
	protected Thread serverThread;
	protected int port; // smpp server listener port
    protected boolean canHandover = false;


	public AbstractSslServer() {
		this.gmmsUtility = GmmsUtility.getInstance();
		running = true;
		module = System.getProperty("module");
		port = Integer.parseInt(gmmsUtility.getModuleProperty("Port").trim());
		canHandover = gmmsUtility.isDBHandover();
		
	}

	public boolean startService() {
		try {
			
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
