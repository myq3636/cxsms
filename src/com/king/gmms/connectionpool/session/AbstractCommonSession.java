package com.king.gmms.connectionpool.session;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.ConnectionStatus;
import com.king.gmms.connectionpool.TCPIPConnection;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.threadpool.ThreadPoolProfile;
import com.king.gmms.threadpool.ThreadPoolProfileBuilder;

/**
 * <p>
 * Title:
 * </p>
 * 
 * <p>
 * Description:
 * </p>
 * 
 * <p>
 * Copyright: Copyright (c) 2006
 * </p>
 * 
 * <p>
 * Company:
 * </p>
 * 
 * @author not attributable
 * @version 1.0
 */
public abstract class AbstractCommonSession extends AbstractSession {
	protected int soTimeout = -1;
	protected int buffersize = -1;
	private static SystemLogger log = SystemLogger
			.getSystemLogger(AbstractCommonSession.class);

	protected TCPIPConnection connection = null;
	protected int moduleIndex = 0;
	
	public AbstractCommonSession() {
		super();
		String moduleName = System.getProperty("module");
		if (moduleName != null) {
			moduleIndex = ModuleManager.getInstance()
					.getModuleIndex(moduleName);
		}
	}

	public void stop() {
		synchronized (mutex) {
			if (keepRunning == false) {
				return;
			}
			
			if (sessionThread != null) {
				sessionThread.stopThread();
			}
			if (connection != null && !connection.isClosed()) {
				connection.close();
			}
		}
	}

	public boolean submit(byte[] msg) throws IOException {
		boolean result = false;
		if (msg == null || msg.length == 0) {
			return result;
		}
		try {
			if (connection != null) {
				connection.send(msg);
				result = true;
			}
		} catch (IOException e) {
			stop();
			throw e;
		}
		return result;
	}

	public ByteBuffer submitAndRec(byte[] msg) throws IOException {
		try {
			if (connection != null) {
				return connection.sendAndReceive(msg);
			} else {
				return null;
			}
		} catch (IOException e) {
			stop();
			throw e;
		}
	}

	public boolean createConnection() throws IOException {
		Socket socket = new Socket();
		try {
			socket.connect(new InetSocketAddress(connectionInfo.getURL(),
					connectionInfo.getPort()), 500);
		} catch (IOException e) {
			if(log.isInfoEnabled()){
				log.info("Connect unsuccessfully with {}:{}", connectionInfo
					.getURL(), connectionInfo.getPort());
			}
			if (socket != null) {
				socket.close();
			}
			throw e;
		}
		synchronized (mutex) {
			connection = new TCPIPConnection(socket);
		}
		connection.setReceiver(this);
		connection.setSendingInterval(1, TimeUnit.MILLISECONDS);
		connection.setMaxSilentTime(gmmsUtility.getMaxSilentTime(),
				TimeUnit.MILLISECONDS);
		connection.setSoTimeout(5, TimeUnit.SECONDS);
		connection.setConnectionName("" + sessionNum);
		if (this.buffersize > 0) {
			connection.setReadBufferSize(buffersize);// TODO:need add to config
														// file
		}
		connection.open();
		return true;
	}

	public boolean createConnection(Socket socket) throws IOException {
		if (socket != null) {
			synchronized (mutex) {
				connection = new TCPIPConnection(socket);
			}
			connection.setReceiver(this);
			connection.setSendingInterval(1, TimeUnit.MILLISECONDS);
			connection.setMaxSilentTime(gmmsUtility.getMaxSilentTime(),
					TimeUnit.MILLISECONDS);
			connection.setConnectionName("" + sessionNum);
			connection.open();
			return true;
		} else {
			return false;
		}
	}

	public abstract ArrayList parsePDU(ByteBuffer buffer);

	public void connectionUnavailable() {
		if (!status.equals(ConnectionStatus.RETRY)) {
			stop();
		}
	}

	public void parse(ByteBuffer buffer) {
		ArrayList list = parsePDU(buffer);
		if (list != null && list.size() > 0) {
			for (Object obj : list) {
				try {
					receiverThreadPool.execute(new ReceiverThread(obj));
				} catch (Exception e) {
					// if thread pool has been shutdown, 
					// will throw exception due to KingCallerRunsPolicy
					if (log.isInfoEnabled()) {
						log.info(e, e);
					}
					return;
				}
				lastActivity = System.currentTimeMillis();
			}
			
		}
	}

	class ReceiverThread implements Runnable {
		Object obj = null;
		public ReceiverThread(Object obj) {
			this.obj = obj;
		}

		public void run() {
			if (obj != null) {
				receive(obj);
			}

		}
	}

	public int getSoTimeout() {
		return soTimeout;
	}

	public void setSoTimeout(int soTimeout) {
		this.soTimeout = soTimeout;
	}

	public int getBuffersize() {
		return buffersize;
	}

	public void setBuffersize(int buffersize) {
		this.buffersize = buffersize;
	}

	public int getModuleIndex() {
		return moduleIndex;
	}

	public void setModuleIndex(int moduleIndex) {
		this.moduleIndex = moduleIndex;
	}
	
	@Override
	public void initSenders() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void initReceivers() {
		if (connectionInfo != null) {
			ThreadPoolProfile profile = new ThreadPoolProfileBuilder("CustReceiver_" + connectionInfo.getSsid() + "_" + sessionName).poolSize(connectionInfo.getMinReceiverNum())
					.maxPoolSize(connectionInfo.getMaxReceiverNum()).build();
			receiverThreadPool = executorServiceManager.newThreadPool(this, "CustReceiver_" + connectionInfo.getSsid(), profile);
		} else {
			// create a single thread pool first, then update after binded. 
			// can't use customer info for thread name here, since haven't binded.
	        receiverThreadPool = executorServiceManager.newFixedThreadPool(this, "CustReceiver_" + sessionName, 1);
		}
	}
	
	@Override
	public void updateReceivers() {
		if (connectionInfo != null) {
			ThreadPoolProfile profile = new ThreadPoolProfileBuilder("CustReceiver_" + connectionInfo.getSsid()).poolSize(connectionInfo.getMinReceiverNum())
					.maxPoolSize(connectionInfo.getMaxReceiverNum()).build();
			executorServiceManager.updateThreadPoolProfile((ThreadPoolExecutor)receiverThreadPool, profile, "CustReceiver_" + customerInfo.getSSID());
		} 
	}

}
