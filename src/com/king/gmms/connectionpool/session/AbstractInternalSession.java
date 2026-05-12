package com.king.gmms.connectionpool.session;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.ConnectionStatus;
import com.king.gmms.connectionpool.TCPIPConnection;
import com.king.gmms.connectionpool.connection.ConnectionManager;
import com.king.gmms.connectionpool.sessionthread.SessionThread;
import com.king.gmms.domain.ConnectionInfo;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.messagequeue.InternalMessageQueue;
import com.king.gmms.messagequeue.OperatorMessageQueue;
import com.king.gmms.protocol.tcp.internaltcp.CommandBind;
import com.king.gmms.protocol.tcp.internaltcp.CommandBindAck;
import com.king.gmms.protocol.tcp.internaltcp.CommandKeepAlive;
import com.king.gmms.protocol.tcp.internaltcp.Pdu;
import com.king.gmms.protocol.tcp.internaltcp.TcpByteBuffer;
import com.king.gmms.protocol.tcp.internaltcp.Unprocessed;
import com.king.gmms.protocol.tcp.internaltcp.exception.HeaderIncompleteException;
import com.king.gmms.protocol.tcp.internaltcp.exception.MessageIncompleteException;
import com.king.gmms.sender.PrioritrySender;
import com.king.gmms.threadpool.ExecutorServiceManager;
import com.king.gmms.threadpool.RunnableMsgTask;
import com.king.gmms.threadpool.ThreadPoolProfile;
import com.king.gmms.threadpool.ThreadPoolProfileBuilder;
import com.king.gmms.util.QueueTimeoutInterface;
import com.king.message.gmms.GmmsMessage;

public abstract class AbstractInternalSession extends Session implements
		QueueTimeoutInterface {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(AbstractInternalSession.class);
	private long waitingTime = 1000;
	protected boolean isServer = false;

	protected String sessionName = "";
	protected int sessionNum = 0;

	protected volatile long lastActivity = System.currentTimeMillis();
	protected GmmsUtility gmmsUtility = null;
	protected ConnectionInfo connectionInfo = null;
	protected volatile boolean keepRunning = false;
	protected Object mutex = new Object();
	protected ConnectionStatus status = ConnectionStatus.INITIAL;
	protected TransactionURI transaction;
	protected SessionThread sessionThread = null;
	protected TCPIPConnection connection = null;
	private Integer pendingAliveCount = 0;
	private Unprocessed unprocessed = new Unprocessed();
	protected int currentVersion = Pdu.VERSION_2_0;
	private int enquirelinktime = 30 * 1000;
	protected ConnectionManager connectionManager = null;
	protected InternalMessageQueue msgQueue = null;
	
	protected ExecutorService senderThreadPool;
	protected ExecutorService receiverThreadPool;
	protected ExecutorServiceManager executorServiceManager;

	public AbstractInternalSession() {
		try {
			gmmsUtility = GmmsUtility.getInstance();
			executorServiceManager = gmmsUtility.getExecutorServiceManager();
			sessionNum = gmmsUtility.assignUniqueNumber();
			sessionName = Integer.toString(sessionNum);
			enquirelinktime = Integer.parseInt(gmmsUtility.getCommonProperty(
					"enquirelinktime", "30"))*1000;
			log.debug("Create the session and the sessionNum is {}", sessionNum);
		} catch(Exception e) {
			log.error(e, e);
		}
		
	}

	public boolean isServer() {
		return isServer;
	}

	public String getSessionName() {
		return sessionName;
	}

	public int getSessionNum() {
		return sessionNum;
	}

	public void setWaitingTime(long waitingTime) {
		this.waitingTime = waitingTime;
	}

	public long getWaitingTime() {
		return waitingTime;
	}

	public long getLastActivity() {
		return lastActivity;
	}

	public void setSessionName(String connectionName) {
		this.sessionName = connectionName + "_" + sessionNum;
	}

	public ConnectionInfo getConnectionInfo() {
		return connectionInfo;
	}

	public void start() {
		synchronized (mutex) {
			if (keepRunning == true) {
				return;
			}
			keepRunning = true;
			if (sessionThread != null) {
				sessionThread.start();
			}
		}
	}

	public synchronized void setKeepRunning(boolean keepRunning) {
		this.keepRunning = keepRunning;
	}

	public boolean isKeepRunning() {
		return keepRunning;
	}

	public ConnectionStatus getStatus() {
		return status;
	}

	public TransactionURI getTransactionURI() {
		return transaction;
	}

	public void setStatus(ConnectionStatus status) {
		synchronized (mutex) {
			this.status = status;
			setChanged();
			notifyObservers(status);
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

	public java.nio.ByteBuffer submitAndRec(GmmsMessage msg) throws IOException {
		return null;
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

	public void connectionUnavailable() {
		stop();
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
		connection.setSoTimeout(2, TimeUnit.SECONDS);
		connection.setSendingInterval(1, TimeUnit.MILLISECONDS);
		connection.setMaxSilentTime(gmmsUtility.getMaxSilentTime(),
				TimeUnit.MILLISECONDS);
		connection.setConnectionName("" + sessionNum);
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

	public boolean connect() {
		boolean result = false;
		unprocessed.reset();
		CommandBind request = new CommandBind();
		request.setUserName("" + System.getProperty("module"));
		request.setVersion(currentVersion);
		request.setTimestamp(Long.toString(System.currentTimeMillis()));

		try {
			if (request != null) {
				ByteBuffer received = connection.sendAndReceive(request
						.toByteBuffer().getBuffer());
				if (received != null) {
					Pdu pdu = parsePDU(received);
					if (pdu != null) {
						lastActivity = System.currentTimeMillis();
						if (pdu.getHeader().getCommandId() == Pdu.COMMAND_BIND_ACK) {
							if (((CommandBindAck) pdu).getStatusCode() == 0) {
								if (isKeepRunning()) {
									currentVersion = ((CommandBindAck) pdu)
											.getVersion();
									setStatus(ConnectionStatus.CONNECT);
									clearActiveTestCount();
									result = true;
								}
							} else {
								log.warn("receive COMMAND_BIND_ACK, BIND failed");
							}
						}
					}
				} else {
					log.warn("Did not receive the COMMAND_BIND_ACK");
				}
			}
		} catch (Exception e) {
			log.warn("Tcp session bind fail.", e);
			return false;
		}
		return result;
	}

	public int enquireLink() throws Exception {
		long currentTime = System.currentTimeMillis();
		synchronized (mutex) {
			if (pendingAliveCount > 0) {
				if(log.isInfoEnabled()){
					log.info("Session({}) does not receive the enquire link response!",
								sessionName);
				}
			}
			if (currentTime - lastActivity >= enquirelinktime) {
				try {
					CommandKeepAlive activeTest = new CommandKeepAlive();
					log.debug("Send an alive request to keep session ...");
					submit(activeTest.toByteBuffer().getBuffer());
					pendingAliveCount++;
				} catch (Exception ex) {
					log.error(
							"IOException occured when sending alive request ",
							ex);
					stop();
					throw ex;
				}
			}
		}
		return pendingAliveCount;
	}

	protected void clearActiveTestCount() {
		synchronized (mutex) {
			pendingAliveCount = 0;
		}
	}

	public void parse(ByteBuffer buffer) {
		if (buffer == null) {
			return;
		}
		try {
			// if there is unprocessed bytes[]
			if (unprocessed.getHasUnprocessed()) {
				// judge if the data in unprocessed is expired
				if ((unprocessed.getLastTimeReceived() + 120000) < System
						.currentTimeMillis()) {
					unprocessed.reset();
					log
							.warn(
									"The unprocessed is expired, abandon it, and unprocesses buffer size is {}",
									unprocessed.getUnprocessed().length());
				}
			}

			byte[] bytes = buffer.array();
			// add the new received bytes[] to unprocessed
			if (buffer != null && bytes.length > 0) {
				TcpByteBuffer tcpBuffer = new TcpByteBuffer(bytes);
				unprocessed.getUnprocessed().appendBytes(tcpBuffer);
				unprocessed.setLastTimeReceived();
				unprocessed.check();
				lastActivity = System.currentTimeMillis();
			}
			TcpByteBuffer tcpBuffer = null;
			while (unprocessed.getHasUnprocessed()) {
				tcpBuffer = Pdu.getPduByteBuffer(unprocessed.getUnprocessed(),
						currentVersion);
				unprocessed.check();
				if (tcpBuffer != null) {
	        		receiverThreadPool.execute(new InternalMessageReceiver(tcpBuffer));
				} 
			}
		} catch (HeaderIncompleteException headerEx) {
			log.info("HeaderIncompleteException when parse the buffer.", headerEx);
			unprocessed.check();
		} catch (MessageIncompleteException messageEx) {
			log.info("MessageIncompleteException when parse the buffer.", messageEx);
			unprocessed.check();
		} catch (Exception e) {
			log.warn("There is an Exception while create PDU, drop all bytes", e);
			unprocessed.reset();
		}
	}

	public Pdu parsePDU(ByteBuffer buffer) {

		if (buffer == null) {
			return null;
		}
		Pdu pdu = null;
		try {
			byte[] bytes = buffer.array();
			if (buffer != null && bytes.length > 0) {
				TcpByteBuffer tcpBuffer = new TcpByteBuffer(bytes);
				pdu = Pdu.createPdu(tcpBuffer, currentVersion);
			}

		} catch (HeaderIncompleteException headerEx) {
			if(log.isInfoEnabled()){
				log.info("HeaderIncompleteException when parse the buffer.");
			}
		} catch (MessageIncompleteException messageEx) {
			if(log.isInfoEnabled()){
				log.info("MessageIncompleteException when parse the buffer.");
			}
		} catch (Exception e) {
			if(log.isInfoEnabled()){
				log.info(e, e);
			}
		}
		return pdu;
	}

	public Pdu parsePDU(TcpByteBuffer buffer) {

		if (buffer == null) {
			return null;
		}
		Pdu pdu = null;
		try {
			pdu = Pdu.createPdu(buffer, currentVersion);
		} catch (HeaderIncompleteException headerEx) {
			log.info("HeaderIncompleteException when parse the buffer.");
		} catch (MessageIncompleteException messageEx) {
			log.info("MessageIncompleteException when parse the buffer.");

		} catch (Exception e) {
			log.info(e, e);
		}
		return pdu;
	}

	public void setConnectionManager(ConnectionManager connectionManager) {
		this.connectionManager = connectionManager;
	}

	public ConnectionManager getConnectionManager() {
		return connectionManager;
	}

	@Override
	public void initReceivers() {
		
		if (connectionInfo != null) {
			ThreadPoolProfile profile = new ThreadPoolProfileBuilder("IntReceiverPool_" + sessionName).poolSize(connectionInfo.getMinReceiverNum())
					.maxPoolSize(connectionInfo.getMaxReceiverNum()).build();
			receiverThreadPool = executorServiceManager.newThreadPool(this, "IntReceiver_" + sessionName, profile);
		} else {
			// create a single thread pool first, then update after binded. 
			receiverThreadPool = executorServiceManager.newFixedThreadPool(this, "IntReceiver_" + sessionName, 1);
		}
	}
	
	@Override
	public void updateReceivers() {
		if (connectionInfo != null) {
			ThreadPoolProfile profile = new ThreadPoolProfileBuilder("IntReceiverPool_" + sessionName).poolSize(connectionInfo.getMinReceiverNum())
			                                     .maxPoolSize(connectionInfo.getMaxReceiverNum()).build();
			executorServiceManager.updateThreadPoolProfile((ThreadPoolExecutor)receiverThreadPool, profile, "IntReceiver_" + sessionName);
		} 
	}
	
	@Override
	public void initSenders() {
		int queueTimeout = GmmsUtility.getInstance().getCacheMsgTimeout();
		ThreadPoolProfile senderProfile = new ThreadPoolProfileBuilder("IntSenderPool_" + sessionName)
		                                            .poolSize(connectionInfo.getMinSenderNum()).maxPoolSize(connectionInfo.getMaxSenderNum()).needSafeExit(true).build();
		// sender thread pool
		senderThreadPool = executorServiceManager.newExpiredThreadPool(this, "IntSender_" + sessionName, senderProfile, this, queueTimeout);
	}

	public boolean isFakeSession() {
		return false;
	}

	class InternalMessageReceiver implements Runnable {
		private Object obj = null;
		public InternalMessageReceiver(Object obj) {
			this.obj = obj;
		}

		public void run() {
			if (obj != null) {
				try {
					TcpByteBuffer buffer = (TcpByteBuffer) obj;
					Pdu pdu = parsePDU(buffer);
					if (pdu != null) {
						receive(pdu);
					}
				} catch (Exception e) {
					if(log.isInfoEnabled()){
						log.info("Receive Error PDU and the session Num is {}, {}",
									sessionNum, e);
					}
				}
			}

		}
	}

	public OperatorMessageQueue getOperatorMessageQueue() {
		return this.msgQueue;
	}
	
	public class InternalMessageSender extends RunnableMsgTask {
		
		public InternalMessageSender(GmmsMessage msg) {
			this.message = msg;
		}

		/** 
		 * 
		 * @see java.lang.Runnable#run()
		 */
		@Override
		public void run() {
			deliver(message);

		}
		
		private boolean deliver(GmmsMessage msg) {
			try {
				if (submit(msg)) {
					if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg
							.getMessageType())
							|| GmmsMessage.MSG_TYPE_DELIVERY
									.equalsIgnoreCase(msg.getMessageType())) {
						if(log.isDebugEnabled()){
							log.debug(msg, "Submit ok.");
						}
					} else {
						if(log.isDebugEnabled()){
							log.debug(msg, "Submit {} OK.",msg.getMessageType());
						}
					}
				} else {
					if(log.isInfoEnabled()){
						log.info(msg, "Submit failed.");
					}
					return false;
				}
			} catch (Exception e) {
				if(log.isInfoEnabled()){
					log.info(msg, e, e);
				}
			}
			return true;
		}

	}
	
	public class InternalConnectionMessageQueue implements InternalMessageQueue {
		

		public InternalConnectionMessageQueue(boolean isServer) {
			
		}

		public boolean putMsg(GmmsMessage msg) {
			if (msg == null) {
				return false;
			}
			if (log.isTraceEnabled()) {
				log.trace(msg, "submit to InternalMessageSender thread pool");
			}
			
			try {
				senderThreadPool.execute(new InternalMessageSender(msg));
			} catch (Exception e) {
				if (log.isInfoEnabled()) {
					log.info(msg, e, e);
				}
				
				return false;
			}
			
			return true;
		}

		@Override
	    public void stopMessageQueue() {
			if (executorServiceManager != null) {
				executorServiceManager.shutdown(senderThreadPool);
			}
	    }
	}


}
