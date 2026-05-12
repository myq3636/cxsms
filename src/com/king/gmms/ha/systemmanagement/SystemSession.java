package com.king.gmms.ha.systemmanagement;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.ConnectionStatus;
import com.king.gmms.connectionpool.Receiver;
import com.king.gmms.connectionpool.TCPIPConnection;
import com.king.gmms.connectionpool.connection.ConnectionManager;
import com.king.gmms.connectionpool.session.Session;
import com.king.gmms.connectionpool.sessionthread.SessionThread;
import com.king.gmms.connectionpool.systemmanagement.ConnectionManagementForFunction;
import com.king.gmms.connectionpool.systemmanagement.ConnectionManagementForMGT;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.ConnectionInfo;
import com.king.gmms.domain.ModuleConnectionInfo;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.ha.ConfigurationException;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.ha.systemmanagement.pdu.ApplyInThrottleQuotaAck;
import com.king.gmms.ha.systemmanagement.pdu.ConnectionConfirm;
import com.king.gmms.ha.systemmanagement.pdu.ConnectionConfirmAck;
import com.king.gmms.ha.systemmanagement.pdu.ConnectionStatusNotification;
import com.king.gmms.ha.systemmanagement.pdu.ConnectionStatusNotificationAck;
import com.king.gmms.ha.systemmanagement.pdu.DBOperationAck;
import com.king.gmms.ha.systemmanagement.pdu.InBindRequest;
import com.king.gmms.ha.systemmanagement.pdu.ModuleRegisterAck;
import com.king.gmms.ha.systemmanagement.pdu.OutBindRequest;
import com.king.gmms.ha.systemmanagement.pdu.SystemPdu;
import com.king.gmms.protocol.tcp.internaltcp.TcpByteBuffer;
import com.king.gmms.protocol.tcp.internaltcp.Unprocessed;
import com.king.gmms.protocol.tcp.internaltcp.exception.HeaderIncompleteException;
import com.king.gmms.protocol.tcp.internaltcp.exception.MessageIncompleteException;
import com.king.gmms.threadpool.ExecutorServiceManager;
import com.king.gmms.threadpool.ThreadPoolProfile;
import com.king.gmms.threadpool.ThreadPoolProfileBuilder;
import com.king.gmms.throttle.ThrottlingControl;
import com.king.gmms.throttle.ThrottlingTimemark;
import com.king.gmms.util.SystemConstants;

public class SystemSession implements Receiver, SystemMessageBufferListener {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(SystemSession.class);
	protected volatile boolean keepRunning = false;
	protected volatile long lastActivity = System.currentTimeMillis();
	protected Object mutex = new Object();
	protected TCPIPConnection connection = null;
	private Socket socket = null;
	private String serverAddress = null;
	private int port = -1;
	private GmmsUtility gmmsUtility;
	protected String moduleName = "";
	protected SessionThread sessionThread = null;
	protected MessageHandler handler;
	protected int failActiveTest = 0;
	private boolean isServer = false;
	protected ConnectionStatus status = ConnectionStatus.INITIAL;
	private SystemMessageBuffer bufferMonitor = null;
	private Map<Integer, SystemPdu> responseBuffer;
	private Map<String, SystemPdu> responseQueryFlagBuffer;
	private ModuleManager moduleManager = null;
	private int maxRetryTimes = 3;
	private int minSenderNum = -1;
	private int minReceiverNum = -1;
	private int maxSenderNum = -1;
	private int maxReceiverNum = -1;
	private boolean isEnableSysMgt = false;
	private long responseWaitTime = 1000 * 120;// 2 minutes
	private long registerTimePeriod = 300;
	private long startTimeMillis = System.currentTimeMillis();
	private Unprocessed unprocessed = new Unprocessed();
	private boolean isMGT = false;
	
	private ExecutorService senderThreadPool;
	private ExecutorService verSenderThreadPool;
	private ExecutorService receiverThreadPool;
	private ExecutorServiceManager executorServiceManager;
	protected SystemSessionFactory sessionFactory = SystemSessionFactory.getInstance();
	private String sessionName;
	
	private ThrottlingControl throttlingControl = ThrottlingControl.getInstance();

	/**
	 * 
	 * @param moduleName
	 * @param serverAddress
	 * @param port
	 */
	public SystemSession(ModuleConnectionInfo conInfo) throws Exception {
		this.moduleName = conInfo.getModuleName();
		String serverAddress = conInfo.getURL();
		if (serverAddress == null || serverAddress.length() < 1) {
			throw new ConfigurationException("The IP list of " + moduleName
					+ " is empty");
		}
		int port = conInfo.getSysPort();
		if (serverAddress != null && !serverAddress.equalsIgnoreCase("")) {
			this.serverAddress = serverAddress;
		}
		if (port > 0) {
			this.port = port;
		}
		gmmsUtility = GmmsUtility.getInstance();
		executorServiceManager = gmmsUtility.getExecutorServiceManager();
		moduleManager = ModuleManager.getInstance();
		sessionName = moduleName;
		maxRetryTimes = Integer.parseInt(moduleManager.getClusterProperty(
				"SystemManager.MessageRetryTimes", "3"));
		minSenderNum = Integer.parseInt(moduleManager.getClusterProperty(
				"SystemManager.MinSenderNumber", "-1"));
		minReceiverNum = Integer.parseInt(moduleManager.getClusterProperty(
				"SystemManager.MinReceiverNumber", "-1"));
		maxSenderNum = Integer.parseInt(moduleManager.getClusterProperty(
				"SystemManager.MaxSenderNumber", "-1"));
		maxReceiverNum = Integer.parseInt(moduleManager.getClusterProperty(
				"SystemManager.MaxReceiverNumber", "-1"));
		responseWaitTime = Long.parseLong(moduleManager.getClusterProperty(
				"SystemManager.ResponseWaitTimeOut", "300000"));
		int windowsSize = Integer.parseInt(moduleManager.getClusterProperty(
				"SystemManager.BufferSize", "2000"));
		registerTimePeriod = Integer.parseInt(moduleManager.getClusterProperty("SystemManager.RegisterTimePeriod", "300"));
		bufferMonitor = new SystemMessageBuffer(sessionName, windowsSize);
		responseBuffer = new ConcurrentHashMap<Integer, SystemPdu>();
		responseQueryFlagBuffer = new ConcurrentHashMap<String, SystemPdu>();
		handler = new SystemMessageHandler();
		sessionThread = new SystemClientSessionThread(moduleName, this);
		isEnableSysMgt = gmmsUtility.isSystemManageEnable();
		String selfModule = System.getProperty("module");
		String selfModuleType = ModuleManager.getInstance().getModuleType(
				selfModule);
		isMGT = SystemConstants.MGT_MODULE_TYPE.equalsIgnoreCase(selfModuleType);
	}

	/**
	 * 
	 * @param name
	 * @param socket
	 */
	public SystemSession(String name, Socket socket) {
		if (name != null && name.length() > 0) {
			this.moduleName = name;
		}
		if (socket != null) {
			this.socket = socket;
			open();
		}
		gmmsUtility = GmmsUtility.getInstance();
		executorServiceManager = gmmsUtility.getExecutorServiceManager();
		moduleManager = ModuleManager.getInstance();
		sessionName = name;
		maxRetryTimes = Integer.parseInt(moduleManager.getClusterProperty(
				"SystemManager.MessageRetryTimes", "3"));
		minSenderNum = Integer.parseInt(moduleManager.getClusterProperty(
				"SystemManager.MinSenderNumber", "-1"));
		minReceiverNum = Integer.parseInt(moduleManager.getClusterProperty(
				"SystemManager.MinReceiverNumber", "-1"));
		maxSenderNum = Integer.parseInt(moduleManager.getClusterProperty(
				"SystemManager.MaxSenderNumber", "-1"));
		maxReceiverNum = Integer.parseInt(moduleManager.getClusterProperty(
				"SystemManager.MaxReceiverNumber", "-1"));
		responseWaitTime = Long.parseLong(moduleManager.getClusterProperty(
				"SystemManager.ResponseWaitTimeOut", "300000"));
		int windowsSize = Integer.parseInt(moduleManager.getClusterProperty(
				"SystemManager.BufferSize", "2000"));
		registerTimePeriod = Integer.parseInt(moduleManager.getClusterProperty("SystemManager.RegisterTimePeriod", "300"));
		bufferMonitor = new SystemMessageBuffer(sessionName, windowsSize);
		responseBuffer = new ConcurrentHashMap<Integer, SystemPdu>();
		responseQueryFlagBuffer = new ConcurrentHashMap<String, SystemPdu>();
		handler = new SystemMessageHandler();
		sessionThread = new SystemServerSessionThread(moduleName, this);
		isEnableSysMgt = gmmsUtility.isSystemManageEnable();
		isServer = true;
		String selfModule = System.getProperty("module");
		String selfModuleType = ModuleManager.getInstance().getModuleType(
				selfModule);
		isMGT = SystemConstants.MGT_MODULE_TYPE.equalsIgnoreCase(selfModuleType);
	}

	/**
	 * startBufferMonitor
	 * 
	 * @return
	 */
	private boolean startBufferMonitor() {
		bufferMonitor.addListener(this);
		bufferMonitor.setWaitTime(200, MILLISECONDS);
		int expiryTime = Integer.parseInt(moduleManager.getClusterProperty(
				"SystemManager.BufferTimeOut", "3")) * 1000;
		bufferMonitor.setTimeout(expiryTime, MILLISECONDS);
		bufferMonitor.startMonitor();
		return true;
	}

	/**
	 * start thread
	 */
	public void start() {
		synchronized (mutex) {
			if (keepRunning) {
				return;
			}
			
			startBufferMonitor();
			this.initReceivers(minReceiverNum, maxReceiverNum);
			this.initSenders(minSenderNum, maxSenderNum);
			
			keepRunning = true;
			if (sessionThread != null) {
				sessionThread.start();
			}
			
		}
	}

	/**
	 * stop when sessionthread stop
	 */
	public void stop() {
		synchronized (mutex) {
			if (keepRunning == false) {
				return;
			}
			if (this.isServer) {
				keepRunning = false;
				if (executorServiceManager != null) {
					executorServiceManager.shutdown(receiverThreadPool);
					executorServiceManager.shutdown(senderThreadPool);
					executorServiceManager.shutdown(verSenderThreadPool);
				}
			}
			
			if (sessionThread != null) {
				sessionThread.stopThread();
			}
			if (connection != null && !connection.isClosed()) {
				connection.close();
			}
		}
	}

	/**
	 * shutdown when module stopservice
	 */
	public void shutdown() {
		synchronized (mutex) {
			if (keepRunning == false) {
				return;
			}
			keepRunning = false;
			
			if (executorServiceManager != null) {
				executorServiceManager.shutdown(receiverThreadPool);
				executorServiceManager.shutdown(senderThreadPool);
				executorServiceManager.shutdown(verSenderThreadPool);
			}
			
			if (sessionThread != null) {
				sessionThread.stopThread();
			}
			if (connection != null && !connection.isClosed()) {
				connection.close();
			}
		}
	}

	/**
	 * 
	 * @param message
	 * @return
	 * @throws Exception
	 */
	public boolean send(SystemPdu message) throws IOException {
		if (message == null) {
			return false;
		}
		if (connection != null) {
			try {
				connection.send(message.toByteBuffer().getBuffer());
				lastActivity = System.currentTimeMillis();
				if (!isMGT&&(message.getCommandId()==SystemPdu.COMMAND_ACK
						||message.getCommandId()==SystemPdu.COMMAND_KEEP_ALIVE
						||message.getCommandId()==SystemPdu.COMMAND_REPORT_IN_MSG_COUNT)){
						log.info("send ({}) to {}",message,moduleName);
				}else{
					log.info("send ({}) to {}",message,moduleName);
				}
				return true;
			} catch (Exception e) {
				log.error(e, e);
				return false;
			}

		} else {
			throw new IOException();
		}
	}

	/**
	 * 
	 * @param msg
	 * @return
	 * @throws IOException
	 */
	public ByteBuffer submitAndRec(byte[] msg) throws IOException {
		try {
			if (connection != null) {
				lastActivity = System.currentTimeMillis();
				return connection.sendAndReceive(msg);
			} else {
				return null;
			}
		} catch (IOException e) {
			throw e;
		}
	}

	/**
	 * connectionUnavailable
	 */
	public void connectionUnavailable() {
		if (!status.equals(ConnectionStatus.RETRY)) {
			stop();
		}
	}

	/**
	 * deal with session broken
	 * 
	 * @return
	 */
	public synchronized boolean exceptionHandler(int failedCount) {
		if (!isMGT) {
			stop();
			return true;
		}
		String lastStatus = moduleManager.getModuleStatusMap().get(moduleName)
				.get(0);
		if (!SystemConstants.UP_MODULE_STATUS.equalsIgnoreCase(lastStatus)
				|| System.currentTimeMillis() - this.startTimeMillis < 1000 * 90) {
			log.info("Session just init, so ignore exceptionHandler.");
			return false;
		}
		boolean isAlive = moduleManager.isAliveModule(moduleName);
		if (!isAlive) {
			return false;
		}
		int maxEnquireLinkNum = Integer.parseInt(moduleManager
				.getClusterProperty("SystemManager.MaxEnquireLinkNumber", "5"));
		if (failedCount < maxEnquireLinkNum) {
			return false;
		}
		ConnectionManagementForMGT.getInstance().moduleDown(moduleName);
		stop();
		return true;
	}

	/**
	 * open connection
	 * 
	 * @return
	 */
	public boolean open() {
		try {
			synchronized (mutex) {
				unprocessed.reset();
				connection = new TCPIPConnection(socket);
			}
			connection.setConnectionName(moduleName);
			connection.setReceiver(this);
			connection.setSoTimeout(2, TimeUnit.SECONDS);
			connection.setSendingInterval(1, TimeUnit.MILLISECONDS);
			connection.setMaxSilentTime(10, TimeUnit.MILLISECONDS);
			connection.open();
			lastActivity = System.currentTimeMillis();
		} catch (IOException e) {
			log.error(e, e);
			return false;
		}
		return true;
	}

	/**
	 * 
	 * @param socket
	 * @throws IOException
	 */
	public void createConnection(Socket socket) throws IOException {
		if (socket != null) {
			synchronized (mutex) {
				unprocessed.reset();
				connection = new TCPIPConnection(socket);
			}
			connection.setReceiver(this);
			connection.setSendingInterval(1, TimeUnit.MILLISECONDS);
			connection.setMaxSilentTime(gmmsUtility.getMaxSilentTime(),
					TimeUnit.MILLISECONDS);
			connection.open();
		}
	}

	 /**
	 * parse ByteBuffer
	 */
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
				tcpBuffer = SystemPdu.getPduByteBuffer(unprocessed
						.getUnprocessed());
				unprocessed.check();
				if (tcpBuffer != null) {
					receiverThreadPool.execute(new ReceiverThread(tcpBuffer));
					lastActivity = System.currentTimeMillis();
				}
			}
		} catch (HeaderIncompleteException headerEx) {
			log.info("HeaderIncompleteException when parse the buffer.");
			unprocessed.check();
		} catch (MessageIncompleteException messageEx) {
			log.info("MessageIncompleteException when parse the buffer.");
			unprocessed.check();
		} catch (Exception e) {
			log.warn("Create PDU Exception {}, drop all bytes.", e);
			unprocessed.reset();
		}
	}

	/**
	 * receive and send response
	 */
	public boolean receive(Object obj) {
		if (obj == null) {
			return false;
		}
		SystemPdu pdu = (SystemPdu) obj;
		log.debug("Received pdu ({}) from {}",pdu, moduleName);
		boolean flag = true;
		lastActivity = System.currentTimeMillis();
		try {
			boolean isRequest = pdu.isRequest();
			if (isRequest) {
				SystemPdu response = handler.process(pdu);
				if (response != null) {
					try{
						flag = send(response);
						if(!flag){
							response.nextRetry();
							flag = putMessage(response);
						}
					}catch(IOException e){
						response.nextRetry();
						flag = putMessage(response);
					}
				}
			}else{
				handleResponse(pdu);
			}
		} catch (Exception e) {
			log.error(e, e);
		}
		return flag;
	}

	/**
	 * isOpen
	 * 
	 * @return
	 */
	public boolean isOpen() {
		if (connection != null) {
			return !connection.isClosed();
		} else {
			return false;
		}
	}

	/**
	 * create
	 * 
	 * @return
	 * @throws Exception
	 */
	public boolean create() throws Exception {
		if (socket != null && !socket.isClosed()) {
			return true;
		}
		socket = new Socket();
		try {
			socket.connect(new InetSocketAddress(serverAddress, port), 500);
		} catch (IOException e) {
			if (socket != null) {
				socket.close();
			}
			throw e;
		}
		return true;
	}

	/**
	 * 
	 * @throws IOException
	 */
	public void createConnection() throws IOException {
		Socket socket = new Socket();
		try {
			socket.connect(new InetSocketAddress(serverAddress, port), 500);
		} catch (IOException e) {
			log.error("Connect unsuccessfully with {}:{}", serverAddress, port);
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
		connection.open();
	}

	/**
	 * dispose connection
	 */
	public void dispose() {
		if (connection != null && !connection.isClosed()) {
			connection.close();
		}
	}

	public void setSessionThread(SessionThread sessionThread) {
		if (sessionThread != null) {
			this.sessionThread = sessionThread;
		}
	}

	/**
	 * keep alive
	 * 
	 * @return
	 * @throws Exception
	 */
	public int activeTest() throws IOException {
		SystemPdu pdu = SystemPdu.createPdu(SystemPdu.COMMAND_KEEP_ALIVE);
		if(send(pdu)){
			log.debug("Send keep_a_live message ({}) to {} successfully", pdu
					.getTimestamp(), moduleName);
			synchronized (mutex) {
				failActiveTest++;
			}
			lastActivity = System.currentTimeMillis();
		}
		return failActiveTest;
	}

	/**
	 * 
	 * @param minReceiverNum
	 */
	public void initReceivers(int minReceiverNum, int maxReceiverNum) {
		ThreadPoolProfile receiverProfile = new ThreadPoolProfileBuilder("SysReceiverPool_" + sessionName).poolSize(minReceiverNum).maxPoolSize(maxReceiverNum).build();
		receiverThreadPool = executorServiceManager.newThreadPool(this, "SysReceiver_" + sessionName, receiverProfile);
	}

	/**
	 * 
	 * @param minSenderNum
	 */
	public void initSenders(int minSenderNum, int maxSenderNum) {
		ThreadPoolProfile senderProfile = new ThreadPoolProfileBuilder("SysSenderPool_" + sessionName)
                                                   .poolSize(minSenderNum).maxPoolSize(maxSenderNum).build();
		// sender thread pool
		senderThreadPool = executorServiceManager.newThreadPool(this, "SysSender_" + sessionName, senderProfile);
		
		// sender thread pool
		verSenderThreadPool = executorServiceManager.newThreadPool(this, "SysVerSender_" + sessionName, senderProfile);
	}

	/**
	 * send and receive response
	 * 
	 * @param message
	 * @return
	 */
	public SystemPdu sendAndReceive(SystemPdu message) {
		SystemPdu response = null;
		try {
			ByteBuffer received = submitAndRec(message.toByteBuffer()
					.getBuffer());
			if(received==null){
				log.info("send to {} but receive null for {}",this.moduleName, message.toString());
				return null;
			}
			TcpByteBuffer buffer = new TcpByteBuffer(received.array());
			response = SystemPdu.createPdu(buffer);
			log.info("Sent {}, received {}",message,response);
		} catch (IOException e) {
			log
					.error("sendAndReceive failed for message:"
							+ message.getUuid(), e);
		} catch (Exception e) {
			log.error(e, e);
		}
		return response;
	}

	/**
	 * moduleRegister when module startup
	 */
	public boolean moduleRegister() {
		ModuleRegisterAck ack = moduleRegisterInDetail();
		if(ack!=null){
			return ack.getResponseCode() == 0;
		}else{
			return false;
		}
	}

	/**
	 * moduleRegister returned ack when module startup
	 */
	public ModuleRegisterAck moduleRegisterInDetail() {
		SystemPdu message = SystemPdu
				.createPdu(SystemPdu.COMMAND_MODULE_REGISTER_REQUEST);
		boolean isSuccess = false;
		ModuleRegisterAck registerAck = null;
		do{
			try {
				registerAck = (ModuleRegisterAck)sendAndReceive(message);
				if(registerAck==null){
					isSuccess=false;
				}else{
					int code = registerAck.getResponseCode();
					isSuccess = (code == 0);
				}
			} catch (Exception e) {
				log.warn("moduleRegister failed due to:", e);
			}
			if (!isSuccess) {
				try {
					Thread.sleep(registerTimePeriod);
				} catch (InterruptedException e) {
					log.warn(e, e);
				}
			} 
		} while (!isSuccess);
		SystemSessionFactory.getInstance().setRegister(isSuccess);
		return registerAck;
	}
	/**
	 * moduleRegister returned ack when module startup
	 */
	public ModuleRegisterAck moduleRegisterInLimit() {
		SystemPdu message = SystemPdu
				.createPdu(SystemPdu.COMMAND_MODULE_REGISTER_REQUEST);
		boolean isSuccess = false;
		ModuleRegisterAck registerAck = null;
		int count = 0;
		do{
			try {
				registerAck = (ModuleRegisterAck)sendAndReceive(message);
				if(registerAck==null){
					isSuccess=false;
				}else{
					int code = registerAck.getResponseCode();
					isSuccess = (code == 0);
				}
			} catch (Exception e) {
				log.warn("moduleRegister failed due to:", e);
			}
			if (!isSuccess) {
				try {
					Thread.sleep(registerTimePeriod);
				} catch (InterruptedException e) {
					log.warn(e, e);
				}
			} 
		} while (!isSuccess  && ++count<3);
		SystemSessionFactory.getInstance().setRegister(isSuccess);
		return registerAck;
	}

	/**
	 * moduleStop when module stop
	 */
	public void moduleStop() {
		SystemPdu message = SystemPdu
				.createPdu(SystemPdu.COMMAND_MODULE_STOP_REQUEST);
		boolean result = false;
		try {
			result = bufferMonitor.put(message.getSequenceNumber(), message);
			if(result){
				result = send(message);
				if (!result) {
					bufferMonitor.remove(message.getSequenceNumber());
					message.nextRetry();
					putMessage(message);
				}
			}else{
				message.nextRetry();
				putMessage(message);
			}
		} catch (IOException e) {
			bufferMonitor.remove(message.getSequenceNumber());
			putMessage(message);
			log.warn("moduleStop failed due to:", e);
		}
		if (result) {
			synchronized (message) {
				try {
					message.wait(1000 * 10);
				} catch (InterruptedException e) {
					log.warn("moduleStop exception:", e);
				}
			}
		}
		
		if (executorServiceManager != null) {
			executorServiceManager.shutdown(senderThreadPool);
			executorServiceManager.shutdown(verSenderThreadPool);
			executorServiceManager.shutdown(receiverThreadPool);
		}
		
		SystemPdu response = removeResp(message.getSequenceNumber());
		if (response == null
				|| SystemPdu.COMMAND_MODULE_STOP_ACK != response.getCommandId()) {
			log.info("Module stop timeout when wait response !");
		}
	}

	/**
	 * tryAddSession to MGT module
	 * 
	 * @param uri
	 */
	public SystemPdu applyNewSession(boolean isServer,
			ConnectionInfo connectionInfo, TransactionURI uri) {
		if (!isEnableSysMgt) {
			return null;
		}
		SystemPdu message = null;
		if (isServer) {
			message = new InBindRequest(connectionInfo, uri);
		} else {
			message = new OutBindRequest(connectionInfo, uri);
		}
		boolean isRegister = SystemSessionFactory.getInstance().isRegister();
		SystemPdu response = null;
		if (isRegister) {
			try {
				boolean result = bufferMonitor.put(message.getSequenceNumber(), message);
				if(result){
					result = send(message);
					if(!result){
						bufferMonitor.remove(message.getSequenceNumber());
						return null;
					}
				}else{
					return null;
				}
			} catch (Exception e) {
				log.warn("applyNewSession failed due to:", e);
				bufferMonitor.remove(message.getSequenceNumber());
				return null;
			}
		} else {
			log.warn("Didn't register before applyNewSession.");
		}
		synchronized (message) {
			try {
				message.wait(responseWaitTime);
			} catch (InterruptedException e) {
				log.warn("applyNewSession exception:", e);
			}
		}
		response = this.removeResp(message.getSequenceNumber());
		if (response == null
				|| (!(SystemPdu.COMMAND_OUT_BIND_ACK == response.getCommandId()))) {
			log.warn("applyNewSession timeout when wait response!");
		}
		return response;
	}

	/**
	 * disconnected, notify to MGT module
	 */
	public void clearSession(ConnectionInfo connectionInfo,TransactionURI uri) {
		if (!isEnableSysMgt) {
			return;
		}
		ConnectionStatusNotification message = new ConnectionStatusNotification(
				connectionInfo, uri);
		message.setAction(SystemConstants.DISCONNECTED_ACTION);
		log.debug("clearSession msg:{}", message);
		try {
			boolean result = bufferMonitor.put(message.getSequenceNumber(), message);
			if(result){
				result = send(message);
				if (!result) {
					bufferMonitor.remove(message.getSequenceNumber());
					message.nextRetry();
					putMessage(message);
				}
			}else{
				message.nextRetry();
				putMessage(message);
			}
			
		} catch (Exception e) {
			bufferMonitor.remove(message.getSequenceNumber());
			putMessage(message);
			log.warn("clearSession failed due to:", e);
		}
	}

	/**
	 * apply db session
	 */
	public boolean applyDBSession() {
		SystemPdu message = SystemPdu
				.createPdu(SystemPdu.COMMAND_DB_OPERATION_REQUEST);

		putMessage(message);

		synchronized (message) {
			try {
				message.wait(responseWaitTime);
			} catch (InterruptedException e) {
				log.warn("applyDBSession exception:", e);
			}
		}
		SystemPdu response = this.removeResp(message.getSequenceNumber());
		if (response == null
				|| SystemPdu.COMMAND_DB_OPERATION_ACK != response
						.getCommandId()) {
			log.info("applyDBSession timeout when wait response!");
			return false;
		}
		int rspCode = ((DBOperationAck) response).getResponseCode();
		return rspCode == 0;
	}
	/**
	 * connectedConfirm for in bind
	 * 
	 * @param msgResponse
	 */
	public void inBindConfirm(TransactionURI transaction,int ssid, int responseCode) {
		ConnectionConfirm message = new ConnectionConfirm(true, ssid, responseCode);
		if(transaction!=null){
			message.setUuid(transaction.toString());
		}
		
		try {
			boolean result = bufferMonitor.put(message.getSequenceNumber(), message);
			if(result){
				result = send(message);
				if (!result) {
					bufferMonitor.remove(message.getSequenceNumber());
					message.nextRetry();
					putMessage(message);
				}
			}else{
				message.nextRetry();
				putMessage(message);
			}
			
		} catch (Exception e) {
			bufferMonitor.remove(message.getSequenceNumber());
			putMessage(message);
			log.warn("inBindConfirm {} failed due to:", message.toString(), e);
		}
		
	}

	/**
	 * connectedConfirm for out bind
	 * 
	 * @param msgResponse
	 *            
	 */
	public void outBindConfirm(boolean isSuccess, SystemPdu msgResponse,int ssid) {
		if (!isEnableSysMgt || msgResponse == null) {
			return;
		}
		int respCode = 1;
		if (isSuccess) {
			respCode = 0;
		}
		ConnectionConfirm message = new ConnectionConfirm(msgResponse, respCode);
		message.setSsid(ssid);
		
		try {
			boolean result = bufferMonitor.put(message.getSequenceNumber(), message);
			if(result){
				result = send(message);
				if (!result) {
					bufferMonitor.remove(message.getSequenceNumber());
					message.nextRetry();
					putMessage(message);
				}
			}else{
				message.nextRetry();
				putMessage(message);
			}
			
		} catch (Exception e) {
			bufferMonitor.remove(message.getSequenceNumber());
			putMessage(message);
			log.warn("outBindConfirm {} failed due to:", message.toString(), e);
		}
		
	}

	/**
	 * handle response
	 * 
	 * @param response
	 */
	public void handleResponse(SystemPdu response) {
		int msgType = response.getCommandId();
//		if (isMGT || (response.getCommandId()!=SystemPdu.COMMAND_ACK)){
//			log.info("Receive the ack ({}) from {}", response, moduleName);
//		}
		
		// SYS -> protocol module Ack
		if (SystemPdu.COMMAND_APPLY_IN_THROTTLE_QUOTA_ACK == msgType) {
			processApplyInThrottleQuotaAck(response);
			return;
		}
		
		if (!moduleManager.isAliveModule(moduleName)) {
			moduleManager.updateModuleStatus2Up(moduleName);
		}
		
		if (SystemPdu.COMMAND_OUT_BIND_ACK == msgType
				|| SystemPdu.COMMAND_MODULE_STOP_ACK == msgType
				|| SystemPdu.COMMAND_DB_OPERATION_ACK == msgType) {
			SystemPdu origMsg = bufferMonitor.remove(response.getSequenceNumber());// remove request
			if (origMsg == null) {
				log.info("Already received the ack,so ignored!");
				return;
			} else {
				putResp(response.getSequenceNumber(), response);
				synchronized (origMsg) {
					origMsg.notifyAll();
				}
			}
		}else if(SystemPdu.COMMAND_QUERY_HTTP_ACK == msgType){
			putQueryFlagResp(response.getUuid(), response);		
		}else if(SystemPdu.COMMAND_CONNECTION_STATUS_NOTIFICATION_ACK == msgType){
			SystemPdu oriMsg = bufferMonitor.remove(response.getSequenceNumber());
			if(((ConnectionStatusNotificationAck)response).getResponseCode()==2&&oriMsg!=null){	
				oriMsg.getHeader().setConfFileVersion(GmmsUtility.getInstance().getCustomerManager().getConfFileVersion());
				this.setVerMessage(oriMsg);								
			}else if(((ConnectionStatusNotificationAck)response).getResponseCode()==1&&oriMsg!=null){
//			    oriMsg.nextRetry();
				bufferMonitor.put(oriMsg.getSequenceNumber(), oriMsg);
			}
		} else if (SystemPdu.COMMAND_CONNECTION_CONFIRM_ACK == msgType) {
			SystemPdu oriMsg = bufferMonitor.remove(response.getSequenceNumber());
			if(((ConnectionConfirmAck)response).getResponseCode() == 2 && oriMsg != null){	
				oriMsg.getHeader().setConfFileVersion(GmmsUtility.getInstance().getCustomerManager().getConfFileVersion());
				this.setVerMessage(oriMsg);								
			}else if(((ConnectionConfirmAck)response).getResponseCode() == 1 && oriMsg != null){
				bufferMonitor.put(oriMsg.getSequenceNumber(), oriMsg);
			}
			
		} else {
			bufferMonitor.remove(response.getSequenceNumber());// remove request
		}

		clearActiveTest();
	}
	
	private void processApplyInThrottleQuotaAck(SystemPdu response) {
		try {
			ApplyInThrottleQuotaAck ack = (ApplyInThrottleQuotaAck)response;
			int sysLoad = ack.getSysLoadPercent();
			if (log.isTraceEnabled()) {
				log.trace("System load is {}%", sysLoad);
			}
			int ssid = ack.getSsid();
			A2PCustomerInfo cust = gmmsUtility.getCustomerManager().getCustomerBySSID(ssid);
			boolean canReApplyFlag = true;
			
			ConcurrentMap<Integer, ThrottlingTimemark> incomingThrottleCache = throttlingControl.getIncomingThrottlingControlCache();
			// in case of reload customer info
			if (incomingThrottleCache.get(ssid) != null) {
				if (cust.isSmsOptionDisableAutoAdjustThrottlingNumFlag()) {
					log.warn("Ssid:{} didn't need auto adjust Throttling Number as the SMSOptionDisableAutoAdjustThrottlingNumFlag value is true", cust.getSSID() );
					return;
				}
				int currentThrottlingNum = incomingThrottleCache.get(ssid).getDateArraySize();
				int newThrottlingNum = (int) (currentThrottlingNum*(1+(100-sysLoad)/2/100f));
				int maxThrottlingNum = gmmsUtility.getMaxCustIncomingThresholdMagnification() * cust.getConfigedIncomingThrottlingNum();
				int sysIncomingThreshold = gmmsUtility.getSystemIncomingThreshold();
				// check
				if (newThrottlingNum > maxThrottlingNum	|| newThrottlingNum > sysIncomingThreshold) {
					log.warn("Ssid:{} reach MaxCustIncomingThresholdMagnification or SystemIncomingThreshold.", cust.getSSID());
					if (maxThrottlingNum > sysIncomingThreshold) {
						newThrottlingNum = sysIncomingThreshold;
					} else {
						newThrottlingNum = maxThrottlingNum;
					}
					canReApplyFlag = false;
				}
				
				if (newThrottlingNum == currentThrottlingNum) {
					canReApplyFlag = false;
				} else {
					// set the new quota to cust
					incomingThrottleCache.put(ssid, new ThrottlingTimemark(newThrottlingNum));
					if (log.isInfoEnabled()) {
						log.info("Ssid: {} incoming throttlingNum changes, currentThrottlingNum={}, new value is {}",
								ssid, currentThrottlingNum, newThrottlingNum);
					}
				}
				
				// no matter whether quota changes, add the quota to expire map, to make sure pplyInThrottleFlag can be reset
				ThrottlingControl.getInstance().getExpireThottleQuotaMap().put(ssid, System.currentTimeMillis());
			}
			
			// reset flag, enable to re-apply
			if (canReApplyFlag) {
				cust.setApplyInThrottleFlag(canReApplyFlag);
			}
		} catch (Exception e) {
			log.error(e, e);
		}
		
		
	}

	/**
	 * response buffer
	 * 
	 * @param key
	 * @param message
	 * @return
	 */
	public boolean putResp(Integer key, SystemPdu message) {
		responseBuffer.put(key, message);
		return true;
	}

	public boolean putQueryFlagResp(String key, SystemPdu message) {
		responseQueryFlagBuffer.put(key, message);
		return true;
	}
	/**
	 * remove response
	 * 
	 * @param key
	 * @return
	 */
	public SystemPdu removeResp(Integer key) {
		SystemPdu msg = null;
		synchronized (responseBuffer) {
			if (responseBuffer.containsKey(key)) {
				msg = responseBuffer.remove(key);
			}
		}
		return msg;
	}
	
	public SystemPdu removeQueryFlagResp(String key) {
		SystemPdu msg = null;
		synchronized (responseQueryFlagBuffer) {
			if (responseQueryFlagBuffer.containsKey(key)) {
				msg = responseQueryFlagBuffer.remove(key);
			}
		}
		return msg;
	}

	/**
	 * Handle the timeout message
	 * 
	 * @param message
	 *            SystemMessage
	 */
	public void timeout(SystemPdu message) {
		if (message == null) {
			return;
		}
		int retryTimes = message.getRetryTimes();
		if (retryTimes >= maxRetryTimes) {
			log.warn("SystemMessage {} is sent to {} unsuccessfully", message
					.getUuid(), moduleName);
		} else {
			log.info("The message({}:{}) is retried {} times to {}", message
					.getUuid(), message.getCommandType(), retryTimes, moduleName);
			message.nextRetry();
			try {
				if (!checkRetry(message)) {
					bufferMonitor.remove(message.getSequenceNumber());
					return;
				}
				
				if (!send(message)) {
					bufferMonitor.put(message.getSequenceNumber(), message);
				}
			} catch (IOException e) {
				log.warn("Send timeout message failed:", e);
			}

		}
	}

	public SystemPdu parsePDU(TcpByteBuffer buffer) {

		if (buffer == null) {
			return null;
		}
		SystemPdu pdu = null;
		try {
			pdu = SystemPdu.createPdu(buffer);
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
	
	/**
	 * ReceiverThread
	 * 
	 * @author jianmingyang
	 * 
	 */
	class ReceiverThread implements Runnable {
		private Object obj = null;
		public ReceiverThread(Object obj) {
			this.obj = obj;
		}
		
		public void run() {
			if (obj != null) {
				try {
					TcpByteBuffer buffer = (TcpByteBuffer) obj;
					SystemPdu pdu = parsePDU(buffer);
					if (pdu != null) {
						receive(pdu);
					}
				} catch (Exception e) {
					log.info("Receive Error PDU and the session name is {}, {}", sessionName, e.getMessage());
				}
			}
		}
	}
	
	public boolean isKeepRunning() {
		return keepRunning;
	}

	public long getLastActivity() {
		return lastActivity;
	}

	public Object getMutex() {
		return mutex;
	}

	public TCPIPConnection getConnection() {
		return connection;
	}

	public Socket getSocket() {
		return socket;
	}

	public String getServerAddress() {
		return serverAddress;
	}

	public int getPort() {
		return port;
	}

	public GmmsUtility getGmmsUtility() {
		return gmmsUtility;
	}

	public String getName() {
		return moduleName;
	}

	public SessionThread getSessionThread() {
		return sessionThread;
	}

	public SystemMessageBuffer getMessageBuffer() {
		return bufferMonitor;
	}

	public MessageHandler getHandler() {
		return handler;
	}

	/**
	 * set MessageHandler
	 * 
	 * @param handler
	 */
	public void setHandler(MessageHandler handler) {
		if (handler != null) {
			this.handler = handler;
		}
	}

	public void setKeepRunning(boolean keepRunning) {
		this.keepRunning = keepRunning;
	}

	public void setLastActivity(long lastActivity) {
		this.lastActivity = lastActivity;
	}

	public void setMutex(Object mutex) {
		this.mutex = mutex;
	}

	public void setConnection(TCPIPConnection connection) {
		this.connection = connection;
	}

	public void setSocket(Socket socket) {
		this.socket = socket;
	}

	public void setServerAddress(String serverAddress) {
		this.serverAddress = serverAddress;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void setGmmsUtility(GmmsUtility gmmsUtility) {
		this.gmmsUtility = gmmsUtility;
	}

	public void setName(String name) {
		this.moduleName = name;
	}

	public void setFailActiveTest(int failActiveTest) {
		this.failActiveTest = failActiveTest;
	}

	public void clearActiveTest() {
		synchronized (mutex) {
			failActiveTest = 0;
		}
	}

	public boolean isServer() {
		return isServer;
	}

	public String getModuleName() {
		return moduleName;
	}

	public void setModuleName(String moduleName) {
		this.moduleName = moduleName;
	}

	public ConnectionStatus getStatus() {
		return status;
	}

	public void setStatus(ConnectionStatus status) {
		this.status = status;
	}
	
	public void setVerMessage(SystemPdu msg) {
		putVerMessage(msg);
	}
	
	/**
	 * Check whether need to send retry message from SystemMessageBuffer.
	 * It is used to avoid retry conflict.
	 * @param systemMessage
	 * @return true: should resend pdu </br>
	 *         false: should abandon pdu
	 */
	private boolean checkRetry(SystemPdu systemMessage) {
		
		String module = System.getProperty("module");
		String moduleType = ModuleManager.getInstance().getFullModuleType(module);
		
		try {
			// check to avoid retry ConnectionStatusNotification conflict from MGT
			if(systemMessage.getCommandId() == SystemPdu.COMMAND_CONNECTION_STATUS_NOTIFICATION
					&& (SystemConstants.MGT_MODULE_TYPE.equalsIgnoreCase(moduleType))){
				ConnectionStatusNotification notification = (ConnectionStatusNotification)systemMessage;
				TransactionURI transURI = TransactionURI.fromString(notification.getUuid());
				int ssid = notification.getSsid();
				String action = notification.getAction();
				ConnectionManagementForMGT connectionManager = ConnectionManagementForMGT.getInstance();
				Session session = connectionManager.getSession(ssid, transURI);
				if(session == null || session.getStatus() == ConnectionStatus.DISCONNECT
						|| session.getStatus() == ConnectionStatus.RETRY){
					// session is not exist in MGT now, should not send connect notification to other modules
					if(SystemConstants.CONNECTED_ACTION.equalsIgnoreCase(action)){
						log.info("({}) to {} canceled due to session already removed", notification, moduleName);
						return false;
					}
				}else if(session.getStatus() == ConnectionStatus.CONNECT ||
						 session.getStatus() == ConnectionStatus.RECOVER){
					// session is in MGT now, should not send disconnect notification to other modules
					if(SystemConstants.DISCONNECTED_ACTION.equalsIgnoreCase(action)){
						log.info("({}) to {} canceled", notification, moduleName);
						return false;
					}
				}
			}
			
			// check to avoid retry ConnectionStatusNotification conflict from smpp modules
			if(systemMessage.getCommandId() == SystemPdu.COMMAND_CONNECTION_STATUS_NOTIFICATION
					&& (SystemConstants.MULTISMPPSERVER_MODULE_TYPE.equalsIgnoreCase(moduleType)
							|| SystemConstants.MULTISMPPCLIENT_MODULE_TYPE.equalsIgnoreCase(moduleType)
							|| SystemConstants.SSLSMPPSERVER_MODULE_TYPE.equalsIgnoreCase(moduleType)
							|| SystemConstants.SSLSMPPCLIENT_MODULE_TYPE.equalsIgnoreCase(moduleType))){
				ConnectionStatusNotification notification = (ConnectionStatusNotification)systemMessage;
				TransactionURI transURI = TransactionURI.fromString(notification.getUuid());
				String connectionName = transURI.getConnectionName();
				int ssid = notification.getSsid();
				String action = notification.getAction();
				A2PCustomerInfo custInfo =  gmmsUtility.getCustomerManager().getCustomerBySSID(ssid);
				ConnectionManager connectionManager = ConnectionManagementForFunction.getInstance().getConnectionManager(custInfo, connectionName);
				Session session = connectionManager.getSession(transURI);
				if(session == null || session.getStatus() == ConnectionStatus.DISCONNECT
						|| session.getStatus() == ConnectionStatus.RETRY){
					// session is not exist in protocol module now, should not send connect notification to MGT
					if(SystemConstants.CONNECTED_ACTION.equalsIgnoreCase(action)){
						log.info("({}) to {} canceled due to session already removed", notification, moduleName);
						return false;
					}

				} else if(session.getStatus() == ConnectionStatus.CONNECT ||
						 session.getStatus() == ConnectionStatus.RECOVER){
					// session is in protocol module now, should not send disconnect notification to MGT
					if(SystemConstants.DISCONNECTED_ACTION.equalsIgnoreCase(action)){
						log.info("({}) to {} canceled", notification, moduleName);
						return false;
					}
				}
			}
			
			// check to avoid ConnectionConfirm(inBind/outBind) conflict sent from smpp modules
			if (systemMessage.getCommandId() == SystemPdu.COMMAND_CONNECTION_CONFIRM
					&& (SystemConstants.MULTISMPPSERVER_MODULE_TYPE.equalsIgnoreCase(moduleType)
							|| SystemConstants.MULTISMPPCLIENT_MODULE_TYPE.equalsIgnoreCase(moduleType)
							|| SystemConstants.SSLSMPPSERVER_MODULE_TYPE.equalsIgnoreCase(moduleType)
							|| SystemConstants.SSLSMPPCLIENT_MODULE_TYPE.equalsIgnoreCase(moduleType))) {
				ConnectionConfirm confirm = (ConnectionConfirm)systemMessage;
				TransactionURI transURI = TransactionURI.fromString(confirm.getUuid());
				String connectionName = transURI.getConnectionName();
				int ssid = confirm.getSsid();
				int statusCode = confirm.getResponseCode();
				A2PCustomerInfo custInfo =  gmmsUtility.getCustomerManager().getCustomerBySSID(ssid);
				ConnectionManager connectionManager = ConnectionManagementForFunction.getInstance().getConnectionManager(custInfo, connectionName);
				Session session = connectionManager.getSession(transURI);
				if(session == null || session.getStatus() == ConnectionStatus.DISCONNECT
						|| session.getStatus() == ConnectionStatus.RETRY){
					// session is not exist in protocol module now, should not send OK confirm to MGT
					if(statusCode == 0) {
						log.info("({}) to {} canceled due to session already removed", confirm, moduleName);
						return false;
					}
				} else if(session.getStatus() == ConnectionStatus.CONNECT ||
						 session.getStatus() == ConnectionStatus.RECOVER){
					// session is connect in protocol module now, should not send fail confirm to MGT
					if(statusCode == 1){
						log.info("({}) to {} canceled", confirm, moduleName);
						return false;
					}
				}
			}
			
		} catch (Exception e) {
			log.warn("SystemSession.checkRetry exception", e);
		}
		
		return true;
	}
	
	public class SystemMessageSender implements Runnable {

		private SystemPdu systemMessage;

		public SystemMessageSender(SystemPdu systemMessage) {
			this.systemMessage = systemMessage;
		}

		public void run() {
			try {
				if (!sessionFactory.isRegister()) {
					Thread.sleep(100);
				}
				if (systemMessage != null) {
					if (systemMessage.isRequest()) {
						// check to avoid conflict
						if (!checkRetry(systemMessage)) {
							return;
						}
						
						// buffer system request message;
						while (!bufferMonitor.put(systemMessage.getSequenceNumber(), systemMessage))
							;
					}
					boolean flag = false;
					flag = send(systemMessage);
					if (!flag) {
						if (systemMessage.isRequest()) {
							bufferMonitor.remove(systemMessage.getSequenceNumber());
						}
						
						log.info("The message({}:{}) is retried {} times to {}", systemMessage.getUuid(), systemMessage.getCommandType(), systemMessage.getRetryTimes(), moduleName);
						systemMessage.nextRetry();
						Thread.sleep(100);
						putMessage(systemMessage);
						
					}
				}
			} catch (IOException e) {
				log.warn(e, e);
				if (systemMessage != null) {
					bufferMonitor.remove(systemMessage.getSequenceNumber());
					try {
						Thread.sleep(100);
					} catch (InterruptedException e1) {
						//
					}
					systemMessage.nextRetry();
					putMessage(systemMessage);
				}
			} catch (InterruptedException e1) {
				//
			}
			catch (Exception ex) {
				log.warn("SystemMessageSender exception", ex);
			}
		}

	}
	
	public boolean putMessage(SystemPdu systemMessage) {
		if (systemMessage == null) {
			return false;
		}
		if (log.isTraceEnabled()) {
			log.trace("submit to SystemMessageSender thread pool: {}", systemMessage);
		}
		try {
			if(systemMessage.getRetryTimes() > maxRetryTimes){
				log.warn("SystemMessage {} is sent to {} unsuccessfully", systemMessage.getUuid(), moduleName);
				return false;
			}
			senderThreadPool.execute(new SystemMessageSender(systemMessage));
		} catch (Exception e) {
			if (log.isInfoEnabled()) {
				log.info("send {} exception: {}", systemMessage, e);
			}
			return false;
		}
		
		return true;
	}
	
	public boolean putVerMessage(SystemPdu systemMessage) {
		if (systemMessage == null) {
			return false;
		}
		if (log.isTraceEnabled()) {
			log.trace("submit to VersionInconformityMessageSender thread pool: {}", systemMessage);
		}
		
		try {
			verSenderThreadPool.execute(new VersionInconformityMessageSender(systemMessage));
		} catch (Exception e) {
			if (log.isInfoEnabled()) {
				log.info("send {} exception: {}", systemMessage, e);
			}
			return false;
		}
		
		return true;
		
	}
	
	public class VersionInconformityMessageSender implements Runnable{

		private SystemPdu systemMessage;
		
	    public VersionInconformityMessageSender(SystemPdu systemMessage) {
	        this.systemMessage = systemMessage;
	    }

	    public void run(){
	    	try{
            	if(!sessionFactory.isRegister()){
            		Thread.sleep(100);
                }
                if(systemMessage != null){
                	Thread.sleep(2000);  
                	if(systemMessage.isRequest()){
                		// check to avoid conflict
						if (!checkRetry(systemMessage)) {
							return;
						}
						
            			//buffer system request message;
                        while(!bufferMonitor.put(systemMessage.getSequenceNumber(),systemMessage));
	            	}
                	boolean flag = false;
                	flag = send(systemMessage);
                	if(!flag){
                		if(systemMessage.isRequest()){
                			bufferMonitor.remove(systemMessage.getSequenceNumber());
                		}
                		Thread.sleep(100);
                		setVerMessage(systemMessage);
                		
                	}               	
                }
            }catch(IOException e){
//                log.warn(e.getMessage());
                if(systemMessage != null){
                	bufferMonitor.remove(systemMessage.getSequenceNumber());
                	try {
						Thread.sleep(100);
					} catch (InterruptedException e1) {
						//
					}
                	setVerMessage(systemMessage);
                	
                }
            }catch (InterruptedException e1) {
				//
			}catch(Exception ex){
				log.warn("VersionInconformityMessageSender exception", ex);
            }
	    }

	}
	
	/**
	 * send query Flag pdu to system
	 * 
	 * @param uri
	 */
	public SystemPdu queryFlag(SystemPdu message) {
		if (!isEnableSysMgt) {
			return null;
		}	
		int count = 0;
		SystemPdu response = null;		
			try {
				send(message);
			} catch (IOException e1) {
				log.error("send query flag pdu error!");
			}
		while(response == null && count<50){
			synchronized (message) {
				try {
					message.wait(100);
				} catch (InterruptedException e) {
					log.warn("send query flag pdu exception:", e);
				}
			}
			count++;
			response = this.removeQueryFlagResp(message.getUuid());		
		}		
		return response;
	}
}
