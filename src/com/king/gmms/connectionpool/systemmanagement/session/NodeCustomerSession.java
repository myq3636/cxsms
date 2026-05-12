package com.king.gmms.connectionpool.systemmanagement.session;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.ConnectionStatus;
import com.king.gmms.connectionpool.connection.ConnectionManager;
import com.king.gmms.connectionpool.connection.NodeConnectionManagerInterface;
import com.king.gmms.connectionpool.session.Session;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.ConnectionInfo;
import com.king.gmms.domain.MultiNodeCustomerInfo;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.messagequeue.OperatorMessageQueue;
import com.king.gmms.strategy.StrategyType;
import com.king.gmms.util.AbstractTimer;
import com.king.message.gmms.GmmsMessage;

public class NodeCustomerSession extends Session {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(NodeCustomerSession.class);
	protected ConnectionStatus status = ConnectionStatus.INITIAL;
	protected String sessionName = "";
	protected TransactionURI transaction = null;
	protected String moduleName = null;
	protected long lastActivity = 0;
	protected ConnectionManager connectionManager = null;
	protected Object mutex = new Object();
	protected boolean isServer = false;
	protected RecoverTimer timer = null;
	protected int recoverTime = 90000;
	protected A2PCustomerInfo customerInfo = null;

	public NodeCustomerSession(A2PCustomerInfo customerInfo,
			TransactionURI transaction, boolean isServer) {
		this.transaction = transaction;
		status = ConnectionStatus.INITIAL;
		sessionName = transaction.getConnectionName() + transaction.getId();
		moduleName = transaction.getModule().getModule();
		lastActivity = System.currentTimeMillis();
		this.isServer = isServer;
		timer = new RecoverTimer(recoverTime);
		this.customerInfo = customerInfo;
	}

	public TransactionURI getTransaction() {
		return transaction;
	}

	public void setTransaction(TransactionURI transaction) {
		this.transaction = transaction;
	}

	public String getModuleName() {
		return moduleName;
	}

	public void setModuleName(String moduleName) {
		this.moduleName = moduleName;
	}

	public boolean connect() {
		String type = ((MultiNodeCustomerInfo) customerInfo)
				.getSubmitNodePolicy();
		if (!((NodeConnectionManagerInterface) connectionManager)
				.getNodeStatus(transaction.getConnectionName())
				&& StrategyType.getStrategyType(type).equals(
						StrategyType.Primary)) {
			if(log.isInfoEnabled()){
				log.info("The session({}) status is set:  RECOVER and the original status is DISCONNECT",
							sessionName);
			}
			setStatus(ConnectionStatus.RECOVER);
			timer.startTimer();
		} else {
			if(log.isInfoEnabled()){
				log.info("The session({}) status is set:  CONNECT and the original status is DISCONNECT",
							sessionName);
			}
			setStatus(ConnectionStatus.CONNECT);
		}
		return true;
	}

	public boolean createConnection() throws IOException {
		log.warn("Customer session can not create connection");
		return false;
	}

	public int enquireLink() throws Exception {
		log.warn("Customer session can not enquire link");
		return 0;
	}

	public ConnectionInfo getConnectionInfo() {
		log.warn("Customer session can not create connection");
		return null;
	}

	public long getLastActivity() {
		return lastActivity;
	}

	public OperatorMessageQueue getOperatorMessageQueue() {
		log.warn("Customer session did not start message queue");
		return null;
	}

	public String getSessionName() {
		return sessionName;
	}

	public int getSessionNum() {
		// TODO Auto-generated method stub
		return 0;
	}

	public ConnectionStatus getStatus() {
		return status;
	}

	public TransactionURI getTransactionURI() {
		return transaction;
	}

	public boolean isKeepRunning() {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean isServer() {
		return isServer;
	}

	public void setConnectionManager(ConnectionManager connManager) {
		connectionManager = connManager;
	}

	public void setKeepRunning(boolean keepRunning) {
		// TODO Auto-generated method stub

	}

	public void setSessionName(String connectionName) {
		sessionName = connectionName + transaction.getId();
	}

	public void setStatus(ConnectionStatus status) {
		synchronized (mutex) {
			this.status = status;
			setChanged();
			notifyObservers(status);
		}
	}

	public void stop() {
		switch (status) {
		case INITIAL:
			if(log.isInfoEnabled()){
				log.info("The session({}) status is set:  RETRY and the original status is INITIAL",
							sessionName);
			}
			setStatus(ConnectionStatus.DISCONNECT);
			break;
		case CONNECT:
			if(log.isInfoEnabled()){
				log.info("The session({}) status is set:  RETRY and the original status is CONNECT",
							sessionName);
			}
			setStatus(ConnectionStatus.DISCONNECT);
			break;
		case RETRY:
			setStatus(ConnectionStatus.DISCONNECT);
			break;
		case RECOVER:
			if(log.isInfoEnabled()){
				log.info("The session({}) status is set:  DISCONNECT and the original status is RECOVER",
							sessionName);
			}
			setStatus(ConnectionStatus.DISCONNECT);
			timer.stopTimer();
			break;
		case DISCONNECT:
		default:
			break;
		}
	}

	public boolean submit(GmmsMessage msg) throws IOException {
		log.warn("Customer session can not submit message");
		return false;
	}

	public ByteBuffer submitAndRec(GmmsMessage msg) throws IOException {
		log.warn("Customer session can not submit message");
		return null;
	}

	public void connectionUnavailable() {
		stop();
	}

	public void parse(ByteBuffer buffer) {
		// TODO Auto-generated method stub

	}

	public boolean receive(Object obj) {
		log.warn("Customer session can not receive object");
		return false;
	}

	class RecoverTimer extends AbstractTimer {

		public RecoverTimer(long recoverTime) {
			super(recoverTime);
		}

		public void excute() {
			if(log.isInfoEnabled()){
				log.info("The session({}) status is set:  CONNECT and the original status is RECOVER",
							sessionName);
			}
			setStatus(ConnectionStatus.CONNECT);
			stopTimer();
		}
	}

	public boolean isFakeSession() {
		return true;
	}

	@Override
	public void initReceivers() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateReceivers() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void initSenders() {
		// TODO Auto-generated method stub
		
	}
}
