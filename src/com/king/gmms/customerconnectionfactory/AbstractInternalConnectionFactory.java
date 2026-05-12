package com.king.gmms.customerconnectionfactory;

import java.util.concurrent.ConcurrentHashMap;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.connection.ConnectionManager;
import com.king.gmms.connectionpool.session.Session;
import com.king.gmms.messagequeue.OperatorMessageQueue;
import com.king.gmms.strategy.ConnectionInternalSameSessionStrategy;
import com.king.gmms.strategy.ConnectionLoadBalanceStrategy;
import com.king.gmms.strategy.ConnectionOriginalWayStrategy;
import com.king.gmms.strategy.ConnectionPrimaryStrategy;
import com.king.gmms.strategy.ConnectionRandomStrategy;
import com.king.gmms.strategy.ConnectionSameIPStrategy;
import com.king.gmms.strategy.ConnectionSameSessionStrategy;
import com.king.gmms.strategy.ConnectionStrategy;
import com.king.gmms.strategy.StrategyType;
import com.king.message.gmms.GmmsMessage;

public abstract class AbstractInternalConnectionFactory {

	protected GmmsUtility gmmsUtility = null;
	protected ConcurrentHashMap<String, ConnectionManager> module2ClientConnectionManagers;
	protected ConcurrentHashMap<String, ConnectionManager> module2ServerConnectionManagers;
	protected String selfModule = null;
	private static SystemLogger log = SystemLogger
			.getSystemLogger(AbstractInternalConnectionFactory.class);

	protected AbstractInternalConnectionFactory() {
		gmmsUtility = GmmsUtility.getInstance();
		module2ClientConnectionManagers = new ConcurrentHashMap<String, ConnectionManager>();
		module2ServerConnectionManagers = new ConcurrentHashMap<String, ConnectionManager>();
		selfModule = System.getProperty("module");
	}

	public abstract boolean initInternalConnectionFactory(String moduleName);

	public OperatorMessageQueue getMessageQueue(GmmsMessage message,
			String moduleName) {
		if (moduleName == null) {
			return null;
		}
		ConnectionManager connManager = null;
		String messageType = message.getMessageType();
		Session session = null;
		try {
			if (GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(messageType)
					|| GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP
							.equalsIgnoreCase(messageType)
					|| GmmsMessage.MSG_TYPE_DELIVERY_RESP
							.equalsIgnoreCase(messageType)
					|| GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY_RESP
							.equalsIgnoreCase(messageType)) {
				connManager = module2ServerConnectionManagers.get(moduleName);
				ConnectionStrategy responseStrategy = getStrategy(
						"InternalSameSession", connManager, null);
				session = responseStrategy.execute(message);
			} else if (GmmsMessage.MSG_TYPE_SUBMIT
					.equalsIgnoreCase(messageType)
					|| GmmsMessage.MSG_TYPE_DELIVERY
							.equalsIgnoreCase(messageType)
					|| GmmsMessage.MSG_TYPE_DELIVERY_REPORT
							.equalsIgnoreCase(messageType)
					|| GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY
							.equalsIgnoreCase(messageType)) {
				connManager = module2ClientConnectionManagers.get(moduleName);
				ConnectionStrategy newMsgStrategy = getStrategy("loadbalance",
						connManager, null);
				session = newMsgStrategy.execute(message);
			} else if (GmmsMessage.MSG_TYPE_INNER_ACK
					.equalsIgnoreCase(messageType)) {
				connManager = module2ClientConnectionManagers.get(moduleName);
				ConnectionStrategy responseStrategy = getStrategy(
						"InternalSameSession", connManager, null);
				session = responseStrategy.execute(message);
			}
		} catch (Exception e) {
			log.warn(message,e.getMessage());
			return null;
		}
		if (session == null) {
			if(log.isInfoEnabled()){
				log.info(message, "Can't get available session to send "
					+ messageType);
			}
		} else {
			return session.getOperatorMessageQueue();
		}
		return null;
	}

	public boolean manageConnection(String moduleName, Session session) {
		boolean result = false;
		if (moduleName == null || session == null) {
			return result;
		}

		if (session.isServer()) {
			if (module2ServerConnectionManagers.containsKey(moduleName)) {
				ConnectionManager connManager = module2ServerConnectionManagers
						.get(moduleName);
				session.setConnectionManager(connManager);
				session.setSessionName(moduleName);
				return connManager.insertSession(moduleName, session);
			} else {
				log.warn(
								"ServerConnectionManagers didn't contains moduleName:{}",
								moduleName);
			}
		}

		return result;
	}

	public boolean connectionBroken(String moduleName, Session session) {
		boolean result = false;
		if (moduleName == null || session == null) {
			return result;
		}

		if (session.isServer()) {
			if (module2ServerConnectionManagers.containsKey(moduleName)) {
				ConnectionManager connManager = module2ServerConnectionManagers
						.get(moduleName);
				return connManager.deleteSession(moduleName, session);
			} else {
				log.warn("ServerConnectionManagers didn't contains moduleName:{}",
								moduleName);
			}
		}

		return result;
	}

	/**
	 * getStrategy
	 * 
	 * @param policy
	 * @param connMan
	 * @param optionValue
	 * @return
	 */
	private ConnectionStrategy getStrategy(String policy,
			ConnectionManager connMan, String optionValue) {
		StrategyType type = StrategyType.getStrategyType(policy);
		ConnectionStrategy strategy = null;
		switch (type) {
		case Primary:
			strategy = new ConnectionPrimaryStrategy(connMan, optionValue);
			break;
		case OriginalWay:
			strategy = new ConnectionOriginalWayStrategy(connMan);
			break;
		case LoadBalance:
			strategy = new ConnectionLoadBalanceStrategy(connMan);
			break;
		case SameIP:
			strategy = new ConnectionSameIPStrategy(connMan);
			break;
		case Random:
			strategy = new ConnectionRandomStrategy(connMan);
			break;
		case SameSession:
			strategy = new ConnectionSameSessionStrategy(connMan);
			break;
		case InternalSameSession:
			strategy = new ConnectionInternalSameSessionStrategy(connMan);
			break;
		}
		return strategy;
	}
}