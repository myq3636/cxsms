package com.king.gmms.connectionpool.connection;

import java.util.Observer;
import java.util.Map;
import java.util.HashMap;
import java.util.Observable;

import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.ConnectionStatus;
import com.king.gmms.connectionpool.session.Session;
import com.king.gmms.strategy.IndexRandomStrategy;
import com.king.gmms.strategy.IndexStrategy;

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
public class NodeServerConnection extends AbstractMultiConnection implements
		Observer {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(NodeServerConnection.class);
	private Map recoverSessions = null;

	public NodeServerConnection() {
		this(new IndexRandomStrategy(), false);
	}

	public NodeServerConnection(boolean isCreatURIMap) {
		this(new IndexRandomStrategy(), isCreatURIMap);
	}

	public NodeServerConnection(IndexStrategy strategy, boolean isCreatURIMap) {
		super(strategy, isCreatURIMap);
		recoverSessions = new HashMap();
	}

	public void update(Observable o, Object arg) {

		if (o instanceof Session && arg instanceof ConnectionStatus) {

			ConnectionStatus st = (ConnectionStatus) arg;
			Session session = (Session) o;
			switch (st) {
			case INITIAL:
				break;
			case CONNECT:
				handleConnection(session);
				break;
			case RECOVER:
				handleRecover(session);
				break;
			case DISCONNECT:
				handleDisconnect(session);
				break;
			}

		}

	}

	private void handleDisconnect(Session session) {
		String sessionName = session.getSessionName();
		Object obj = null;

		synchronized (mutex) {
			obj = connectedSessions.remove(sessionName);
			if (obj == null) {
				obj = recoverSessions.remove(sessionName);
			}
			if (obj != null) {
				if (sessionNum > 0) {
					sessionNum -= 1;
				}
				session.deleteObserver(this);
			}
			removeSessionFURIMap(session.getTransactionURI());

		}

		switch (getStatus()) {
		case INITIAL:
			if (connectedSessions.size() <= 0) {
				if(log.isInfoEnabled()){
					log.info("The connection({}) status is modified DISCONNECT from INITIAL",
								info.getConnectionName());
				}
				setStatus(ConnectionStatus.DISCONNECT);
			}
			break;
		case CONNECT:
			if (connectedSessions.size() <= 0) {
				if(log.isInfoEnabled()){
					log.info("The connection({}) status is modified DISCONNECT from CONNECT",
								info.getConnectionName());
				}
				setStatus(ConnectionStatus.DISCONNECT);
			}
			break;
		case DISCONNECT:
			break;
		}
	}

	public void handleRecover(Session session) {
		String sessionName = session.getSessionName();
		if (recoverSessions.containsKey(sessionName)) {
			return;
		} else {
			synchronized (mutex) {
				recoverSessions.put(sessionName, session);
			}
		}
	}

	private void handleConnection(Session session) {
		String sessionName = session.getSessionName();
		if (!connectedSessions.containsKey(sessionName)) {
			synchronized (mutex) {
				recoverSessions.remove(sessionName);
				connectedSessions.put(sessionName, session);
			}
		}

		switch (getStatus()) {
		case INITIAL:
			setStatus(ConnectionStatus.CONNECT);
			if(log.isInfoEnabled()){
				log.info("The connection({}) status is set:  CONNECT and the original status is INITIAL",
								info.getConnectionName());
			}
			break;
		case CONNECT:
			break;
		case DISCONNECT:
			if (recoverSessions.size() <= 0) {
				if(log.isInfoEnabled()){
					log.info("The connection({}) status is set:  CONNECT and the original status is DISCONNECT",
									info.getConnectionName());
				}
				setStatus(ConnectionStatus.CONNECT);
			}
			break;
		}
	}

	/**
	 * addSession
	 * 
	 * @param session
	 *            AbstractSession
	 * @return boolean
	 * @todo Implement.gmms.connectionpool.connection.Connection
	 *       method
	 */
	public boolean addSession(Session session) {
		if (session == null) {
			return false;
		}
		String sessionName = session.getSessionName();
		if (!connectedSessions.containsKey(sessionName)) {
			synchronized (mutex) {
				if (info.getSessionNum() > 0
						&& sessionNum + 1 > info.getSessionNum()) {
					return false;
				}
				update(session, session.getStatus());
				sessionNum += 1;
				putSession2URIMap(session);
			}
			session.addObserver(this);
		}
		return true;
	}

	/**
	 * deleteSession
	 * 
	 * @param session
	 *            AbstractSession
	 * @return boolean
	 * @todo Implement.gmms.connectionpool.connection.Connection
	 *       method
	 */
	public boolean deleteSession(Session session) {
		if (session == null) {
			return false;
		}
		String sessionName = session.getSessionName();
		ConnectionStatus st = session.getStatus();
		synchronized (mutex) {
			switch (st) {
			case INITIAL:
				break;
			case CONNECT:
				if (connectedSessions.containsKey(sessionName)) {
					if (connectedSessions.remove(sessionName) != null) {
						if (sessionNum > 0) {
							sessionNum -= 1;
						}
					}
					session.deleteObserver(this);
					if (sessionNum <= 0) {
						setStatus(ConnectionStatus.DISCONNECT);
					}
				}
				break;
			case RECOVER:
				if (recoverSessions.remove(sessionName) != null) {
					if (sessionNum > 0) {
						sessionNum -= 1;
					}
				}
				session.deleteObserver(this);
				if (sessionNum <= 0) {
					setStatus(ConnectionStatus.DISCONNECT);
				}
				break;
			case DISCONNECT:
				if (this.recoverSessions.containsKey(sessionName)) {
					if (recoverSessions.remove(sessionName) != null) {
						if (sessionNum > 0) {
							sessionNum -= 1;
						}
					}
					session.deleteObserver(this);
				}
				break;
			default:
				break;
			}
			removeSessionFURIMap(session.getTransactionURI());
		}
		return true;
	}
}
