package com.king.gmms.connectionpool.connection;

import java.util.Map;
import java.util.HashMap;
import java.util.Observer;
import java.util.Observable;

import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.*;
import com.king.gmms.connectionpool.session.*;
import com.king.gmms.domain.ConnectionInfo;
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
public class NodeClientConnection extends AbstractMultiConnection implements
		Observer {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(NodeClientConnection.class);

	private Map disconnectSessions = null;
	private Map retrySessions = null;
	private Map recoverSessions = null;

	public NodeClientConnection() {
		this(new IndexRandomStrategy(), false);
	}

	public NodeClientConnection(boolean isCreatURIMap) {
		this(new IndexRandomStrategy(), isCreatURIMap);
	}

	public NodeClientConnection(IndexStrategy strategy, boolean isCreatURIMap) {
		super(strategy, isCreatURIMap);
		retrySessions = new HashMap();
		recoverSessions = new HashMap();
		disconnectSessions = new HashMap();
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
			case RETRY:
				handleRetry(session);
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

	public void handleRetry(Session session) {
		String sessionName = session.getSessionName();
		if (retrySessions.containsKey(sessionName)) {
			return;
		} else {
			synchronized (mutex) {
				if (connectedSessions.remove(sessionName) != null) {
					if (sessionNum > 0) {
						sessionNum -= 1;
					}
				}
				retrySessions.put(sessionName, session);
			}
		}
	}

	public void handleRecover(Session session) {
		String sessionName = session.getSessionName();
		if (recoverSessions.containsKey(sessionName)) {
			return;
		} else {
			synchronized (mutex) {
				disconnectSessions.remove(sessionName);
				recoverSessions.put(sessionName, session);
				sessionNum += 1;
			}
		}
	}

	private void handleConnection(Session session) {
		String sessionName = session.getSessionName();
		Object obj = null;
		if (!connectedSessions.containsKey(sessionName)) {
			synchronized (mutex) {
				obj = retrySessions.remove(sessionName);
				if (obj == null) {
					obj = disconnectSessions.remove(sessionName);
					if (obj == null) {
						if (recoverSessions.remove(sessionName) == null) {
							sessionNum += 1;
						}
					}
				}
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
			if (disconnectSessions.size() <= 0 && recoverSessions.size() <= 0) {
				if(log.isInfoEnabled()){
					log.info("The connection({}) status is set:  CONNECT and the original status is DISCONNECT",
								info.getConnectionName());
				}
				setStatus(ConnectionStatus.CONNECT);
			}
			break;
		}
	}

	private void handleDisconnect(Session session) {
		String sessionName = session.getSessionName();
		Object obj = null;
		if (!disconnectSessions.containsKey(sessionName)) {
			synchronized (mutex) {
				obj = retrySessions.remove(sessionName);
				if (obj == null) {
					if (recoverSessions.remove(sessionName) != null) {
						if (sessionNum > 0) {
							sessionNum -= 1;
						}
					}
				}
				disconnectSessions.put(sessionName, session);
			}
		}
		switch (getStatus()) {
		case INITIAL:
			if (connectedSessions.size() <= 0 && retrySessions.size() <= 0) {
				if(log.isInfoEnabled()){
					log.info("The connection({}) status is modified DISCONNECT from INITIAL",
								info.getConnectionName());
				}
				setStatus(ConnectionStatus.DISCONNECT);
			}
			break;
		case CONNECT:
			if (connectedSessions.size() <= 0 && retrySessions.size() <= 0) {
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

	public boolean addSession(Session session) {
		if (session == null) {
			return false;
		}
		synchronized (mutex) {
			if (session.getStatus().equals(ConnectionStatus.CONNECT)
					|| session.getStatus().equals(ConnectionStatus.RECOVER)) {
				if (info.getSessionNum() > 0
						&& sessionNum + 1 > info.getSessionNum()) {
					return false;
				}
				update(session, session.getStatus());
			}
			putSession2URIMap(session);
		}
		session.addObserver(this);
		return true;
	}

	public boolean deleteSession(Session session) {
		if (session == null) {
			return false;
		}
		String sessionName = session.getSessionName();
		ConnectionStatus st = session.getStatus();
		Object obj = null;
		synchronized (mutex) {
			switch (st) {
			case INITIAL:
				break;
			case CONNECT:
				obj = connectedSessions.remove(sessionName);
				if (obj == null) {
					retrySessions.remove(sessionName);
				} else {
					if (sessionNum > 0) {
						sessionNum -= 1;
					}
				}
				if (sessionNum <= 0 && retrySessions.size() <= 0) {
					if(log.isTraceEnabled()){
						log.trace("The connection({}) status is modified DISCONNECT from CONNECT",
									info.getConnectionName());
					}
					setStatus(ConnectionStatus.DISCONNECT);
				}
				break;
			case RECOVER:
				obj = recoverSessions.remove(sessionName);
				if (obj != null) {
					if (sessionNum > 0) {
						sessionNum -= 1;
					}
				} else {
					disconnectSessions.remove(sessionName);
				}

				if (sessionNum <= 0 && retrySessions.size() < 0) {
					setStatus(ConnectionStatus.DISCONNECT);
				}
				break;
			case DISCONNECT:
				obj = disconnectSessions.remove(sessionName);
				if (obj == null) {
					if (recoverSessions.remove(sessionName) != null) {
						if (sessionNum > 0) {
							sessionNum -= 1;
						}
					}
				}
				if (sessionNum > 0 && disconnectSessions.size() <= 0
						&& recoverSessions.size() <= 0) {
					log
							.trace(
									"The connection({}) status is modified CONNECT from DISCONNECT",
									info.getConnectionName());
					setStatus(ConnectionStatus.CONNECT);
				}
				break;
			default:
				return false;
			}
			removeSessionFURIMap(session.getTransactionURI());
		}
		session.deleteObserver(this);
		return true;
	}

}
