package com.king.gmms.connectionpool.connection;

import java.util.Map;
import java.util.Observer;
import java.util.Observable;
import java.util.HashMap;

import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.ConnectionStatus;
import com.king.gmms.connectionpool.session.*;
import com.king.gmms.domain.ConnectionInfo;
import com.king.gmms.strategy.IndexBalanceStrategy;
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
public class MultiServerConnectionImpl extends AbstractMultiConnection
		implements Observer {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(MultiServerConnectionImpl.class);

	public MultiServerConnectionImpl() {
		this(new IndexBalanceStrategy(), false);
	}

	public MultiServerConnectionImpl(boolean isCreatURIMap) {
		this(new IndexBalanceStrategy(), isCreatURIMap);
	}

	public MultiServerConnectionImpl(IndexStrategy strategy,
			boolean isCreatURIMap) {
		super(strategy, isCreatURIMap);
	}

	public void update(Observable o, Object arg) {

		if (o instanceof Session && arg instanceof ConnectionStatus) {
			ConnectionStatus st = (ConnectionStatus) arg;
			Session session = (Session) o;
			switch (st) {
			case INITIAL:
				break;
			case CONNECT:
				// handleConnection(session);
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
			if (obj != null) {
				if (sessionNum > 0) {
					sessionNum -= 1;
				}
				session.deleteObserver(this);
			}
			this.removeSessionFURIMap(session.getTransactionURI());
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
				connectedSessions.put(sessionName, session);
				sessionNum += 1;
				if (connectedSessions.size() == 1) {
					setStatus(ConnectionStatus.CONNECT);
				}
				putSession2URIMap(session);
			}
			session.addObserver(this);
		}
		return true;
	}

	public boolean deleteSession(Session session) {
		if (session == null) {
			return false;
		}
		String sessionName = session.getSessionName();
		if (connectedSessions.containsKey(sessionName)) {
			synchronized (mutex) {
				if (connectedSessions.remove(sessionName) != null) {
					if (sessionNum > 0) {
						sessionNum -= 1;
					}
				}
				session.deleteObserver(this);
				this.removeSessionFURIMap(session.getTransactionURI());
				if (connectedSessions.size() <= 0) {
					setStatus(ConnectionStatus.DISCONNECT);
				}
			}
		}
		return true;
	}

}
