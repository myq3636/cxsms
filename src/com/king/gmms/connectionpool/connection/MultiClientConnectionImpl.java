package com.king.gmms.connectionpool.connection;

import java.util.Map;
import java.util.Observable;
import java.util.HashMap;
import java.util.Observer;

import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.*;
import com.king.gmms.connectionpool.connection.*;
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
public class MultiClientConnectionImpl extends AbstractMultiConnection
		implements Observer {
	private SystemLogger log = SystemLogger
			.getSystemLogger(MultiClientConnectionImpl.class);

	public MultiClientConnectionImpl(boolean isCreatURIMap) {
		this(new IndexBalanceStrategy(), isCreatURIMap);
	}

	public MultiClientConnectionImpl() {
		this(new IndexBalanceStrategy(), false);
	}

	public MultiClientConnectionImpl(IndexStrategy strategy,
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
				handleConnect(session);
				break;
			case DISCONNECT:
				handleDisconnect(session);
				break;
			}
		}
	}

	private void handleConnect(Session session) {
		String sessionName = session.getSessionName();
		if (!connectedSessions.containsKey(sessionName)) {
			synchronized (mutex) {
				connectedSessions.put(sessionName, session);
				sessionNum += 1;
				switch (getStatus()) {
				case INITIAL:
					if(log.isInfoEnabled()){
						log.info("The connection({}) status is set:  CONNECT and the original status is INITIAL",
									info.getConnectionName());
					}
					setStatus(ConnectionStatus.CONNECT);
					break;
				case DISCONNECT:
					if(log.isInfoEnabled()){
						log.info("The connection({}) status is set:  CONNECT and the original status is DISCONNECT",
									info.getConnectionName());
					}
					setStatus(ConnectionStatus.CONNECT);
					break;
				case CONNECT:
					break;
				}
			}
		}
	}

	private void handleDisconnect(Session session) {
		String sessionName = session.getSessionName();
		synchronized (mutex) {
			if (connectedSessions.remove(sessionName) != null) {
				if (sessionNum > 0) {
					sessionNum -= 1;
				}
			}
			switch (getStatus()) {
			case CONNECT:
				if (connectedSessions.size() <= 0) {
					if(log.isInfoEnabled()){
						log.info("The connection({}) status is modified DISCONNECT from CONNECT",
									info.getConnectionName());
					}
					setStatus(ConnectionStatus.DISCONNECT);
				}
				break;
			case INITIAL:
				if(log.isInfoEnabled()){
					log.info("The connection({}) status is modified DISCONNECT from INITIAL",
								info.getConnectionName());
				}
				setStatus(ConnectionStatus.DISCONNECT);
				break;
			case DISCONNECT:
				break;
			}
		}
	}

	public boolean addSession(Session session) {
		if (session == null) {
			return false;
		}
		String sessionName = session.getSessionName();
		if (!connectedSessions.containsKey(sessionName)) {
			synchronized (mutex) {
				if (session.getStatus().equals(ConnectionStatus.CONNECT)) {
					if (info.getSessionNum() > 0
							&& sessionNum + 1 > info.getSessionNum()) {
						return false;
					}
				}
				update(session, session.getStatus());
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
