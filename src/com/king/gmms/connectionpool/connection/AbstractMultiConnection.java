package com.king.gmms.connectionpool.connection;

import java.util.*;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.*;
import com.king.gmms.connectionpool.session.*;
import com.king.gmms.domain.*;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.strategy.*;

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * <p>Copyright: Copyright (c) 2006</p>
 *
 * <p>Company: </p>
 *
 * @author not attributable
 * @version 1.0
 */
public abstract class AbstractMultiConnection
    extends Observable implements Connection {
    protected IndexStrategy strategy = null;
    protected int sessionNum = 0;
    protected ConnectionStatus status = ConnectionStatus.INITIAL;
    protected ConnectionInfo info = null;
    protected Object mutex = new Object();
    protected Map connectedSessions = null;
    protected BindMode bindMode = BindMode.Transceiver;
    private Map<TransactionURI, Session> transactionURI2Connections = null;
    private GmmsUtility gmmsUtility = null;
    protected String module = null;
    protected boolean isCoreEngine = false;
    protected boolean isSystemManageEnable = false;

    public AbstractMultiConnection(IndexStrategy strategy,boolean isCreatURIMap) {
        this.strategy = strategy;
        if(isCreatURIMap){
            transactionURI2Connections = new HashMap<TransactionURI, Session> ();
        }
        gmmsUtility = GmmsUtility.getInstance();
        module = System.getProperty("module");
        ModuleManager moduleManager = ModuleManager.getInstance();
        isCoreEngine = moduleManager.isCoreEngine(module);
        isSystemManageEnable = gmmsUtility.isSystemManageEnable();
    }

    protected void putSession2URIMap(Session session){
        if(transactionURI2Connections != null){
            transactionURI2Connections.put(session.getTransactionURI(),session);
        }
    }

    public Session getSession(TransactionURI uri) {
        if(transactionURI2Connections != null){
            return transactionURI2Connections.get(uri);
        }
        return null;
    }

    protected void removeSessionFURIMap(TransactionURI uri){
        if(transactionURI2Connections != null){
            transactionURI2Connections.remove(uri);
        }
    }

    public ConnectionStatus getStatus() {
        return status;
    }

    public void setStatus(ConnectionStatus status) {
        synchronized (mutex) {
            this.status = status;
            setChanged();
            notifyObservers(status);
        }
    }

    public String getConnectioName() {
        if (info != null) {
            return info.getConnectionName();
        }
        return null;
    }

    public ConnectionStatus getConnectionStatus() {
        return status;
    }

    public Session getSession() {
        if (connectedSessions.size() == 0) {
            return null;
        }
        Session session = null;
        Object obj = null;
        synchronized (mutex) {
            int index = strategy.getNextIndex(connectedSessions.size());
            Iterator iterator = connectedSessions.values().iterator();
            if (connectedSessions.size() > index) {
                int i = 0;
                while (iterator.hasNext()) {
                	obj = iterator.next();
                    if (i++ >= index) {
                        if (obj != null) {
                            session = (Session) obj;
                            if(isCoreEngine || !isSystemManageEnable){
                            	return session;
                            }
                            else{
 	                            if(!session.isFakeSession()){
 	                            	return session;
 	                            }else{
 	                            	session = null;
 	                            	continue;
 	                           }
                            }
                        }
                    }
                }
            	iterator = connectedSessions.values().iterator();
            	 while ((index-- >= 0) && iterator.hasNext()) {
            		 obj = iterator.next();
                     if (obj != null) {
                         session = (Session) obj;;
                         if(isCoreEngine || !isSystemManageEnable){
                        	 return session;
                         }
                         else{
                            if(!session.isFakeSession()){
                            	return session;
                            }else{
                            	session = null;
                            	continue;
                           }
                         }
                     }
            	  }
            }
            else {
                while (iterator.hasNext()) {
                    obj = iterator.next();
                    if (obj != null) {
                    	session = (Session) obj;
                        if(isCoreEngine || !isSystemManageEnable){
                        	return session;
                        }
                        else{
                            if(!session.isFakeSession()){
                            	return session;
                            }else{
                            	session = null;
                            	continue;
                           }
                        }
                    }
                }
            }
        }

        return session;
    }
    
    

    public int getSessionNum() {
        return sessionNum;
    }

    public BindMode getBindMode() {
        return bindMode;
    }

    public boolean initialize(ConnectionInfo info) {
        if (info == null) {
            return false;
        }
        this.info = info;
        this.bindMode = info.getBindMode();
        connectedSessions = new HashMap();
        return true;
    }

    public abstract boolean addSession(Session session);

    public abstract boolean deleteSession(Session session);

	public Map<TransactionURI, Session> getTransactionURI2Connections() {
		return transactionURI2Connections;
	}
}
