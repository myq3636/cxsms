package com.king.gmms.connectionpool.connection;

import java.util.Observable;
import java.util.ArrayList;
import java.util.Iterator;

import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.session.*;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.strategy.IndexRandomStrategy;
import com.king.gmms.strategy.IndexStrategy;
import com.king.gmms.strategy.Strategy;


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
public class ConnectionManagerImpl implements ConnectionManager {
	private static SystemLogger log = SystemLogger.getSystemLogger(ConnectionManagerImpl.class);
    private ConnectionPool connectionPool = null;
    private IndexStrategy strategy = null;

    public ConnectionManagerImpl() {
        this(new IndexRandomStrategy());
    }

    public ConnectionManagerImpl(IndexStrategy strategy) {
        this.strategy = strategy;
        connectionPool = new ConnectionPool();

    }

    public void update(Observable o, Object arg) {
        Connection connection = null;
        if (o instanceof Connection) {
            connection = (Connection) o;
        }
        else {
            return;
        }
        connectionPool.updateConnectionStatus(connection);
    }

    public Session getSession() {
        int index = strategy.getNextIndex(connectionPool.getAvailableSize());
        Connection connection = connectionPool.getAvailableConnection(index);
        if(connection != null){
            return connection.getSession();
        }
        else{
            return null;
        }
    }

    public Session getSession(TransactionURI transaction){
        String name = transaction.getConnectionName();
        Connection connection = connectionPool.getAvailableConnection(name);
        if (connection != null) {
            return connection.getSession(transaction);
        }
        return null;
    }

    public Session getSession(ArrayList connectionIDList,boolean isSplit4DR) {
        if (connectionIDList == null || connectionIDList.size() <= 0) {
            return null;
        }
        Object obj = null;
        Connection connection = null;
        Iterator iterator = connectionIDList.iterator();
        while(iterator.hasNext()){
            obj = iterator.next();
            if (obj != null) {
                connection = connectionPool.getAvailableConnection( (String)
                    obj);
                if (connection != null) {
                    return connection.getSession();
                }
            }
        }
        return null;
    }

    public boolean contain(String connectionName) {
        if (connectionName != null && !connectionName.equals(""))
            return connectionPool.contain(connectionName);
        else
            return false;
    }
    /**
     * get available connection
     */
    public Connection getConnection(String connectionName) {
        if (connectionName != null && !connectionName.equals("")){
        	return connectionPool.getConnection(connectionName);
        }else{
        	return null;
        }
    }
    
    
    public boolean insertConnection(Connection connection) {
        boolean result = false;
        if (connection != null) {
            result = connectionPool.addConnection(connection);
            if (result) {
                ( (AbstractMultiConnection) connection).addObserver(this);
            }
        }
        return result;
    }
    public boolean deleteConnection(Connection connection) {
        boolean result = false;
        if (connection != null) {
            if (connectionPool.remove(connection.getConnectioName()) != null) {
                ( (AbstractMultiConnection) connection).deleteObserver(this);
                result = true;
            }
            else {
                result = false;
            }
        }
        return result;
    }

    public boolean closePool() {
        return false;
    }

    public boolean insertSession(String connectionName, Session session) {
        boolean result = false;
        result = connectionPool.addSession(connectionName, session);
        return result;
    }

    public boolean deleteSession(String connectionName, Session session) {
        boolean result = false;
        result = connectionPool.removeSession(connectionName, session);
        return result;
    }

    public int getConnectionNum() {
        return connectionPool.getConnectionNum();
    }

    public int getSessionNum() {
        return connectionPool.getSessionNum();
    }

    public void setStrategy(Strategy strategy) {
        this.strategy = (IndexStrategy) strategy;
    }

}
