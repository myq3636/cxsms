package com.king.gmms.connectionpool.connection;

import java.util.*;

import com.king.gmms.connectionpool.*;
import com.king.gmms.connectionpool.node.Node;
import com.king.gmms.connectionpool.node.NodeStatus;
import com.king.gmms.connectionpool.session.*;
import com.king.gmms.ha.*;
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
public class NodeConnectionManager
    extends Observable implements NodeConnectionManagerInterface {

    private ConnectionPool connectionPool = null;
    private IndexStrategy strategy = null;
    private Object mutex = new Object();
    private Node node = null;

    public NodeConnectionManager() {
        this(new IndexRandomStrategy());
    }

    public NodeConnectionManager(IndexStrategy strategy) {
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
        ConnectionStatus status = null;
        if (arg instanceof ConnectionStatus) {
            status = (ConnectionStatus) arg;
        }
        else {
            return;
        }
        if(connectionPool.updateConnectionStatus(connection)){
            ConnectionStatusInfo statusInfo = new ConnectionStatusInfo(
                connection.getConnectioName(), status);
            synchronized (mutex) {
                setChanged();
                notifyObservers(statusInfo);
            }
        }
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
        return connectionPool.addSession(connectionName, session);
    }

    public boolean deleteSession(String connectionName, Session session) {
        return connectionPool.removeSession(connectionName, session);
    }

    public int getConnectionNum() {
        return connectionPool.getConnectionNum();
    }

    public int getSessionNum() {
        return connectionPool.getSessionNum();
    }

    public void setNode(Node node){
        this.node = node;
    }

    public void setStrategy(Strategy strategy) {
        this.strategy = (IndexStrategy)strategy;
    }

    public boolean getNodeStatus(String connectionName){
        boolean result = true;
        if(node != null){
            NodeStatus st = node.getStatus();
            switch(st){
                case up:
                    result = true;
                    break;
                case down:
                    result = false;
                    break;
                default:
            }
        }
        return result;
    }
}
