package com.king.gmms.connectionpool.connection;

import java.util.*;

import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.session.*;

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
public class ConnectionPool {
	
	private static SystemLogger log = SystemLogger.getSystemLogger(ConnectionPool.class);

    private Map connectedPool = null;
    private Map disconnectedPool = null;
    private Object mutex = new Object();
    private int maxConnectionSize = -1;
    private int maxSessionSize = -1;
    private int connectionNum = 0;
    private int sessionNum = 0;

    public ConnectionPool(){
        connectedPool = new HashMap();
        disconnectedPool = new HashMap();
    }

    public boolean addConnection(Connection connection) {
        if (connection == null) {
            return false;
        }

        String connectionName = connection.getConnectioName();
        synchronized (mutex) {
            if(maxConnectionSize > 0 && connectionNum +1 > maxConnectionSize){
                return false;
            }
            else if(maxSessionSize > 0 && sessionNum + connection.getSessionNum() > maxSessionSize){
                return false;
            }
            switch (connection.getConnectionStatus()) {
                case CONNECT:
                    if (!connectedPool.containsKey(connectionName)) {
                        connectedPool.put(connectionName, connection);
                        connectionNum += 1;
                        sessionNum += connection.getSessionNum();
                    }
                    break;
                case INITIAL:
                case DISCONNECT:
                    if (!disconnectedPool.containsKey(connectionName)) {
                        disconnectedPool.put(connectionName, connection);
                        connectionNum += 1;
                        sessionNum += connection.getSessionNum();
                    }
                    break;
            }
        }
        return true;
    }

    public boolean contain(String connectionName) {
        if (connectionName == null) {
            return false;
        }
        synchronized (mutex) {
            if (connectedPool.containsKey(connectionName)) {
                return true;
            }
            else if (disconnectedPool.containsKey(connectionName)) {
                return true;
            }
        }
        return false;
    }

    public Connection remove(String connectionName) {
        if (connectionName == null) {
            return null;
        }
        Connection connection = null;
        synchronized (mutex) {
            if (connectedPool.containsKey(connectionName)) {
                connection = (Connection)connectedPool.get(connectionName);
                if(connection != null){
                    connectionNum -= 1;
                    if(sessionNum >= connection.getSessionNum()){
                        sessionNum -= connection.getSessionNum();
                    }
                }
                return (Connection) connectedPool.remove(connectionName);
            }
            else if (disconnectedPool.containsKey(connectionName)) {
                connection = (Connection) connectedPool.get(connectionName);
                 if(connection != null){
                     connectionNum -= 1;
                     if(sessionNum >= connection.getSessionNum()){
                         sessionNum -= connection.getSessionNum();
                    }
                 }
                return (Connection) disconnectedPool.remove(connectionName);
            }
        }
        return null;
    }

    public boolean updateConnectionStatus(Connection connection) {
        if(connection == null){
            return false;
        }
        String connectionName = connection.getConnectioName();
        if (connectionName == null || connectionName.equals("")) {
            return false;
        }
        synchronized (mutex) {
            Object obj =  connectedPool.remove(connectionName);
            if(obj == null){
                disconnectedPool.remove(connectionName);
            }
            switch (connection.getConnectionStatus()) {
                case INITIAL:
                    break;
                case CONNECT:
                    connectedPool.put(connectionName, connection);
                    break;
                case DISCONNECT:
                    disconnectedPool.put(connectionName, connection);
                    break;
                default:
                    return false;
            }

        }
        return true;
    }

    public int getSessionNum() {
        return sessionNum;
    }

    public int getConnectionNum() {
        return connectionNum;
    }

    public int getMaxSessionSize() {
        return maxSessionSize;
    }

    public int getMaxConnectionSize() {
        return maxConnectionSize;
    }

    public Connection getAvailableConnection(int index){
        Connection connection = null;
        Object obj = null;
        synchronized (mutex) {
            Iterator iterator = connectedPool.entrySet().iterator();
            if (connectedPool.size() > index) {
                for (int i = 0; i <= index && iterator.hasNext(); i++) {
                    obj = iterator.next();
                }
            }
            else {
                if(iterator.hasNext()){
                    obj = iterator.next();
                }
            }
        }
        if(obj != null){
            connection = (Connection)( (Map.Entry) obj).getValue();
        }
        return connection;
    }

    public Connection getAvailableConnection(String connectionName){
        Object obj = null;
        synchronized(mutex){
            obj = connectedPool.get(connectionName);
        }
        if(obj != null){
            Connection connection = (Connection) obj;
            return connection;
        }else{
            return null;
        }
    }

    public int getAvailableSize(){
        return connectedPool.size();
    }

    public Connection getConnection(String connectionName){
        if (connectionName == null) {
            return null;
        }
        Object obj = null;
        synchronized (mutex) {
            obj = connectedPool.get(connectionName);
            if(obj == null){
                obj = disconnectedPool.get(connectionName);
            }
        }
        if(obj != null){
            return (Connection)obj;
        }else{
            return null;
        }
    }

    public boolean removeSession(String connectionName, Session session) {
        boolean result = false;
        if (connectionName != null && session != null) {
            Connection connection = getConnection(connectionName);
            if (connection != null) {
                result = connection.deleteSession(session);
                synchronized(mutex){
                    if(result && sessionNum > 0){
                        sessionNum -= 1;
                    }
                }
            }
        }
        return result;
    }

    public boolean addSession(String connectionName,Session session){
        boolean result = false;
        if (connectionName != null && session != null) {
            Connection connection = getConnection(connectionName);
            if (connection != null) {
                if(maxSessionSize > 0 && sessionNum + 1 > maxSessionSize){
                    return result;
                }
                result = connection.addSession(session);
                synchronized(mutex){
                    if(result){
                        sessionNum += 1;
                    }
                }
            } else {
            	log.info("ConnectionPool.addSession failed, connectionName={}, connection={}", connectionName, connection);
            }
        }
        return result;
    }

    public void setMaxConnectionSize(int maxConnectionSize) {
        this.maxConnectionSize = maxConnectionSize;
    }

    public void setMaxSessionSize(int maxSessionSize) {
        this.maxSessionSize = maxSessionSize;
    }
}
