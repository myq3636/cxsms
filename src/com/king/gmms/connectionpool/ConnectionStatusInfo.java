package com.king.gmms.connectionpool;

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
public class ConnectionStatusInfo {


    public String connectionName;

    public ConnectionStatus status;
    
    public ConnectionStatusInfo(String connectionName, ConnectionStatus status) {
        this.connectionName = connectionName;
        this.status = status;
    }
}