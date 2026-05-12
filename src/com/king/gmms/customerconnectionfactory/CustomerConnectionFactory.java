package com.king.gmms.customerconnectionfactory;

import com.king.gmms.connectionpool.connection.ConnectionManager;
import com.king.gmms.messagequeue.OperatorMessageQueue;

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
public interface CustomerConnectionFactory {
    
	public OperatorMessageQueue getOperatorMessageQueue(int ssid);
	
    public ConnectionManager getConnectionManagerBySSID(int ssid);
    
    public ConnectionManager getConnectionManager(int ssid, String name);
    
    public void putConnectionManager(int ssid, String name, ConnectionManager ncm); 
}
