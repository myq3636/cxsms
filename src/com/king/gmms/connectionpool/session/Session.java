package com.king.gmms.connectionpool.session;

import com.king.gmms.connectionpool.ConnectionStatus;
import com.king.gmms.connectionpool.Receiver;
import com.king.gmms.connectionpool.connection.ConnectionManager;
import com.king.gmms.domain.ConnectionInfo;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.messagequeue.OperatorMessageQueue;
import com.king.message.gmms.GmmsMessage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Observable;

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
public abstract class Session extends Observable implements Receiver{


    public abstract void setConnectionManager(ConnectionManager connManager);

    public abstract void stop();
    
    public abstract void initSenders();
    public abstract void initReceivers();
    public abstract void updateReceivers();
    
    public abstract int enquireLink() throws Exception;
    public abstract boolean connect();
    public abstract boolean createConnection() throws IOException;
    public abstract boolean submit(GmmsMessage msg) throws IOException;
    public abstract ByteBuffer submitAndRec(GmmsMessage msg) throws IOException;
    
    public abstract ConnectionInfo getConnectionInfo();
    public abstract long getLastActivity();
    
    public abstract void setKeepRunning(boolean keepRunning);
    public abstract boolean isKeepRunning();
    
    public abstract TransactionURI getTransactionURI();
    public abstract String getSessionName();
    public abstract void setSessionName(String connectionName);
    public abstract int getSessionNum();
    public abstract ConnectionStatus getStatus();
    public abstract void setStatus(ConnectionStatus status);
    public abstract boolean isServer();
    
    public abstract OperatorMessageQueue getOperatorMessageQueue();
    
    public abstract boolean isFakeSession();
    
    /**
     * need ovverrid to
     * destroy sessionThread
     */
    public void destroy() {
    	stop();
//    	keepRunning = false;
    }

	@Override
	public boolean equals(Object obj) {
		Session session = (Session)obj;
		return this.getTransactionURI().equals(session.getTransactionURI());
	}

	@Override
	public int hashCode() {
		return this.getTransactionURI().hashCode();
	}
    
}
