package com.king.gmms.connectionpool.session;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLSocket;

import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.ConnectionStatus;
import com.king.gmms.connectionpool.TCPIPConnection;
import com.king.gmms.connectionpool.ssl.SslContextFactory;
import com.king.gmms.domain.ConnectionInfo;

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
public class SslSmppSession extends MultiSmppSession {
    private static SystemLogger log = SystemLogger.getSystemLogger(SslSmppSession.class);

    private SslContextFactory contextFactory = null;
    
    //construct for server
    public SslSmppSession(Socket socket, SslContextFactory contextFactory) {
    	super(socket);
    	this.contextFactory = contextFactory;
    }
    
    //construct for client
    public SslSmppSession(ConnectionInfo info) {
        super(info);
    }
    
    @Override
	public boolean createConnection() throws IOException {

        if (ConnectionStatus.CONNECT.equals(status) ||
            ConnectionStatus.RECOVER.equals(status)) {
        	log.warn("MultiSmppSession is already binded. And the current session number is {}",getSessionNum());
            return true;
        }
        int ssid = customerInfo.getSSID();
        int applySuccess = 0;// applyNewSession();
        if(applySuccess!=0){
        	if(applySuccess==3){
        		log.warn("applyNewSession failed!");
        	}
        	else if(applySuccess == 2){
        		if(log.isDebugEnabled()){
        			log.debug("The session number reach to max limitation, no need to setup the new session");
        		}
        	}else{
        		log.warn("System management refuses the connection and now there are {} sessions in connection pool!",
                        connectionManager.getSessionNum());
        	}
            if(isEnableSysMgt && applySuccess==1){
            	this.sysSession.outBindConfirm(false, msgResponse, ssid);
            }
            return false;
        }
        
		SSLSocket socket = null;
		try {
			contextFactory = new SslContextFactory(customerInfo.getSslConfiguration()== null? gmmsUtility.getSslConfiguration():customerInfo.getSslConfiguration());
			socket = contextFactory.newSslSocket();
			socket.connect(new InetSocketAddress(connectionInfo.getURL(),
					connectionInfo.getPort()), 500);
			socket.startHandshake();
		} catch (Exception e) {
			if(log.isInfoEnabled()){
				log.info("Connect unsuccessfully with {}:{}, {}", connectionInfo
					.getURL(), connectionInfo.getPort(), e);
			}
			if (socket != null) {
				socket.close();
			}
            if(isEnableSysMgt){
            	this.sysSession.outBindConfirm(false, msgResponse, ssid);
            }
			throw new IOException(e.getMessage());
		}

		synchronized (mutex) {
			connection = new TCPIPConnection(socket);
		}
		try{
			connection.setReceiver(this);
			connection.setSendingInterval(10, TimeUnit.MILLISECONDS);
			connection.setMaxSilentTime(gmmsUtility.getMaxSilentTime(),
					TimeUnit.MILLISECONDS);
			connection.setSoTimeout(5, TimeUnit.SECONDS);
			connection.setConnectionName("" + sessionNum);
			if (this.buffersize > 0) {
				connection.setReadBufferSize(buffersize);
			}
			connection.open();
			
		}catch(IOException ex){
            if(isEnableSysMgt){
            	this.sysSession.outBindConfirm(false, msgResponse, ssid);
            }
            log.info("Open connection {}:{} failed: {}", connectionInfo
					.getURL(), connectionInfo.getPort(), ex);
			throw ex;
		}catch(Exception e){
            if(isEnableSysMgt){
            	this.sysSession.outBindConfirm(false, msgResponse, ssid);
            }
            log.info("Open connection {}:{} failed: {}", connectionInfo
					.getURL(), connectionInfo.getPort(), e);
            return false;
		}
		return true;
	}

}
