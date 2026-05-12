package com.king.gmms.connectionpool;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;



import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
/**
 * NIO TCPIP connection
 * @author jianmingyang
 *
 */
public class NIOTCPIPConnection {
	private static SystemLogger log = SystemLogger.getSystemLogger(NIOTCPIPConnection.class);
    private Socket socket;
    private TCPReceiver tcpReceiver;
    private Receiver receiver = null;
    private int soTimeout;
    private int readBufferSize;
    private long eventInterval;
    private long sendingInterval;
    private long maxSilentTime;
    private volatile boolean closed;
    private volatile boolean async;
    private final Object mutex;//Used for lock during sendAndReceive duration
    private String connectionname;
    private SocketChannel sc = null;
    private Selector se = null;
    
    public NIOTCPIPConnection(Socket socket) {
        if(socket == null) {
            throw new NullPointerException("Socket can't be null!");
        }
        closed = true;
        async = true;
        mutex = new Object();
        this.socket = socket;
        sc = socket.getChannel();
        setSoTimeout(1, SECONDS);
        setEventInterval(20, MILLISECONDS);
        setReadBufferSize(10240);
        setSendingInterval(1, MILLISECONDS);
        setMaxSilentTime(2 * 60, SECONDS);
    }
    /**
     * open socket channel
     * @throws IOException
     */
    public void open() throws IOException {
    	sc.configureBlocking(false);
        se = Selector.open();
        sc.register(se, SelectionKey.OP_CONNECT | SelectionKey.OP_READ | SelectionKey.OP_WRITE |SelectionKey.OP_ACCEPT);
    	if(!sc.isConnected()) {
            throw new IOException("Socket is not connected.");
        }
    	tcpReceiver = new TCPReceiver("TCP Receiver "+connectionname);
        tcpReceiver.start();
        log.info("A receiver thread is started to receive data from socket.");
    }
    /**
     * send data
     * @param data
     * @throws IOException
     */
    public void send(byte [] data) throws IOException {
    	if(closed) {
            throw new IOException("Connection is not open!!");
        }
        if(data == null) {
            return;
        }
        while(!sc.finishConnect());
        ByteBuffer buffer = ByteBuffer.wrap(data);
        synchronized(mutex){
	        sc.write(buffer);
        }
    }

    /**
     * send and receive
     * @param data
     * @return
     * @throws IOException
     */
    public ByteBuffer sendAndReceive(byte [] data) throws IOException {
    	 if(closed) {
             throw new IOException("Connection is not open!!");
         }
         if(data == null) {
             return null;
         }
         ByteBuffer result = null;
         ByteBuffer buffer = ByteBuffer.wrap(data);
         synchronized(mutex) {
 	        async = false;
 	        sc.write(buffer);
            try {
                mutex.wait(soTimeout);
            }
            catch(InterruptedException e) {
                Thread.interrupted();
                log.warn("TcpIpConnection is interrupted!", e);
            }
 	        result = tcpReceiver.getSyncResponse();
 	        tcpReceiver.clearSyncResponse();
 	        async = true;
         }
         return result;
    }
    public void setReceiver(Receiver receiver){
        if(receiver != null){
           this.receiver = receiver;
        }
    }
    public void setConnectionName(String connectionName){
        this.connectionname = connectionName;
    }
    /**
     * 
     */
    private void fireConnectionUnavailable() {
        close();
        if (receiver != null) {
            receiver.connectionUnavailable();
        }
    }
    
    /**
     * close
     */
    public void close() {
        if(!closed) {
        	synchronized(mutex){
        		if(closed)
        			return;
	            closed = true;
	            log.trace("TcpIpConnection closed.");
        	}
        }
    }
    
    public int getSoTimeout(TimeUnit timeUnit) {
        return (int) timeUnit.convert(soTimeout, MILLISECONDS);
    }

    public void setSoTimeout(int soTimeout, TimeUnit timeUnit) {
        if(!closed) {
            throw new IllegalStateException("The connection is already open!");
        }
        if(soTimeout < 0) {
            throw new IllegalArgumentException("SoTimeOut >= 0, but now it's:" + soTimeout);
        }
        this.soTimeout = (int) MILLISECONDS.convert(soTimeout, timeUnit);
    }

    public boolean isClosed() {
        return closed;
    }

    public int getReadBufferSize() {
        return readBufferSize;
    }

    public void setReadBufferSize(int readBufferSize) {
        if(!closed) {
            throw new IllegalStateException("The connection is already open!");
        }
        if(readBufferSize <= 0) {
            throw new IllegalArgumentException("ReadBufferSize > 0, but now it's:" + readBufferSize);
        }
        try {
            this.readBufferSize = readBufferSize;
            //socket.setReceiveBufferSize(readBufferSize / 2);
        }
        catch(Exception e) {
        }
    }

    public void setEventInterval(long eventInterval, TimeUnit timeUnit) {
        if(eventInterval < 0) {
            throw new IllegalArgumentException("EventInterval >=0, but now it's:" + eventInterval);
        }
        this.eventInterval = MILLISECONDS.convert(eventInterval, timeUnit);
    }

    public long getEventInterval(TimeUnit timeUnit) {
        return timeUnit.convert(eventInterval, MILLISECONDS);
    }

    public long getSendingInterval(TimeUnit timeUnit) {
        return timeUnit.convert(sendingInterval, MILLISECONDS);
    }

    public void setSendingInterval(long sendingInterval, TimeUnit timeUnit) {
        if(sendingInterval < 0) {
            throw new IllegalArgumentException("SendingInterval >=0, but now it's:" + sendingInterval);
        }
        this.sendingInterval = MILLISECONDS.convert(sendingInterval, timeUnit);
    }

    public long getMaxSilentTime(TimeUnit timeUnit) {
        return timeUnit.convert(maxSilentTime, MILLISECONDS);
    }

    /**
     * Set to 0 means allowing unlimited.
     *
     * @param maxSilentTime long
     * @param timeUnit TimeUnit
     */
    public void setMaxSilentTime(long maxSilentTime, TimeUnit timeUnit) {
        if(maxSilentTime < 0) {
            throw new IllegalArgumentException("MaxSilentTime >=0, but now it's:" + maxSilentTime);
        }
        this.maxSilentTime = MILLISECONDS.convert(maxSilentTime, timeUnit);
    }
    /**
     * TCPReceiver
     * @author jianmingyang
     *
     */
    private class TCPReceiver extends Thread {
        private ByteBuffer syncResponse;
        
        public TCPReceiver(String receiverName) throws IOException {
            super(A2PThreadGroup.getInstance(),receiverName);
        }

        public void run() {
            try {
                ByteBuffer received = ByteBuffer.allocateDirect(1024);
                int len = -1;
                while(!closed) {
                    if(closed) {
                        break;
                    }
                    received.clear();
                    len = sc.read(received);
                    received.flip();
                    if(len > 0) {
                        received.rewind();
                        fireReceived(received);
                    }
                    else if(len < 0) {
                        log.debug("Connection unavailable due to closed by remote.");
                        fireConnectionUnavailable();
                        break;
                    }
                }
            }
            catch(InterruptedIOException e) {
                fireConnectionUnavailable();
                log.warn(e,e);
            }
            catch(IOException e) {
                log.warn("Connection unavailable due to exception.", e);
                fireConnectionUnavailable();
            }
            finally {
                try {
                	if(sc!=null && sc.isConnected()){
                		sc.close();
                	}	
                	if(se!=null && se.isOpen()){
                		se.close();
                	}	
                	if(socket != null && !socket.isClosed()) {
                        socket.close();
                        socket = null;
                    }
                }
                catch(IOException e) {
                    log.warn(e, e);
                }
                log.info("The receiver is stoped");
            }
        }

        private void fireReceived(ByteBuffer received) {
            if(async) {
                if(receiver != null){
                    receiver.parse(received);
                }
            }
            else {
                this.syncResponse = received;
                synchronized(mutex) {
                    mutex.notifyAll();
                }
            }
        }

        ByteBuffer getSyncResponse() {
            return syncResponse;
        }

        void clearSyncResponse() {
            syncResponse = null;
        }
    }

    }
