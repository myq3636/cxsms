/**
 * Author: frank.xue@King.com
 * Date: 2006-6-1
 * Time: 11:33:07
 * Document Version: 0.1
 */

package com.king.gmms.transport.tcp;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * All time variable in this class is milliseconds
 */
@Deprecated
public class TcpIpConnection {
    private static SystemLogger log = SystemLogger.getSystemLogger(TcpIpConnection.class);
    private Socket socket;
    private Receiver receiver;
    private EventFirer firer;
    private OutputStream os;
    private HashSet<TcpIpListener> listeners;
    private int soTimeout;
    private int readBufferSize;
    private long eventInterval;
    private long sendingInterval;
    private long maxSilentTime;
    private volatile boolean closed;
    private volatile boolean async;
    private final Object mutex;//Used for lock during sendAndReceive duration
    private String connectionname;

    public TcpIpConnection(Socket socket) {
        if(socket == null) {
            throw new NullPointerException("Socket can't be null!");
        }
        closed = true;
        async = true;
        mutex = new Object();
        this.socket = socket;
        listeners = new HashSet<TcpIpListener>();
        setSoTimeout(1, SECONDS);
        setEventInterval(20, MILLISECONDS);
        setReadBufferSize(10240);
        setSendingInterval(5, MILLISECONDS);
        setMaxSilentTime(2 * 60, SECONDS);
    }

    public void open() throws IOException {
        if(listeners == null || listeners.size() == 0) {
            throw new NullPointerException("At least One TcpIpListener must be add to this connection before opening!");
        }
        if(socket.isClosed() || !socket.isConnected()) {
            throw new IOException("Socket is not connected.");
        }
        closed = false;
        socket.setTcpNoDelay(true);
        os = new BufferedOutputStream(socket.getOutputStream());
        firer = new EventFirer("EventFirer"+connectionname);
        firer.start();
        log.info("A firer thread is started to process PDU.");
        receiver = new Receiver("Receiver"+connectionname);
        receiver.start();
        log.info("A receiver thread is started to receive data from socket.");
    }

    public synchronized void close() {
        if(!closed) {
            closed = true;
            try {
                if (receiver!=null&&receiver.is!=null){
                    receiver.is.close();
                }
            }
            catch(IOException e) {
                log.warn(e, e);
            }
            log.trace("TcpIpConnection closed.");
        }
    }

    /**
     * Wait until sending buffer is not full to send the data.
     *
     * @param data byte[]
     * @throws IOException
     */
    public synchronized void send(byte [] data) throws IOException {
        if(closed) {
            throw new IOException("Connection is not open!!");
        }
        if(data == null) {
            return;
        }
        os.write(data);
        os.flush();
        if(sendingInterval > 0) {
            try {
                Thread.sleep(sendingInterval);
            }
            catch(InterruptedException e) {
                log.warn("TcpIpConnection is interrupted!", e);
                Thread.interrupted();
            }
        }
    }

    /**
     * null is returned if socket timeout
     *
     * @param data byte[]
     * @return java.nio.ByteBuffer
     * @throws IOException
     */
    public synchronized ByteBuffer sendAndReceive(byte [] data) throws IOException {
        if(closed) {
            throw new IOException("Connection is not open!!");
        }
        if(data == null) {
            return null;
        }
        async = false;
        os.write(data);
        os.flush();
        synchronized(mutex) {
            try {
                mutex.wait(soTimeout);
            }
            catch(InterruptedException e) {
                Thread.interrupted();
                log.warn("TcpIpConnection is interrupted!", e);
            }
        }
        ByteBuffer result = receiver.getSyncResponse();
        receiver.clearSyncResponse();
        async = true;
        return result;
    }

    public void addTcpIpListener(TcpIpListener listener) {
        if(!closed) {
            throw new IllegalStateException("The connection is already open!");
        }
        listeners.add(listener);
    }

    public void removeTcpIpListener(TcpIpListener listener) {
        if(!closed) {
            throw new IllegalStateException("The connection is already open!");
        }
        if(listeners.contains(listener)) {
            listeners.remove(listener);
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
            socket.setReceiveBufferSize(readBufferSize / 2);
        }
        catch(SocketException e) {
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
     * @param maxSilentTime
     * @param timeUnit
     */
    public void setMaxSilentTime(long maxSilentTime, TimeUnit timeUnit) {
        if(maxSilentTime < 0) {
            throw new IllegalArgumentException("MaxSilentTime >=0, but now it's:" + maxSilentTime);
        }
        this.maxSilentTime = MILLISECONDS.convert(maxSilentTime, timeUnit);
    }

    private void fireConnectionUnavailable() {
        close();
        for(TcpIpListener listener : listeners) {
            listener.connectionUnavailable();
        }
    }
    public void setConnectionName(String connectionName){
        this.connectionname = connectionName;
    }
    public String getConnectionName(){
        return this.connectionname;
    }

    private class Receiver extends Thread {
        private InputStream is;
        private ByteBuffer syncResponse;

        public Receiver(String receiverName) throws IOException {
            super(A2PThreadGroup.getInstance(),receiverName);
            this.is = new BufferedInputStream(socket.getInputStream(), readBufferSize);
            this.setPriority(Thread.MAX_PRIORITY);
        }

        public void run() {
            try {
                ByteBuffer received = null;
                byte [] input = null;
                int len = -1;
                while(!closed) {
                    len = -1;
                    input = new byte[readBufferSize];
                    len = is.read(input);

                    if(closed) {
                        break;
                    }
                    if(len > 0) {
                        received = ByteBuffer.allocate(len).put(input, 0, len);
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
                closed = true;
                log.warn(e.getMessage());
            }
            catch(IOException e) {
                log.warn("Connection unavailable due to exception." , e);
                fireConnectionUnavailable();
            }
            finally {
                try {
                    if (os!=null){
                        os.close();
                        os =null;
                    }
                    if(socket != null && !socket.isClosed()) {
                        socket.close();
                        socket = null;
                    }
                }
                catch(IOException e) {
                    log.warn(e, e);
                }
            }
        }

        private void fireReceived(ByteBuffer received) {
            if(async) {
                firer.offer(received);
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

    private class EventFirer extends Thread {
        private Queue<ByteBuffer> queue;

        public EventFirer(String EventFirerName) {
            super(A2PThreadGroup.getInstance(),EventFirerName);
            queue = new ConcurrentLinkedQueue<ByteBuffer>();
        }

        public void offer(ByteBuffer received) {
            queue.offer(received);
        }

        public void run() {
            long lastReceivingTime = System.currentTimeMillis();
            while(!closed) {
                ByteBuffer data = queue.poll();
                //needMathing?
                if(data != null) {
                    //fire received
                    lastReceivingTime = System.currentTimeMillis();
                    for(TcpIpListener listener : listeners) {
                        listener.received(data);
                    }
                }
                else {
                    if(System.currentTimeMillis() - lastReceivingTime < maxSilentTime || maxSilentTime <= 0) {
                        try {
                            Thread.sleep(eventInterval);
                        }
                        catch(InterruptedException e) {
                            log.warn("TcpIpConnection.EventFirer is interrupted!", e);
                            Thread.interrupted();
                        }
                    }
                    else {
                        log.debug("Connection unavailable due to max silent time is reached.");

                        SimpleDateFormat formater=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                        log.debug("LastReceivingTime: {}",formater.format(new Date(lastReceivingTime)));
                        log.debug("CurrentTime: {}",formater.format(new Date(System.currentTimeMillis())));
                        log.debug("MaxSilentTime: {}",maxSilentTime);

                        fireConnectionUnavailable();
                    }
                }
            }
        }
    }
}
