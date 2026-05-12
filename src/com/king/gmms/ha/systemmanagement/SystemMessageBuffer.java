package com.king.gmms.ha.systemmanagement;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.ha.systemmanagement.pdu.SystemPdu;

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
public class SystemMessageBuffer
    extends Thread {
    private static SystemLogger log = SystemLogger.getSystemLogger(SystemMessageBuffer.class);
    private Map<Integer, BufferEntry> buffer;
    private HashSet<SystemMessageBufferListener> listeners;
    private long timeout;
    private long waitTime;
    private int bufferCapacity;
    private boolean running;
    private String name;
    private int maxRetryTimes = 3;

    public SystemMessageBuffer(String name) {
        this(name, 0);
    }

    public SystemMessageBuffer(String name, int bufferCapacity) {
        super(A2PThreadGroup.getInstance(), "Buffer Monitor");
        if (name != null && name.length() > 0) {
            this.name = name;
            this.setName(name + "_BufferMon");
        }
        if (bufferCapacity < 0) {
            throw new IllegalArgumentException("Capacity must bigger than 0!");
        }
        this.bufferCapacity = bufferCapacity;
        this.running = false;
        this.buffer = Collections.synchronizedMap(new HashMap<Integer, BufferEntry> ());
        listeners = new HashSet<SystemMessageBufferListener> ();
        maxRetryTimes = Integer.parseInt(ModuleManager.getInstance().getClusterProperty(
                "SystemManager.MessageRetryTimes", "3"));
        setWaitTime(50, MILLISECONDS);
    }

    public void addListener(SystemMessageBufferListener listener) {
        if (running) {
            throw new IllegalStateException(
                "The buffer monitor has already start!");
        }
        listeners.add(listener);
    }

    public void removeListener(SystemMessageBufferListener listener) {
        if (running) {
            throw new IllegalStateException(
                "The buffer monitor has already start!");
        }
        if (listeners.contains(listener)) {
            listeners.remove(listener);
        }
    }

    public boolean put(int key, SystemPdu message) {
        if (bufferCapacity > 0) {
            synchronized (buffer) {
                if (buffer.size() >= bufferCapacity) {
                    try {
                        buffer.wait(waitTime);
                    }
                    catch (InterruptedException e) {
                        interrupted();
                    }
                }
                if (buffer.size() >= bufferCapacity) {
                	if (log.isInfoEnabled()) {
                		log.info("SystemMessageBuffer is full: size={}, bufferCapacity={}", size(), bufferCapacity);
                	}
                    return false;
                }
            }
        }
        buffer.put(key, new BufferEntry(message));
        if(log.isTraceEnabled()){
			 log.trace("Put a message to {}, its size:{}, Message {}", name, size(), message.toString());
		}
        return true;
    }
    
    public SystemPdu remove(int key) {
    	SystemPdu msg = null;
		synchronized (buffer) {
			if (buffer.containsKey(key)) {
				msg = buffer.remove(key).getMessage();
				if (log.isTraceEnabled() && msg != null) {
					log.trace("Remove message from buffer {}, msg {}", name, msg);
				}
			}
		}
        
        return msg;
    }
    
    public synchronized void startMonitor() {
        if (!running) {
            running = true;
            start();
        }
    }

    public synchronized void stopMonitor() {
        if (running) {
            running = false;
            clear();
            listeners.clear();
        }
        log.info("The buffer monitor of {} is stopped",name);
    }

    public boolean isRunning() {
        return running;
    }

    public void run() {
        log.info("BufferMonitor of {} start.",name);
        while (running) {
            Object timeoutKey = null;
            SystemPdu timeoutMessage = null;
            BufferEntry entry = null;
            synchronized (buffer) {
                for (Object key : buffer.keySet()) {
                    if (buffer.get(key).isTimeout()) {
                        timeoutKey = key;
                        entry = buffer.get(timeoutKey);
                        timeoutMessage = entry.getMessage();
                        break;
                    }
                }
                if (timeoutKey != null) {
                	if(!timeoutMessage.isRequest()){//pdu response
                        buffer.remove(timeoutKey);
                	}
                }
            }
            if (timeoutKey != null) {
                if(timeoutMessage.getRetryTimes() >= maxRetryTimes){
                	buffer.remove(timeoutKey);
                	continue;
                }else if(entry!=null){
                	entry.freshStartTime();
                }
                
                for (SystemMessageBufferListener listener : listeners) {
                    listener.timeout(timeoutMessage);
                }
            }
            else {
                try {
                    Thread.sleep(timeout / 3);
                }
                catch (InterruptedException e) {
                    interrupted();
                    log.trace("SystemMessageBuffer is interrupted:{}", e.getMessage());
                }
            }
        }
        log.info("SystemMessageBuffer of {} exit!",name);
    }

    public SystemPdu get(String key) {
        BufferEntry bufferEntry = buffer.get(key);
        if (bufferEntry != null) {
            return bufferEntry.getMessage();
        }
        else {
            return null;
        }
    }

    public Set<SystemPdu> getAll() {
        HashSet<SystemPdu> result = new HashSet<SystemPdu> (size());
        synchronized (buffer) {
            for (BufferEntry entry : buffer.values()) {
                if (entry.getMessage() != null) {
                    result.add(entry.getMessage());
                }
            }
        }
        return result;
    }

    public void clear() {
        synchronized (buffer) {
            for (Map.Entry entry : buffer.entrySet()) {
                if (entry != null) {
                	SystemPdu message = ( (BufferEntry) entry.getValue()).
                        getMessage();
                    log.warn("The message is sent to {} unsuccessful and message is {}",
                             name,message.toString());
                }
            }
            buffer.clear();
        }
    }

    public int size() {
        return buffer.size();
    }

    public long getTimeout(TimeUnit timeUnit) {
        return timeUnit.convert(timeout, MILLISECONDS);
    }

    public void setTimeout(long timeout, TimeUnit timeUnit) {
        if (running) {
            throw new IllegalStateException(
                "The buffer monitor has already start!");
        }
        if (timeout <= 0) {
            throw new IllegalArgumentException("Timeout > 0, but now it's:" +
                                               timeout);
        }
        this.timeout = MILLISECONDS.convert(timeout, timeUnit);
    }

    public long getWaitTime(TimeUnit timeUnit) {
        return timeUnit.convert(waitTime, MILLISECONDS);
    }

    public void setWaitTime(long waitTime, TimeUnit timeUnit) {
        if (running) {
            throw new IllegalStateException(
                "The buffer monitor has already start!");
        }
        if (waitTime < 0) {
            throw new IllegalArgumentException("WaitTime >=0, but now it's:" +
                                               waitTime);
        }
        this.waitTime = MILLISECONDS.convert(waitTime, timeUnit);
    }

    public String toString() {
        return buffer.toString();
    }

    private class BufferEntry {
        private SystemPdu message;
        private long startTime;

        BufferEntry(SystemPdu message) {
            this.message = message;
            this.startTime = System.currentTimeMillis();
        }

        SystemPdu getMessage() {
            return message;
        }
        
        void freshStartTime(){
        	startTime = System.currentTimeMillis();
        }

        boolean isTimeout() {
            return System.currentTimeMillis() - startTime > timeout;
        }
    }
}
