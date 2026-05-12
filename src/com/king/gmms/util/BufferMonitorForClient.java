package com.king.gmms.util;

import com.king.framework.SystemLogger;
import com.king.gmms.threadpool.impl.BufferMonitorManager;
import com.king.message.gmms.GmmsMessage;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BufferMonitorForClient extends BufferMonitor {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(BufferMonitorForClient.class);
	protected Map<Object, GmmsMessage> oneBuffer;
	protected Map<Object, GmmsMessage> twoBuffer;
	protected Map<Object, GmmsMessage> threeBuffer;
	protected Map<Object, GmmsMessage> fourBuffer;
	protected List<Map<Object, GmmsMessage>> list;
	private BufferTimeoutInterface listener;
	private long timeout;
	private long waitTime;
	private int bufferCapacity;
	protected AtomicBoolean isAlow = new AtomicBoolean(true);
	private Object wait = new Object();

	protected ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	protected Lock readLock = lock.readLock();
	protected Lock writeLock = lock.writeLock();
	private volatile boolean firstRotate = false;
	private volatile boolean secondRotate = false;
	protected UUID uuid = UUID.randomUUID();
	protected String bufferName;
	protected BufferMonitorManager bufferMonitorManager;

	public BufferMonitorForClient() {
		this(1000);
	}

	public BufferMonitorForClient(int bufferCapacity) {
		if (bufferCapacity < 0) {
			throw new IllegalArgumentException("Capacity must bigger than 0!");
		}
		this.bufferCapacity = bufferCapacity;

		this.oneBuffer = new ConcurrentHashMap<Object, GmmsMessage>(
				bufferCapacity);
		this.twoBuffer = new ConcurrentHashMap<Object, GmmsMessage>(
				bufferCapacity);
		this.threeBuffer = new ConcurrentHashMap<Object, GmmsMessage>(
				bufferCapacity);
		this.fourBuffer = new ConcurrentHashMap<Object, GmmsMessage>(bufferCapacity);
		list = new ArrayList<>();
		for(int i =0; i<10; i++){
			Map<Object, GmmsMessage> buffer = new ConcurrentHashMap<Object, GmmsMessage>(bufferCapacity);
			list.add(buffer);
		}
		setTimeout(90, SECONDS);
		setWaitTime(50, MILLISECONDS);
		bufferMonitorManager = BufferMonitorManager.getInstance();
	}

	private void tryWait() {
		try {
			synchronized (wait) {
				wait.wait();
			}
		} catch (Exception ex) {
		}
	}

	public void setListener(BufferTimeoutInterface listener) {
		this.listener = listener;
	}

	public boolean put(Object key, GmmsMessage gmmsMessage) {

		if (key == null || gmmsMessage == null)
			return false;
		String lastChar = key.toString().substring(key.toString().length()-1);
		Map<Object, GmmsMessage> buffer = list.get(Integer.parseInt(lastChar)%10);
		
		if (bufferCapacity > 0 && buffer.size() > bufferCapacity) {
			try {
				synchronized (wait) {
					if(log.isDebugEnabled()){
						 log.debug(gmmsMessage, "start Put a message to {}, its size:{}, Message type:{}, Message key:{}, {}", bufferName, buffer.size(), gmmsMessage.getMessageType(), key, bufferCapacity);
					 }
					wait.wait(50);
				}
			} catch (Exception e) {
				log.warn(gmmsMessage, e, e);
			}
			if (bufferCapacity > 0 && buffer.size() > bufferCapacity) {
				return false;
			}
		}

		try {
			//readLock.lock();
			synchronized(buffer){
				buffer.put(key, gmmsMessage);
			}
			
		} catch (Exception e) {
			log.warn(e, e);
		} /*finally {
			readLock.unlock();
		}*/
		/*
		 * if(log.isDebugEnabled()){ log.debug(gmmsMessage,
		 * "Put a message to {}, its size:{}, Message type:{}, Message key:{}",
		 * bufferName, buffer.size(), gmmsMessage.getMessageType(), key); }
		 */
		return true;
	}

	public GmmsMessage remove(Object key) {
		if (!isAlow.get()) {
			tryWait();
			return null;
		}
		GmmsMessage msg = null;
		String lastChar = key.toString().substring(key.toString().length()-1);
		try {
			Map<Object, GmmsMessage> buffer = list.get(Integer.parseInt(lastChar)%10);
			//readLock.lock();
			synchronized(buffer){
				msg = buffer.remove(key);
			}			
			if (msg != null) {
				return msg;
			}

			/*if (secondRotate) {
				msg = threeBuffer.remove(key);
				if (msg != null) {
					if (threeBuffer.size() == 0) {
						secondRotate = false;
					}
					return msg;
				}
			}
			if (firstRotate) {
				msg = twoBuffer.remove(key);
				if (msg != null) {
					if (twoBuffer.size() == 0) {
						firstRotate = false;
					}
					return msg;
				}
			}*/

		} catch (Exception e) {
			log.warn(e, e);
		} finally {
			if(log.isDebugEnabled()){
				log.debug(msg, "Remove message from {}", bufferName);
			}
			//readLock.unlock();
		}
		return msg;
	}

	public synchronized void startMonitor(String bufferName) {
		this.bufferName = bufferName;
		bufferMonitorManager.register(this);
	}

	public synchronized void stopMonitor() {
		bufferMonitorManager.deregister(this);
	}


	public GmmsMessage get(Object key) {

		if (!isAlow.get()) {
			tryWait();
			return null;
		}
		String lastChar = key.toString().substring(key.toString().length()-1);
		GmmsMessage msg = null;
		try {
			Map<Object, GmmsMessage> buffer = list.get(Integer.parseInt(lastChar)%10);
			//readLock.lock();
			synchronized(buffer){
				msg = buffer.get(key);
			}	
			
			/*if (secondRotate) {
				msg = threeBuffer.get(key);
				if (msg != null) {
					return msg;
				}
			}
			if (firstRotate) {
				msg = twoBuffer.get(key);
				if (msg != null) {
					return msg;
				}
			}*/
			return msg;
		} catch (Exception e) {
			log.warn(e, e);
		} finally {
			//readLock.unlock();
		}
		return msg;
	}

	public void sendbackBuffer() {
		try {
			
			readLock.lock();
			for(int i=0; i<10;i++){
				Map<Object, GmmsMessage> buffer = list.get(i);
				for (Object key : buffer.keySet()) {
					GmmsMessage msg = buffer.get(key);
					if(log.isInfoEnabled()){
						log.info(msg, "{} backup message!", bufferName);
					}
					listener.timeout(key, msg);
				}
			}
			
			/*for (Object key : twoBuffer.keySet()) {
				GmmsMessage msg = twoBuffer.get(key);
				if(log.isInfoEnabled()){
					log.info(msg, "{} backup message!", bufferName);
				}
				listener.timeout(key, msg);
			}
			for (Object key : threeBuffer.keySet()) {
				GmmsMessage msg = threeBuffer.get(key);
				if(log.isInfoEnabled()){
					log.info(msg, "{} backup message!", bufferName);
				}
				listener.timeout(key, msg);
			}
			for (Object key : fourBuffer.keySet()) {
				GmmsMessage msg = fourBuffer.get(key);
				if(log.isInfoEnabled()){
					log.info(msg, "{} backup message!", bufferName);
				}
				listener.timeout(key, msg);
			}*/
		} catch (Exception e) {
			log.warn(e, e);
		} finally {
			readLock.unlock();
		}

	}

	public Set<GmmsMessage> getAll() {

		if (!isAlow.get()) {
			tryWait();
			return null;
		}

		HashSet<GmmsMessage> result = new HashSet<GmmsMessage>();
		try {
			readLock.lock();
			/*for (GmmsMessage entry : oneBuffer.values()) {
				if (entry != null) {
					result.add(entry);
				}
			}
			for (GmmsMessage entry : twoBuffer.values()) {
				if (entry != null) {
					result.add(entry);
				}
			}
			for (GmmsMessage entry : threeBuffer.values()) {
				if (entry != null) {
					result.add(entry);
				}
			}
			for (GmmsMessage entry : fourBuffer.values()) {
				if (entry != null) {
					result.add(entry);
				}
			}*/
			
			for(int i=0; i<10;i++){
				Map<Object, GmmsMessage> buffer = list.get(i);
				for (GmmsMessage msg : buffer.values()) {
					if(msg!=null){
						result.add(msg);
					}					
				}
			}
		} catch (Exception e) {
			log.warn(e, e);
		} finally {
			readLock.unlock();
		}
		return result;
	}

	public void clear() {
		try {
			readLock.lock();
			/*oneBuffer.clear();
			twoBuffer.clear();
			threeBuffer.clear();
			fourBuffer.clear();*/
			for(int i=0; i<10;i++){
				Map<Object, GmmsMessage> buffer = list.get(i);
				buffer.clear();
			}
		} catch (Exception e) {
			log.warn(e, e);
		} finally {
			readLock.unlock();
		}
	}

	public int size() {
		int size = 0;
		for(int i=0; i<10;i++){
			Map<Object, GmmsMessage> buffer = list.get(i);
			size= size + buffer.size();
		}
		return size;
	}

	/*public boolean isFull() {
		if (bufferCapacity <= 0) {
			return false;
		} else {
			return oneBuffer.size() >= bufferCapacity;
		}
	}*/

	public long getTimeout(TimeUnit timeUnit) {
		return timeUnit.convert(timeout, MILLISECONDS);
	}

	public void setTimeout(long timeout, TimeUnit timeUnit) {
		if (timeout <= 0) {
			throw new IllegalArgumentException("Timeout > 0, but now it's:"
					+ timeout);
		}
		this.timeout = MILLISECONDS.convert(timeout, timeUnit);
	}

	public long getWaitTime(TimeUnit timeUnit) {
		return timeUnit.convert(waitTime, MILLISECONDS);
	}

	public void setWaitTime(long waitTime, TimeUnit timeUnit) {
		if (waitTime < 0) {
			throw new IllegalArgumentException("WaitTime >=0, but now it's:"
					+ waitTime);
		}
		this.waitTime = MILLISECONDS.convert(waitTime, timeUnit);
	}
	
	public String toString() {
		return oneBuffer.toString();
	}
	
	public void checkTimeoutProcess() {
		Object timeoutKey = null;
		GmmsMessage timeoutMessage = null;
		
		/*fourBuffer.clear();
		try {
			writeLock.lock();
			fourBuffer = threeBuffer;
			if (twoBuffer.size() > 0) {
				threeBuffer = twoBuffer;
				secondRotate = true;
			} else {
				threeBuffer = new HashMap<Object, GmmsMessage>();
			}
			if (oneBuffer.size() > 0) {
				twoBuffer = oneBuffer;
				oneBuffer = new ConcurrentHashMap<Object, GmmsMessage>(
						bufferCapacity);
				firstRotate = true;
			} else {
				twoBuffer = new HashMap<Object, GmmsMessage>();
			}
		} catch (Exception e) {
			log.warn(e, e);
		} finally {
			writeLock.unlock();
		}
		for (Object key : fourBuffer.keySet()) {
			timeoutKey = key;
			timeoutMessage = fourBuffer.get(timeoutKey);
			
			if(log.isInfoEnabled()){
	        	log.info(timeoutMessage,"timeout in {}, listener is {}", bufferName, listener.getClass().getSimpleName());
	        }
			
			listener.timeout(timeoutKey, timeoutMessage);
		}*/
	}

	public UUID getUuid() {
		return uuid;
	}

	public String getBufferName() {
		return bufferName;
	}

	public void setBufferName(String bufferName) {
		this.bufferName = bufferName;
	}

}
