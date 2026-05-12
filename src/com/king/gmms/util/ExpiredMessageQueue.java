package com.king.gmms.util;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.*;
import com.king.gmms.processor.MemoryQueueManagement;
import com.king.message.gmms.*;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * Title:
 * </p>
 * 
 * <p>
 * Description:
 * </p>
 * 
 * <p>
 * Copyright: Copyright (c) 2006
 * </p>
 * 
 * <p>
 * Company:
 * </p>
 * 
 * @author not attributable
 * @version 1.0
 */
public class ExpiredMessageQueue implements Runnable, Queue {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(ExpiredMessageQueue.class);
	protected LinkedList<MessagePair> messageQueue = null;
	protected LinkedBlockingQueue<GmmsMessage> backupMessageQueue = null;
	private long lastMsgTimeMillis = 0;
	private int limit = 20000;
	private long waitingTime = 200L;
	private QueueTimeoutInterface listener;
	private boolean running;
	private boolean isServer = false;
	private int timeout = 900000;
	private String queuename;
	private Object wait = new Object();
	private UUID uuid = null;
	protected AtomicBoolean isAlow = new AtomicBoolean(true);
	protected MemoryQueueManagement memoryQueueManager = null;
	protected Thread messageTimeoutThread = null;
	protected Thread expiredMessageThread = null;

	public ExpiredMessageQueue(int limit, int timeout, boolean isServer,
			String name) {
		if (limit > 0) {
			this.limit = limit;
		}
		messageQueue = new LinkedList<MessagePair>();
		backupMessageQueue = new LinkedBlockingQueue<GmmsMessage>();
		if (timeout <= 0) {
			this.timeout = 900000;
		} else {
			this.timeout = timeout;
		}
		this.isServer = isServer;
		queuename = name;
		uuid = UUID.randomUUID();
		memoryQueueManager = MemoryQueueManagement.getInstance();
	}

	public ExpiredMessageQueue(int timeout, boolean isServer, String name) {
		this(100000, timeout, isServer, name);
	}

	public ExpiredMessageQueue(boolean isServer, String name) {
		// never timeout
		this(100000, -1, isServer, name);
	}

	public int size() {
		return messageQueue.size();
	}

	public void run() {
		long checkInterval = timeout / 3;
		Object temp = null;
		try {
			while (running) {
				if (messageQueue.size() > 0
						&& System.currentTimeMillis() - lastMsgTimeMillis > timeout) {
					synchronized (messageQueue) {
						while (running) {
							temp = messageQueue.peek();
							if (temp == null)
								break;
							MessagePair pair = (MessagePair) temp;
							if (timeout > 0 && pair.isTimeout(timeout)) {
								messageQueue.removeFirst();
								Object message = pair.getMessage();
								if(log.isInfoEnabled()){
									log.info((GmmsMessage) message,
												"The message is timeout and backup from expired message queue");
								}
								addTimeoutMessage(message);
								continue;
							} else {
								break;
							}
						}
					}
				}
				try {
					Thread.sleep(checkInterval);
				} catch (InterruptedException ex) {
				}
			}
		} catch (Exception e) {
			running = false;
			log.warn(e, e);
		}
	}

	public void setListener(QueueTimeoutInterface listener) {
		this.listener = listener;
	}

	protected void addTimeoutMessage(Object msg) {
		if (msg != null) {
			try {
				backupMessageQueue.put((GmmsMessage) msg);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				log
						.warn((GmmsMessage) msg,
								"Failed to put timeout message to backup message queue");
			}
		}
	}

	public void handleTimeoutMessage(Object msg) {
		listener.timeout(msg);
	}

	public synchronized void start() {
		if (running) {
			return;
		}
		running = true;
		messageTimeoutThread = new Thread(A2PThreadGroup.getInstance(), this,
				"ExpiredMsgQ" + queuename);
		messageTimeoutThread.start();
		expiredMessageThread = new ExpiredMessageThread();
		expiredMessageThread.start();
		memoryQueueManager.register(uuid, this);
	}

	public synchronized void stop() {
		if (!running) {
			return;
		}
		isAlow.set(false);

		backupMessage();

		running = false;

		messageTimeoutThread.interrupt();
		
		expiredMessageThread.interrupt();
		
		memoryQueueManager.cancel(uuid);
	}

	protected void backupMessage() {
		MessagePair messagePair = messageQueue.poll();

		while (messagePair != null) {
			handleTimeoutMessage((GmmsMessage) messagePair.getMessage());
			messagePair = messageQueue.poll();
		}
		messageQueue.clear();

		try {
			GmmsMessage message = backupMessageQueue.poll(200L,
					TimeUnit.MILLISECONDS);
			while (message != null) {
				handleTimeoutMessage(message);
				message = backupMessageQueue.poll(200L, TimeUnit.MILLISECONDS);
			}
			backupMessageQueue.clear();

		} catch (Exception e) {
			log.warn("Failed to backup message");
		}
	}

	public Object get(long stimeout) throws InterruptedException {
		Object temp = null;
		Object message = null;
		try {
			if (!isAlow.get()) {
				tryWait();
				return null;
			} else {
				synchronized (messageQueue) {
					if (messageQueue.size() <= 0) {
						messageQueue.wait(stimeout);
					}
					if (messageQueue.size() <= 0) {
						return null;
					} else {
						temp = messageQueue.poll();
					}
				}
				if (temp != null) {
					MessagePair pair = (MessagePair) temp;
					message = pair.getMessage();
					lastMsgTimeMillis = pair.getTimeStamp();
				}
			}
		} catch (InterruptedException e) {
			log.warn(e, e);
			throw e;
		} catch (Exception e) {
			log.warn(e, e);
			return null;
		}
		return message;
	}

	/**
	 * get Message from queue
	 * 
	 * @return Object
	 */
	public Object get() {
		Object temp = null;
		Object message = null;
		try {
			if (!isAlow.get()) {
				tryWait();
				return null;

			} else {
				synchronized (messageQueue) {
					if (messageQueue.size() <= 0) {
						messageQueue.wait(waitingTime);
					}
					if (messageQueue.size() <= 0) {
						return null;
					} else {
						temp = messageQueue.poll();
					}
				}
				if (temp != null) {
					MessagePair pair = (MessagePair) temp;
					message = pair.getMessage();
					lastMsgTimeMillis = pair.getTimeStamp();
				}
			}
		} catch (Exception e) {
			log.warn(e, e);
			return null;
		}
		return message;
	}

	private void tryWait() {
		try {
			synchronized (wait) {
				wait.wait();
			}
		} catch (Exception ex) {
		}
	}

	public LinkedList<GmmsMessage> getAll() {
		if (!isAlow.get()) {
			tryWait();
		}
		LinkedList<GmmsMessage> queue = new LinkedList<GmmsMessage>();
		synchronized (messageQueue) {
			for (MessagePair pair : messageQueue) {
				queue.add((GmmsMessage) pair.getMessage());
				lastMsgTimeMillis = pair.getTimeStamp();
			}
		}
		return queue;
	}

	/**
	 * put message to the queue
	 * 
	 * @param obj
	 *            Object
	 * @return boolean
	 */
	public boolean put(Object obj) {
		if (!isAlow.get()) {
			GmmsMessage msg = (GmmsMessage) obj;
			addTimeoutMessage(msg);
			tryWait();
			return true;
		}

		MessagePair message = null;
		if (obj != null) {
			message = new MessagePair(obj);
			synchronized (messageQueue) {
				try {
					int count = 0;
					while (messageQueue.size() >= limit && count < 3) {
						messageQueue.wait(100);
						count++;
					}
					if (messageQueue.size() < limit) {
						messageQueue.offer(message);
						messageQueue.notifyAll();
						return true;
					}
				} catch (Exception e) {
					log.warn(e, e);
				}
			}
			log.warn((GmmsMessage) obj, "ExpiredMessageQueue is full");
		}
		return false;
	}

	public void waitingOnQueue(long time) {
		try {
			synchronized (messageQueue) {
				messageQueue.wait(time);
			}
		} catch (Exception e) {
			log.error(e, e);
		}
	}

	public void notifyOnQueue() {
		try {
			synchronized (messageQueue) {
				messageQueue.notifyAll();
			}
		} catch (Exception e) {
			log.error(e, e);
		}
	}

	/**
	 * get message without wait
	 * 
	 * @return Object
	 */
	public Object getNoWait() {
		Object temp = null;
		Object message = null;
		if (!isAlow.get()) {
			tryWait();
			return null;
		} else {
			if (messageQueue.isEmpty()) {
				return null;
			}
			synchronized (messageQueue) {
				temp = messageQueue.poll();
			}
			if (temp != null) {
				MessagePair pair = (MessagePair) temp;
				message = pair.getMessage();
				lastMsgTimeMillis = pair.getTimeStamp();
				return message;
			} else {
				return null;
			}
		}
	}

	public void addAll(ArrayList<GmmsMessage> list) {
		if (list == null)
			return;
		if (!isAlow.get()) {
			for (Object obj : list) {
				GmmsMessage msg = (GmmsMessage) obj;
				addTimeoutMessage(msg);
			}
			tryWait();
		}

		MessagePair message = null;
		for (Object obj : list) {
			message = new MessagePair(obj);
			synchronized (messageQueue) {
				try {
					int count = 0;
					while (messageQueue.size() > limit && count < 3) {
						messageQueue.wait(waitingTime);
						count++;
					}
					if (messageQueue.size() < limit) {
						messageQueue.offer(message);
						messageQueue.notifyAll();
						continue;
					}
				} catch (Exception e) {
					log.warn(e, e);
				}
			}
			log.warn("ExpiredMessageQueue is full");
			addTimeoutMessage(message.getMessage());
		}
	}

	class ExpiredMessageThread extends Thread {
		GmmsUtility gmmsUtility = GmmsUtility.getInstance();
		Thread thread = null;

		public ExpiredMessageThread() {
			gmmsUtility = GmmsUtility.getInstance();
		}

		public void start() {
			thread = new Thread(A2PThreadGroup.getInstance(), this,
					"ExpiredMsgDeal" + queuename);
			thread.start();
			log.debug("ExpiredMessageDealing Thread start for:{}", queuename);
		}
		
		public void interrupt(){
			try{
				thread.interrupt();
			}catch(Exception e){
			}
		}

		public void run() {
			GmmsMessage msg = null;
			while (running) {
				try {
					msg = backupMessageQueue.poll(1000L, TimeUnit.MILLISECONDS);
					if (msg != null) {
						if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg
								.getMessageType())
								|| GmmsMessage.MSG_TYPE_DELIVERY
										.equalsIgnoreCase(msg.getMessageType())
								|| GmmsMessage.MSG_TYPE_SUBMIT_RESP
										.equalsIgnoreCase(msg.getMessageType())
								|| GmmsMessage.MSG_TYPE_DELIVERY_RESP
										.equalsIgnoreCase(msg.getMessageType())) {
							msg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
						} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT
								.equalsIgnoreCase(msg.getMessageType())
								|| GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP
										.equalsIgnoreCase(msg.getMessageType())) {
							msg
									.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT
											.getCode());
						} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY
								.equalsIgnoreCase(msg.getMessageType())
								|| GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY_RESP
										.equalsIgnoreCase(msg.getMessageType())) {
							msg.setStatus(GmmsStatus.FAIL_QUERY_DELIVERREPORT);
						} else { // invalid message type
							log.warn(msg,
									"Unknown Message Type! when update the fail status"
											+ msg.getMessageType());
							msg.setStatus(GmmsStatus.UNKNOWN_ERROR);
							continue;
						}
						handleTimeoutMessage(msg);
					}
					if (messageQueue.size() >= limit) {
						log
								.warn(
										"The size of Expired Message Queue({}) is over limit and current message queue size is {}",
										queuename, messageQueue.size());
					}
				} catch (Exception e) {
					log.error(e, e);
				}
			}
			log.info("ExpiredMessageDealing Thread stop.");
		}
	}

	protected class MessagePair {
		private long timeStamp = 1000000000L;
		private Object message = null;

		MessagePair(Object message) {
			this.message = message;
			timeStamp = System.currentTimeMillis();
		}

		public Object getMessage() {
			return message;
		}

		public long getTimeStamp() {
			return timeStamp;
		}

		public boolean isTimeout(int timeout) {
			return System.currentTimeMillis() - timeStamp > (long) timeout;
		}
	}

	public static void main(String[] args) {
	}

	public int getTimeout() {
		return timeout;
	}

	public long getWaitingTime() {
		return waitingTime;
	}

	public boolean isServer() {
		return isServer;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public void setWaitingTime(long waitingTime) {
		this.waitingTime = waitingTime;
	}

	public void setIsServer(boolean isServer) {
		this.isServer = isServer;
	}

	public boolean isFull() {
		return messageQueue.size() >= limit;
	}

	public boolean putAll(Collection msgCollection) {
		if (msgCollection == null || msgCollection.size() <= 0) {
			return false;
		}
		MessagePair message = null;
		Iterator iterator = msgCollection.iterator();
		while (iterator.hasNext()) {
			message = new MessagePair(iterator.next());
			synchronized (messageQueue) {
				try {
					int count = 0;
					while (messageQueue.size() >= limit && count < 3) {
						messageQueue.wait(waitingTime);
						count++;
					}
					if (messageQueue.size() < limit) {
						messageQueue.offer(message);
						messageQueue.notifyAll();
						continue;
					}
				} catch (Exception e) {
					log.warn(e, e);
				}
			}
			log.warn("ExpiredMessageQueue full");
			addTimeoutMessage(message.getMessage());
		}
		return true;
	}
}