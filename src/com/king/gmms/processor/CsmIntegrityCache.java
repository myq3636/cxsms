package com.king.gmms.processor;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.MessageStoreManager;

/**
 * @author bensonchen
 * @version 1.0.0
 */
public class CsmIntegrityCache extends Thread {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(CsmIntegrityCache.class);
	private ConcurrentHashMap<CsmKeyInfo, CsmValueInfoMark> oneBuffer;
	private ConcurrentHashMap<CsmKeyInfo, CsmValueInfoMark> twoBuffer;
	private ConcurrentHashMap<CsmKeyInfo, CsmValueInfoMark> threeBuffer;
	private ConcurrentHashMap<CsmKeyInfo, CsmValueInfoMark> fourBuffer;
	
	private MessageStoreManager msm = null;

	private Object sync = new Object();

	/**
	 * use to clear expired cache info
	 */
	private long timeout = 3600000; // millisecond, 1 hour

	private long waitTime = 200; // millisecond
	private int bufferCapacity;
	private boolean running;

	public CsmIntegrityCache() {
		this(0);
	}

	public CsmIntegrityCache(int bufferCapacity) {
		super(A2PThreadGroup.getInstance(), "CsmIntegrityCache");
		if (bufferCapacity < 0) {
			throw new IllegalArgumentException("Capacity must bigger than 0!");
		}

		this.bufferCapacity = bufferCapacity;
		this.running = false;
		this.oneBuffer = new ConcurrentHashMap<CsmKeyInfo, CsmValueInfoMark>();
		this.twoBuffer = new ConcurrentHashMap<CsmKeyInfo, CsmValueInfoMark>();
		this.threeBuffer = new ConcurrentHashMap<CsmKeyInfo, CsmValueInfoMark>();
		this.fourBuffer = new ConcurrentHashMap<CsmKeyInfo, CsmValueInfoMark>();
		
		msm = GmmsUtility.getInstance().getMessageStoreManager();

	}

	/**
	 * put a message to cache
	 * 
	 * @param key
	 * @param gmmsMessage
	 * @return
	 */
	private void put(CsmKeyInfo key, GmmsMessage gmmsMessage) {
		if (key == null || bufferCapacity < 1)
			return;

		synchronized (sync) {
			if (oneBuffer.size() >= bufferCapacity) {
				try {
					oneBuffer.wait(waitTime);
				} catch (InterruptedException e) {
						log.debug(e, e);
					interrupted();
				}
			}
			if (oneBuffer.size() >= bufferCapacity) {
				log.warn(gmmsMessage, "CsmIntegrityCache is full");
				return;
			}

			CsmValueInfo csmValueInfo = CsmUtility
					.getCsmValueInfoFromGmmsMessage(gmmsMessage);
			CsmValueInfoMark infoMark = get(key);
			if (infoMark == null) {
				infoMark = new CsmValueInfoMark();
				infoMark.add(csmValueInfo);
				setDRFlag(infoMark,gmmsMessage);//set DR flag
				CsmValueInfoMark putRet = oneBuffer.putIfAbsent(key,
						infoMark);
				// put failed
				if (putRet != null) {
					putRet.add(csmValueInfo);
				}
			} else {
				infoMark.add(csmValueInfo);
				setDRFlag(infoMark,gmmsMessage);//set DR flag
			}

		}
		 if(log.isTraceEnabled()){
			 log.trace(gmmsMessage,
				"Put a message to CsmIntegrityCache, oneBuffer size:{}", oneBuffer.size());
		 }
	}
	
	/**
	 * Set DR flag for CsmValueInfoMark if one of the concatenated messages is needed return DR
	 * @param infoMark
	 * @param gmmsMessage
	 */
	public void setDRFlag(CsmValueInfoMark infoMark,GmmsMessage gmmsMessage){		
		if(!infoMark.isDeliverReport()){
			boolean drFlag = gmmsMessage.getDeliveryReport();
			if(drFlag) infoMark.setDeliverReport(drFlag);
		}
	}
	
	public void putAndInsertCsmq(CsmKeyInfo key, GmmsMessage message) {
		synchronized (sync) {
			// update msgID and timeStamp as the first(previous) arrived csm
			CsmValueInfoMark mark = get(key);
			if (mark != null) {
				Iterator<CsmValueInfo> iter = mark.getValueSet().iterator();
				if (iter.hasNext()) {
					CsmValueInfo info = iter.next();
					message.setTimeStamp(info.getTimeStamp());
					message.setMsgID(info.getMsgID());
				}
			}
			
			// put to cache
			put(key, message);
			// insert to db
			msm.sendToCsmq(message);
		}
	}

	/**
	 * remove from cache by key
	 * 
	 * @param key
	 * @return
	 */
	private void remove(CsmKeyInfo key) {
		CsmValueInfoMark infoMark = null;
		synchronized (sync) {
			infoMark = threeBuffer.remove(key);
			if (infoMark == null) {
				infoMark = twoBuffer.remove(key);
				if (infoMark == null) {
					infoMark = oneBuffer.remove(key);
				}
			}
			
			// remove from db CSMQ
			msm.clearCsmqByCsmKeyInfo(key, infoMark);
		}
		if (infoMark != null) {
			if(log.isTraceEnabled()){
				log.trace("{} removed from CsmIntegrityCache.", key);
			}
		}
	}

	public synchronized void startMonitor() {
		if (!running) {
			running = true;
			start();
		}
	}

	public boolean isRunning() {
		return running;
	}

	public void run() {
			log.debug("CsmIntegrityCache start.");

		// a new message will be switch from oneBuffer to fourBuffer after
		// (timeout~3/2 timeout)
		long sleepTime = timeout / 2;
		while (running) {
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
				interrupted();
				if(log.isTraceEnabled()){
					log.trace("CsmIntegrityCache is interrupted:{}", e.getMessage());
				}
			}

			synchronized (sync) {
				fourBuffer.clear();
				log.trace("fourBuffer has been cleared.");

				fourBuffer = threeBuffer;
				threeBuffer = twoBuffer;
				twoBuffer = oneBuffer;
				oneBuffer = new ConcurrentHashMap<CsmKeyInfo, CsmValueInfoMark>();
			}
		}
		log.trace("CsmIntegrityCache exit!");
	}
	
	/**
	 * If cache not exist, check csmq
	 * @param csmKeyInfo
	 */
	public void checkCsmq(CsmKeyInfo csmKeyInfo) {
		synchronized (sync) {
			if (null == get(csmKeyInfo)) {
				// load from db CSMQ
				List<GmmsMessage> dbList = msm
						.getMessagesFromCsmqByCsmKeyInfo(csmKeyInfo);
				if (dbList != null && dbList.size() > 0) {
					// insert csmq msg to cache
					for (GmmsMessage dbMsg : dbList) {
						// check if csmq msg expired
						long now = new Date().getTime();
						long dateInTime = dbMsg.getDateIn().getTime();
						if ((now - dateInTime) > timeout) {
							// expired csm
							continue;
						}

						// add check, in case of cc 02 01; cc 02 03; cc 02 02
						if (CsmUtility.isValidCsms(dbMsg)) {
							put(csmKeyInfo, dbMsg);
						} 
					}
				}
			}
		}
		
	}
	
	/**
	 * If hasReceivedAllCsm, return assembled message, else return null
	 * @param csmKeyInfo
	 * @param message
	 * @return
	 */
	public GmmsMessage assembleMessages(CsmKeyInfo csmKeyInfo, GmmsMessage message) {
		synchronized (sync) {
			GmmsMessage assembledMsg = null;
			CsmValueInfoMark csmValueInfoMark = get(csmKeyInfo);
			if (csmValueInfoMark != null && csmValueInfoMark.hasReceivedAllCsm(message)) {
				// Assemble messages
				try {					
					assembledMsg = CsmUtility.assembleCsm(csmValueInfoMark, message);					
				} catch (Exception e) {
					log.warn(message, "Occur error when assemble csm message", e);
				}
				
				// remove from csmIntegrityCache
				remove(csmKeyInfo);
			}
			return assembledMsg;
		}
		
	}
	
	/**
	 * get CsmValueInfoMark from cache by key
	 * 
	 * @param key
	 * @return
	 */
	private CsmValueInfoMark get(CsmKeyInfo key) {
		synchronized (sync) {
			CsmValueInfoMark infoMark = threeBuffer.get(key);
			if (infoMark == null) {
				infoMark = twoBuffer.get(key);
				if (infoMark == null) {
					infoMark = oneBuffer.get(key);
					if (infoMark == null) {
						return null;
					}
				}
			}
			return infoMark;
		}
	}

	public void setTimeout(long timeout) {
		if (running) {
			throw new IllegalStateException(
					"The CsmIntegrityCache has already start!");
		}
		if (timeout <= 0) {
			throw new IllegalArgumentException(
					"Timeout should > 0, but now it's:" + timeout);
		}
		this.timeout = timeout;
	}

	public void setWaitTime(long waitTime) {
		if (running) {
			throw new IllegalStateException(
					"The CsmIntegrityCache has already start!");
		}
		if (waitTime < 0) {
			throw new IllegalArgumentException(
					"WaitTime should >=0, but now it's:" + waitTime);
		}
		this.waitTime = waitTime;
	}
}
