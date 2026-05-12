package com.king.gmms.customerconnectionfactory;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.connection.ConnectionManager;
import com.king.gmms.domain.*;
import com.king.gmms.messagequeue.OperatorMessageQueue;

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
public abstract class AbstractShortConnectionFactory implements
		CustomerConnectionFactory {

	protected GmmsUtility gmmsUtility = null;
	protected ConcurrentHashMap<Integer, OperatorMessageQueue> ssid2messageQueues;
	protected A2PCustomerManager cim;
	protected boolean isServer;

	private static SystemLogger log = SystemLogger
			.getSystemLogger(AbstractShortConnectionFactory.class);

	public AbstractShortConnectionFactory() {
		gmmsUtility = GmmsUtility.getInstance();

		ssid2messageQueues = new ConcurrentHashMap<Integer, OperatorMessageQueue>();
		cim = gmmsUtility.getCustomerManager();
	}

	protected void setOperatorMessageQueue(int ssid, OperatorMessageQueue queue) {
		ssid2messageQueues.put(ssid, queue);
	}

	public void removeOperatorMessageQueue(int ssid) {
		if (ssid2messageQueues.containsKey(ssid)) {
			OperatorMessageQueue messageQueue = ssid2messageQueues.remove(ssid);
			if(messageQueue != null){
				messageQueue.stopMessageQueue();
			}
		}
	}

	public synchronized OperatorMessageQueue getOperatorMessageQueue(int ssid) {
		if (ssid2messageQueues.containsKey(ssid)) {
			return ssid2messageQueues.get(ssid);
		} else {
			return this.constructOperatorMessageQueue(ssid);
		}

	}

	public OperatorMessageQueue constructOperatorMessageQueue(int ssid) {
		if (ssid2messageQueues.containsKey(ssid)) {
			return ssid2messageQueues.get(ssid);
		}
		OperatorMessageQueue queue = createOperatorMessageQueue(ssid);
		if (queue != null) {
			log.trace("coming start message queue!ssid:{}", ssid);
			this.startOperatorMessageQueue(queue, ssid);
			this.setOperatorMessageQueue(ssid, queue);
			return queue;
		} else {
			log.debug("ssid:{} can not create messagequeue!", ssid);
			return null;
		}

	}

	protected abstract OperatorMessageQueue createOperatorMessageQueue(int ssid);

	protected abstract void startOperatorMessageQueue(
			OperatorMessageQueue queue, int ssid);

	// @Override
	public ConnectionManager getConnectionManager(int ssid, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	// @Override
	public ConnectionManager getConnectionManagerBySSID(int ssid) {
		// TODO Auto-generated method stub
		return null;
	}

	// @Override
	public void putConnectionManager(int ssid, String name,
			ConnectionManager ncm) {
		// TODO Auto-generated method stub

	}

}
