package com.king.message.gmms;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import sun.misc.Signal;
import sun.misc.SignalHandler;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.framework.lifecycle.LifecycleListener;
import com.king.framework.lifecycle.LifecycleSupport;
import com.king.framework.lifecycle.event.Event;
import com.king.gmms.GmmsUtility;
import com.king.gmms.util.SolarisSignal;

public class SdqMessageManager implements LifecycleListener, SignalHandler {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(SdqMessageManager.class);
	private SdqMessageWriter asynSdqWriter = null;
	private SdqMessageWriter synSdqWriter = null;
	private GmmsUtility gmmsUtility = null;
	private LinkedBlockingQueue<GmmsMessage> sdqQueue = null;
	private static SdqMessageManager instance = new SdqMessageManager();
	private SdqFileWriter sdqFileWriter = null;
	protected LifecycleSupport lifecycle;
	protected AtomicBoolean isAlow = new AtomicBoolean(true);

	private SdqMessageManager() {
		gmmsUtility = GmmsUtility.getInstance();
		int sdqQueueSize = Integer.parseInt(gmmsUtility.getModuleProperty(
				"MaxSDQAsynQueueSize", "1000"));
		sdqQueue = new LinkedBlockingQueue<GmmsMessage>(sdqQueueSize);

		synSdqWriter = new SdqMessageWriter("syn");
		synSdqWriter.initialize();
		asynSdqWriter = new SdqMessageWriter("asyn");
		asynSdqWriter.initialize();

		sdqFileWriter = new SdqFileWriter();
		sdqFileWriter.start();

		lifecycle = GmmsUtility.getInstance().getLifecycleSupport();
		lifecycle.addListener(Event.TYPE_SHUTDOWN, this, 1);
	}

	public static SdqMessageManager getInstance() {
		return instance;
	}

	public int OnEvent(Event event) {
		log.trace("Event Received. Type: {}", event.getEventType());
		if (event.getEventType() == Event.TYPE_SHUTDOWN) {
			isAlow.set(false);
		}
		return 1;
	}

	/**
	 * handle kill signal
	 */
	public void handle(Signal signal) {
		if (SolarisSignal.needHandle(signal)) {
			isAlow.set(false);
		}
	}

	public boolean insertSDQMessage(GmmsMessage message) {
		boolean result = false;

		if (message == null) {
			return result;
		}

		if (!isAlow.get()) {
			if(log.isInfoEnabled()){
				log.info(message, "Put msg to SDQ.");
			}
			return synSdqWriter.insertSDQMessage(message);
		}

		result = sdqQueue.offer(message);

		if (!result) {
			if(log.isInfoEnabled()){
				log.info(message, "Put msg to SDQ.");
			}
			result = synSdqWriter.insertSDQMessage(message);
		}

		return result;
	}

	class SdqFileWriter implements Runnable {
		volatile boolean running = false;
		GmmsMessage message = null;

		public void run() {
			while (running) {
				try {
					message = sdqQueue.poll(500L, TimeUnit.MILLISECONDS);
					if (message != null) {
						asynSdqWriter.insertSDQMessage(message);
					}
				} catch (Exception e) {
					log.error(e, e);
				}
			}
			log.info("CDRFileMonitor thread stop!");
		}

		public void start() {
			running = true;
			Thread monitor = new Thread(A2PThreadGroup.getInstance(), this,
					"SDQFileWriter");
			monitor.start();
			log.info("SDQFileWriter thread start!");
		}

		public void stop() {
			running = false;
		}
	}
}
