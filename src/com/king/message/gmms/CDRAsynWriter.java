package com.king.message.gmms;

import java.io.File;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import sun.misc.Signal;
import sun.misc.SignalHandler;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.framework.lifecycle.LifecycleListener;
import com.king.framework.lifecycle.LifecycleSupport;
import com.king.framework.lifecycle.event.Event;
import com.king.gmms.GmmsUtility;
import com.king.gmms.MailSender;
import com.king.gmms.ha.ModuleURI;
import com.king.gmms.util.SolarisSignal;

public class CDRAsynWriter implements LifecycleListener, SignalHandler {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(CDRAsynWriter.class);
	private static CDRAsynWriter instance;
	private static GmmsUtility gmmsUtility = null;
	private boolean isInitialize;
	private boolean isOpen;
	private String currentFilePath = null;
	private String previousFilePath = null;
	private File currentFile = null;
	private int maxFileSize;
	private long maxFileTime;
	private int currentFileSize;
	private long lastFileTime;
	private PrintWriter outStream;
	private String moduleName = null;
	private String filePrefix = null;
	private CDRFileMonitor monitor = null;
	private CDRFileWriter cdrWriter = null;
	private LinkedBlockingQueue<String> cdrQueue = null;
	private int cdrQueueSize = 1000;
	private static SimpleDateFormat sdFormat = new SimpleDateFormat(
			"yyyyMMdd.HHmmss.SSS");
	protected Object mutex = new Object();
	protected LifecycleSupport lifecycle;

	protected AtomicBoolean isAlow = new AtomicBoolean(true);

	public static CDRAsynWriter getInstance() {
		if (instance == null) {
			instance = new CDRAsynWriter();
		}
		return instance;
	}

	private CDRAsynWriter() {
		isInitialize = false;
		isOpen = false;
		gmmsUtility = GmmsUtility.getInstance();
		cdrQueueSize = Integer.parseInt(gmmsUtility.getModuleProperty(
				"MaxCDRAsynQueueSize", "1000"));
		monitor = new CDRFileMonitor();
		cdrWriter = new CDRFileWriter();
		cdrQueue = new LinkedBlockingQueue<String>(cdrQueueSize);
		lifecycle = GmmsUtility.getInstance().getLifecycleSupport();
		lifecycle.addListener(Event.TYPE_SHUTDOWN, this, 1);
	}

	public int OnEvent(Event event) {
		log.trace("Event Received. Type: {}", event.getEventType());
		if (event.getEventType() == Event.TYPE_SHUTDOWN) {
			isAlow.set(false);
		}
		return 1;
	}

	public void initialize() {
		if (isInitialize) {
			return;
		}

		currentFilePath = System.getProperty("a2p_home") + "/cdr/";
		if (!createFolder(currentFilePath)) {
			return;
		}

		moduleName = System.getProperty("module");
		currentFilePath = currentFilePath + "/";
		if (!createFolder(currentFilePath)) {
			return;
		}

		previousFilePath = currentFilePath + "link/";
		if (!createFolder(previousFilePath)) {
			return;
		}

		this.maxFileTime = CDRManager.getMaxFileTime();
		this.maxFileSize = CDRManager.getMaxFileSize();
		this.filePrefix = currentFilePath + moduleName + ".asyn.cdr.";

		backupFile(currentFilePath);
		isOpen = open();
		monitor.start();
		cdrWriter.start();
		isInitialize = true;
	}

	public void stopCDRFileMonitor() {
		if (monitor != null) {
			monitor.stop();
		}
	}

	private boolean createFolder(String folderName) {
		File d = new File(folderName);
		if (d.exists() == false) {
			if (d.mkdirs() == false) {
				log.error("Error create folder: {}", folderName);
				MailSender.getInstance().sendAlertMail(
						"A2P alert mail from " + ModuleURI.self().getAddress()
								+ " for error create folder", "", null);
				return false;
			}
		}
		return true;
	}

	private boolean open() {
		String tempTime = sdFormat.format(new Date());
		String fileName = filePrefix + tempTime;
		boolean result = false;

		try {
			synchronized (mutex) {
				currentFile = new File(fileName);
				outStream = new PrintWriter(fileName, "UTF-8");
				currentFileSize = 0;
				lastFileTime = System.currentTimeMillis();
			}
			result = true;
		} catch (Exception ex) {
			log.error("Error open new CDR file: " + fileName, ex);
			MailSender.getInstance().sendAlertMail(
					"A2P alert mail from " + ModuleURI.self().getAddress()
							+ " for open CDR file Exception", ex);
		}
		return result;
	}

	public boolean recoreCDR(String cdr) {
		if (cdr == null || !isOpen || !isAlow.get()) {
			return false;
		}
		return cdrQueue.offer(cdr);
	}

	private boolean write(String content) {
		boolean result = false;
		try {
			if (currentFileSize > maxFileSize) {
				log.info("CDRWriter up to file max size");
				newLog();
			}

			synchronized (mutex) {
				currentFileSize = currentFileSize + content.length();
				outStream.write(content);
				if (outStream.checkError()) {
					reOpen();
					Exception exception = new Exception("Error occur in the cdr file:"
							+ currentFile.getName());
					MailSender.getInstance().sendAlertMail(
							"A2P alert mail from " + ModuleURI.self().getAddress()
									+ " for CDR Asny writer Exception", exception);
					throw exception;
				}
			}
			result = true;
		} catch (Exception ex) {
			log.error("Error Inserting new CDR: " + content, ex);
		}
		return result;
	}

	private void swichFile() {
		if (System.currentTimeMillis() - lastFileTime >= maxFileTime) {
			log.debug("CDRWriter up to file max time");
			if (!isOpen || (currentFileSize == 0)) {
				lastFileTime = System.currentTimeMillis();
				return;
			}
			newLog();
		}
	}

	private void reOpen(){
		try {
			synchronized (mutex) {
				if(currentFileSize >0 ){
					close();
					open();
				}else{
					String fileName = currentFile.getPath();
					outStream.close();
					currentFile.delete();
					currentFile = new File(fileName);
					outStream = new PrintWriter(fileName, "UTF-8");
					currentFileSize = 0;
					lastFileTime = System.currentTimeMillis();
				}
			}
		} catch (Exception ex) {
			MailSender.getInstance().sendAlertMail(
					"A2P alert mail from " + ModuleURI.self().getAddress()
							+ " for open CDR file Exception", ex);
		}
	}
	
	private void newLog() {
		synchronized (mutex) {
			close();
			open();
		}
	}

	boolean close() {
		outStream.close();
		backupFile();
		return true;
	}

	void backupFile() {
		if (currentFile == null) {
			return;
		}
		String backupFile = previousFilePath + currentFile.getName();
		File backFile = new File(backupFile);
		currentFile.renameTo(backFile);
	}

	void backupFile(String filePathName) {
		File[] fileList;
		File dirFile = new File(filePathName);
		fileList = dirFile.listFiles();
		String fileStart = moduleName + ".asyn.cdr.";
		if (fileList == null || fileList.length == 0) {
			return;
		}
		for (int i = 0; i < fileList.length; i++) {
			String fileName = fileList[i].getName();
			if (fileList[i].isFile() == false
					|| !fileName.startsWith(fileStart)) {
				continue;
			}
			currentFile = new File(fileList[i].toString());
			if (currentFile.length() > 0) {
				backupFile();
			} else {
				currentFile.delete();
			}
		}
	}

	/**
	 * @Override handle kill signal
	 */
	public void handle(Signal signal) {
		if (SolarisSignal.needHandle(signal)) {
			isAlow.set(false);
		}
	}

	class CDRFileMonitor implements Runnable {
		volatile boolean running = false;

		public void run() {
			while (isOpen && running) {
				try {
					Thread.sleep(1000L);
					long waitTime = maxFileTime
							- (System.currentTimeMillis() - lastFileTime);
					if (waitTime > 0L) {
						Thread.sleep(waitTime);
					}
					swichFile();
				} catch (Exception e) {
					log.error(e, e);
				}
			}
			log.info("CDRFileMonitor thread stop!");
		}

		public void start() {
			running = true;
			Thread monitor = new Thread(A2PThreadGroup.getInstance(), this,
					"CDRFileMonitor");
			monitor.start();
			log.info("CDRFileMonitor thread start!");
		}

		public void stop() {
			running = false;
		}
	}

	class CDRFileWriter implements Runnable {
		volatile boolean running = false;
		String cdr = null;

		public void run() {
			while (running) {
				try {
					cdr = cdrQueue.poll(500L, TimeUnit.MILLISECONDS);
					if (cdr != null) {
						write(cdr);
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
					"CDRFileWriter");
			monitor.start();
			log.info("CDRFileWriter thread start!");
		}

		public void stop() {
			running = false;
		}
	}

}
