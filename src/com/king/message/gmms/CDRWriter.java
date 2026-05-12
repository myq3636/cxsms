package com.king.message.gmms;

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
import java.io.*;
import java.util.*;
import java.text.*;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.MailSender;
import com.king.gmms.ha.ModuleURI;

public class CDRWriter {
	private static SystemLogger log = SystemLogger.getSystemLogger(CDRWriter.class);
	private static CDRWriter instance;
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
	protected Object mutex = new Object();
	private static SimpleDateFormat sdFormat = new SimpleDateFormat(
			"yyyyMMdd.HHmmss.SSS");

	public static CDRWriter getInstance() {
		if (instance == null) {
			instance = new CDRWriter();
		}
		return instance;
	}

	private CDRWriter() {
		isInitialize = false;
		isOpen = false;
		monitor = new CDRFileMonitor();
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

		this.maxFileTime = CDRManager.getMaxFileTime() + 1000L;
		this.maxFileSize = CDRManager.getMaxFileSize();
		this.filePrefix = currentFilePath + moduleName + ".cdr.";

		backupFile(currentFilePath);
		isOpen = open();
		monitor.start();
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
			synchronized(mutex){
				currentFile = new File(fileName);
				outStream = new PrintWriter(fileName, "UTF-8");
				currentFileSize = 0;
				lastFileTime = System.currentTimeMillis();
			}
			// restartTimer();
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
		if (!isOpen) {
			return false;
		}
		return write(cdr);
	}

	private synchronized boolean write(String content) {
		boolean result = false;
		try {
			if (currentFileSize > maxFileSize) {
				log.info("CDRWriter up to file max size");
				newLog();
			}

			synchronized(mutex){
				currentFileSize = currentFileSize + content.length();
				outStream.write(content);
				if (outStream.checkError()) {
					reOpen();
					Exception exception = new Exception("Error occur in the cdr file:"
							+ currentFile.getName());
					MailSender.getInstance().sendAlertMail(
							"A2P alert mail from " + ModuleURI.self().getAddress()
									+ " for CDR writer Exception", exception);
					throw exception;
				}
			}
			result = true;
		} catch (Exception ex) {
			log.error("Error Inserting new CDR: " + content, ex);
		}
		return result;
	}

	private synchronized void swichFile() {
		if (System.currentTimeMillis() - lastFileTime >= maxFileTime) {
			log.debug("CDRWriter up to file max time");
			if (!isOpen || (currentFileSize == 0)) {
				lastFileTime = System.currentTimeMillis();
				return;
			}
			newLog();
		}
	}

	private void newLog() {
		synchronized (mutex) {
			close();
			open();
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
		String fileStart = moduleName + ".cdr.";
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

}
