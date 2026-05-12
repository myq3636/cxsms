package com.king.message.gmms;

import java.io.File;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.MailSender;
import com.king.gmms.ha.ModuleURI;
import com.king.gmms.protocol.MessageToolkit;

public class SdqMessageWriter {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(SdqMessageWriter.class);
	protected boolean isOpen;
	protected PrintWriter outStream;
	protected String currentFilePath = null;
	private String previousFilePath = null;
	protected String moduleName = null;
	protected int currentFileSize;
	protected File currentFile = null;
	protected String filePrefix = null;
	protected int maxFileSize = 10 * 1024 * 1024;
	protected long maxFileTime = 300 * 1000;// ms
	private long lastFileTime;
	private SdqFileMonitor monitor = null;
	private static SimpleDateFormat sdFormat = new SimpleDateFormat(
			"yyyyMMdd.HHmmss.SSS");
	protected GmmsUtility gmmsUtility = GmmsUtility.getInstance();
	private String synSuffix = "syn";

	public SdqMessageWriter(String asyn) {
		isOpen = false;
		monitor = new SdqFileMonitor();
		if (asyn != null) {
			synSuffix = asyn;
		}
	}

	/**
	 * initialize
	 * 
	 * @param group
	 */
	public void initialize(String suffix) {
		currentFilePath = System.getProperty("a2p_home") + "/queue/" + suffix
		+ "/";
		moduleName = System.getProperty("module");
		if (!createFolder(currentFilePath)) {
			return;
		}
		this.filePrefix = currentFilePath + moduleName + "." + suffix + "."
				+ synSuffix + ".";
		previousFilePath = currentFilePath + "link/";
		if (!createFolder(previousFilePath)) {
			return;
		}
		maxFileTime = Integer.parseInt(gmmsUtility.getCommonProperty(
				"SdqFileSwitchInterval", "300")) * 1000;
		maxFileSize = Integer.parseInt(gmmsUtility.getCommonProperty(
				"SdqFileMaxSize", "10240")) * 1024;
		backupFile(currentFilePath);
		isOpen = open();
		monitor.start();
	}

	/**
	 * initialize
	 * 
	 * @param group
	 */
	public void initialize() {
		initialize("sdq");
	}

	/**
	 * createFolder
	 * 
	 * @param folderName
	 * @return
	 */
	protected boolean createFolder(String folderName) {
		File d = new File(folderName);
		if (d.exists() == false) {
			if (d.mkdirs() == false) {
				log.error("Error create folder: {}", folderName);
				MailSender.getInstance().sendAlertMail(
						"A2P alert mail from " + ModuleURI.self().getAddress()
								+ " for error message backup create folder",
						"", null);
				return false;
			}
		}
		return true;
	}

	/**
	 * open file
	 * 
	 * @return
	 */
	private boolean open() {
		String tempTime = sdFormat.format(new Date());
		String fileName = filePrefix + tempTime;
		currentFile = new File(fileName);
		boolean result = false;

		try {
			outStream = new PrintWriter(fileName, "UTF-8");
			currentFileSize = 0;
			lastFileTime = System.currentTimeMillis();
			result = true;
		} catch (Exception ex) {
			log.error("Error open new SDQ file: " + fileName, ex);
			MailSender.getInstance().sendAlertMail(
					"A2P alert mail from " + ModuleURI.self().getAddress()
							+ " for open SDQ file Exception", ex);
		}
		return result;
	}

	/**
	 * backupFile
	 */
	void backupFile() {
		if (currentFile == null) {
			return;
		}
		String backupFile = previousFilePath + currentFile.getName();
		File backFile = new File(backupFile);
		currentFile.renameTo(backFile);
	}

	/**
	 * backupFile
	 * 
	 * @param filePathName
	 */
	void backupFile(String filePathName) {
		File[] fileList;
		File dirFile = new File(filePathName);
		fileList = dirFile.listFiles();
		String fileStart = moduleName + ".sdq." + synSuffix + ".";
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
	 * 
	 * @param message
	 * @return
	 */
	public boolean insertSDQMessage(GmmsMessage message) {
		if (!isOpen) {
			log.info("SDQ file is not opened!");
			return false;
		}
		return write(constructMsg(message));
	}

	/**
	 * 
	 * @param message
	 * @return
	 */
	private String constructMsg(GmmsMessage message) {
		StringBuilder sb = new StringBuilder();
		message.setActionCode(-1);
		sb.append("INSERT INTO SDQ SET ");
		sb.append(makeUpdateStmt(message));
		sb.append(";");
		return gmmsUtility.filterSpecialChara(sb.toString());
	}

	/**
	 * 
	 * @param msg
	 * @return
	 */
	private String makeUpdateStmt(GmmsMessage msg) {
		StringBuilder sb = new StringBuilder(1024);
		sb.append("OutMsgID=").append(makeSqlStrReplace(msg.getOutMsgID()))
				.append(", ");
		sb.append("RSsID=").append(msg.getRSsID()).append(", ");
		if (msg.getTextContent() != null) {
			sb.append("TextContent=").append(
					makeSqlStrReplace(gmmsUtility.modifybackslash(msg
							.getTextContent(), 1))).append(", ");
		}
		sb.append("StatusCode=").append(msg.getStatusCode()).append(", ");
		sb.append("StatusText=").append(makeSqlStr(msg.getStatusText()))
				.append(", ");
		sb.append("DeliveryChannel=").append(makeSqlStrReplace(msg.getDeliveryChannel())).append(", ");
		sb.append("DateIn=NOW() ");
		return sb.toString();
	}

	/**
	 * 
	 * @param strSQL
	 * @return
	 */
	private String makeSqlStrReplace(String strSQL) {
		if (strSQL == null) {
			return "NULL";
		}
		strSQL = strSQL.replaceAll("'", "''");
		return new StringBuilder("'").append(strSQL).append("'").toString();
	}

	private String makeSqlStr(String strSQL) {
		if (strSQL == null) {
			return "NULL";
		}
		return new StringBuilder("'").append(strSQL).append("'").toString();
	}

	/**
	 * 
	 * @param date
	 * @return
	 */
	private String formatMySqlDate(java.util.Date date) {
		if (date == null) {
			return null;
		}
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		return "'" + formatter.format(date) + "'";
	}

	/**
	 * 
	 * @param data
	 * @return
	 */
	public String getHexString(byte[] data) {
		StringBuilder sb = new StringBuilder();
		for (byte bb : data) {
			String element = Integer.toHexString(bb);
			element = MessageToolkit.makeNumericString(element, 2); // format
																	// the
																	// length
			sb.append(element);
		}
		return sb.toString();
	}

	/**
	 * 
	 * @param content
	 * @return
	 */
	private synchronized boolean write(String content) {
		boolean result = false;
		try {
			if (currentFileSize > maxFileSize) {
				log.info("SDQWriter up to file max size");
				rotate();
			}

			currentFileSize = currentFileSize + content.length();
			outStream.write(content + "\n");
			if (outStream.checkError()) {
				throw new Exception("Error occur in the backup message file:"
						+ currentFile.getName());
			}
			result = true;
		} catch (Exception ex) {
			log.error("Error backup message: " + content, ex);
		}
		return result;
	}

	/**
	 * close
	 */
	public void close() {
		outStream.close();
		backupFile();
	}

	/**
	 * rotate
	 */
	private void rotate() {
		close();
		open();
	}

	/**
	 * swichFile
	 */
	private synchronized void swichFile() {
		if (System.currentTimeMillis() - lastFileTime >= maxFileTime) {
			//log.debug("SDQWriter up to file max time");
			if (!isOpen || (currentFileSize == 0)) {
				lastFileTime = System.currentTimeMillis();
				return;
			}
			rotate();
		}
	}

	/**
	 * PmqFileMonitor
	 * 
	 * @author jianmingyang
	 * 
	 */
	class SdqFileMonitor implements Runnable {
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
			log.info("SDQFileMonitor thread stop!");
		}

		public void start() {
			running = true;
			Thread monitor = new Thread(A2PThreadGroup.getInstance(), this,
					"SdqFileMonitor");
			monitor.start();
			log.info("SDQFileMonitor thread start!");
		}

		public void stop() {
			running = false;
		}
	}

}
