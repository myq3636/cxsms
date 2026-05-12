package com.king.message.gmms;

import java.io.PrintWriter;
import java.io.File;
import java.util.Date;
import java.text.SimpleDateFormat;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.MailSender;
import com.king.gmms.ha.ModuleURI;
import com.king.gmms.protocol.MessageToolkit;

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
public class PmqMessageWriter {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(PmqMessageWriter.class);
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
	private PmqFileMonitor monitor = null;
	private static SimpleDateFormat sdFormat = new SimpleDateFormat(
			"yyyyMMdd.HHmmss.SSS");
	protected GmmsUtility gmmsUtility = GmmsUtility.getInstance();
	private static PmqMessageWriter instance;

	private PmqMessageWriter() {
		isOpen = false;
		monitor = new PmqFileMonitor();
	}

	/**
	 * singleton mode
	 * 
	 * @return
	 */
	public static PmqMessageWriter getInstance() {
		if (instance == null) {
			instance = new PmqMessageWriter();
		}
		return instance;
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
		this.filePrefix = currentFilePath + moduleName + "." + suffix + ".";
		previousFilePath = currentFilePath + "link/";
		if (!createFolder(previousFilePath)) {
			return;
		}
		maxFileTime = Integer.parseInt(gmmsUtility.getCommonProperty(
				"PmqFileSwitchInterval", "600")) * 1000;
		maxFileSize = Integer.parseInt(gmmsUtility.getCommonProperty(
				"PmqFileMaxSize", "10240")) * 1024;
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
		initialize("pmq");
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
			log.error("Error open new PMQ file: " + fileName, ex);
			MailSender.getInstance().sendAlertMail(
					"A2P alert mail from " + ModuleURI.self().getAddress()
							+ " for open PMQ file Exception", ex);
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
		String fileStart = moduleName + ".pmq.";
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
	public boolean backupMessage(GmmsMessage message) {
		if (!isOpen) {
			log.info("Pmq file is not opened!");
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
		if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(message
				.getMessageType())
				|| GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(message
						.getMessageType())) {
			if (message.getOriginalQueue() == null) {
				message.setActionCode(-1);
				sb.append("INSERT INTO PMQ SET ");
				sb.append(makeUpdateStmt(message));
				sb.append(";");
			}
		}
		return gmmsUtility.filterSpecialChara(sb.toString());
	}

	/**
	 * 
	 * @param msg
	 * @return
	 */
	private String makeUpdateStmt(GmmsMessage msg) {
		StringBuilder sb = new StringBuilder(1024);
		sb.append("InTransID=").append(makeSqlStrReplace(msg.getInTransID()))
				.append(", ");
		sb.append("OutTransID=").append(makeSqlStrReplace(msg.getOutTransID()))
				.append(", ");
		sb.append("MsgID=").append(makeSqlStr(msg.getMsgID())).append(",");
		sb.append("InMsgID=").append(makeSqlStrReplace(msg.getInMsgID()))
				.append(", ");
		sb.append("OutMsgID=").append(makeSqlStrReplace(msg.getOutMsgID()))
				.append(", ");
		sb.append("O_Operator=").append(msg.getOoperator()).append(", ");
		sb.append("R_operator=").append(msg.getRoperator()).append(", ");
		sb.append("OSsID=").append(msg.getOSsID()).append(", ");
		sb.append("RSsID=").append(msg.getRSsID()).append(", ");
		sb.append("GmmsMsgType=").append(makeSqlStr(msg.getGmmsMsgType()))
				.append(", ");
		sb.append("MessageType=").append(makeSqlStr(msg.getMessageType()))
				.append(", ");
		sb.append("MessageSize=").append(msg.getMessageSize()).append(", ");
		sb.append("ProtocolVersion=").append(
				makeSqlStr(msg.getProtocolVersion())).append(", ");
		sb.append("SenderAddresses=").append(
				makeSqlStrReplace(msg.getSenderAddress())).append(", ");
		sb.append("RecipientAddresses=").append(
				makeSqlStrReplace(msg.getRecipientAddress())).append(", ");
		sb.append("OriginalSenderAddr=").append(
				makeSqlStrReplace(msg.getOriginalSenderAddr())).append(", ");
		sb.append("OriginalRecipientAddr=").append(
				makeSqlStrReplace(msg.getOriginalRecipientAddr())).append(", ");
		sb.append("TimeMark=").append(formatMySqlDate(msg.getTimeStamp()))
				.append(", ");
		sb.append("ExpiryDate=").append(formatMySqlDate(msg.getExpiryDate()))
				.append(", ");
		sb.append("DeliveryReport=").append(
				(msg.getDeliveryReport() ? "1" : "0")).append(", ");
		sb.append("RetriedNumber=").append(msg.getRetriedNumber()).append(", ");
		sb.append("NextRetryTime=").append(
				formatMySqlDate(msg.getNextRetryTime())).append(", ");
		if (msg.getOMncMcc() != null) {
			sb.append("OMncMcc=").append(makeSqlStr(msg.getOMncMcc())).append(
					", ");
		}
		if (msg.getRMncMcc() != null) {
			sb.append("RMncMcc=").append(makeSqlStr(msg.getRMncMcc())).append(
					", ");
		}
		sb.append("msgRefNum=").append(makeSqlStr(msg.getSarMsgRefNum()))
				.append(", ");
		sb.append("totalSegments=").append(msg.getSarTotalSeqments()).append(
				", ");
		sb.append("segmentSeqNum=").append(msg.getSarSegmentSeqNum()).append(
				", ");
		sb.append("Priority=").append(msg.getPriority()).append(", ");
		sb.append("ContentType=").append(makeSqlStr(msg.getContentType()))
				.append(", ");
		sb.append("StatusCode=").append(msg.getStatusCode()).append(", ");
		sb.append("StatusText=").append(makeSqlStr(msg.getStatusText()))
				.append(", ");
		
//		sb.append("InClientPull=").append((msg.inClientPull() ? "1" : "0"))
//				.append(", ");
		sb.append("InClientPull=").append((msg.getInClientPull()))
		.append(", ");
		
		sb.append("OutClientPull=").append((msg.outClientPull() ? "1" : "0"))
				.append(", ");
		sb.append("DeliveryChannel=").append(
				makeSqlStr(msg.getDeliveryChannel())).append(",");
		sb.append("SenderAddrType=")
				.append(makeSqlStr(msg.getSenderAddrType())).append(",");
		sb.append("RecipientAddrType=").append(
				makeSqlStr(msg.getRecipientAddrType())).append(",");
		sb.append("SenderAddrTon=").append(
				makeSqlStr(msg.getSenderAddrTon())).append(",");
		sb.append("RecipientAddrTon=").append(
				makeSqlStr(msg.getRecipientAddrTon())).append(",");		
		sb.append("MilterActionCode=").append(msg.getMilterActionCode())
				.append(",");
		sb.append("R_A2P=").append(msg.getRA2P()).append(",");
		sb.append("O_A2P=").append(msg.getOA2P()).append(",");
		sb.append("Current_A2P=").append(msg.getCurrentA2P()).append(",");
		sb.append("ActionCode=").append(msg.getActionCode()).append(",");
		sb.append("Split=").append(msg.getSplitStatus()).append(",");
		sb.append("ConnectionID=").append(makeSqlStr(msg.getConnectionID()))
				.append(",");
		sb.append("OperatorPriority=").append(msg.getOperatorPriority())
				.append(", ");
		sb.append("SpecialDCS=").append(
				makeSqlStr(msg.getSpecialDataCodingScheme())).append(", ");
		sb.append("DateIn=NOW() ");
		sb.append(",").append("TextContent=").append(
				makeSqlStrReplace(gmmsUtility.modifybackslash(
						replaceBreakLine(msg.getTextContent()), 1)));
		if (msg.getUdh() != null)
			sb.append(",").append("UDH=0x").append(getHexString(msg.getUdh()));
		if (msg.getMimeMultiPartData() != null)
			sb.append(",").append("Payload=0x").append(
					getHexString(msg.getMimeMultiPartData()));

		sb.append(",").append("InCsm=").append((msg.isInCsm() ? "1" : "0"));
		sb.append(",").append("ServiceTypeID=").append(msg.getServiceTypeID());
		sb.append(",").append("ScheduleDeliveryTime=").append(formatMySqlDate(msg.getScheduleDeliveryTime()));
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

	private String replaceBreakLine(String originalString) {
		if (originalString == null || originalString.length() == 0)
			return originalString;

		String tmp = originalString.replaceAll("\r\n", " ");

		return tmp.replaceAll("\n", " ");
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
				log.info("PMQWriter up to file max size");
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
			log.debug("PMQWriter up to file max time");
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
	class PmqFileMonitor implements Runnable {
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
			log.info("PMQFileMonitor thread stop!");
		}

		public void start() {
			running = true;
			Thread monitor = new Thread(A2PThreadGroup.getInstance(), this,
					"PMQFileMonitor");
			monitor.start();
			log.info("PMQFileMonitor thread start!");
		}

		public void stop() {
			running = false;
		}
	}

}
