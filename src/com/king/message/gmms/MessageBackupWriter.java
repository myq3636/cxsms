package com.king.message.gmms;

import java.io.PrintWriter;
import java.io.File;
import java.util.Date;
import java.text.SimpleDateFormat;

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
public class MessageBackupWriter {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(MessageBackupWriter.class);

	private static MessageBackupWriter instance = new MessageBackupWriter();

	private boolean isOpen;
	private PrintWriter outStream;
	private String currentFilePath = null;
	private int currentFileSize;
	private File currentFile = null;
	private String filePrefix = null;
	private String moduleName = null;
	private GmmsUtility gmmsUtility = GmmsUtility.getInstance();

	private MessageBackupWriter() {
		isOpen = false;
	}

	public static MessageBackupWriter getInstance() {
		return instance;
	}

	public void initialize(int group) {
		currentFilePath = System.getProperty("a2p_home") + "/msgbackup/";
		moduleName = System.getProperty("module");
		if (!createFolder(currentFilePath)) {
			return;
		}
		this.filePrefix = currentFilePath + moduleName + ".msgbackup." + group;
		isOpen = open();
	}

	private boolean createFolder(String folderName) {
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

	private boolean open() {

		currentFile = new File(filePrefix);
		boolean result = false;

		try {
			outStream = new PrintWriter(filePrefix, "UTF-8");
			currentFileSize = 0;
			result = true;
		} catch (Exception ex) {
			log.error("Error open backup message file: " + filePrefix, ex);
			MailSender.getInstance().sendAlertMail(
					"A2P alert mail from " + ModuleURI.self().getAddress()
							+ " for backup message file Exception", ex);
		}
		return result;
	}

	private void updateMessage(GmmsMessage message) {

		if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(message
				.getMessageType())
				|| GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(message
						.getMessageType())) {
			message.setStatus(GmmsStatus.COMMUNICATION_ERROR);
		} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT
				.equalsIgnoreCase(message.getMessageType())) {
			message.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT
					.getCode());
		} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY
				.equalsIgnoreCase(message.getMessageType())) {
			message.setStatus(GmmsStatus.FAIL_QUERY_DELIVERREPORT);
		} else { // invalid message type
			log.warn(message,
					"Unknown Message Type! when update the fail status"
							+ message.getMessageType());
			message.setStatus(GmmsStatus.UNKNOWN_ERROR);
		}
	}

	public boolean backupMessage(GmmsMessage message) {
		if (!isOpen) {
			log.info("is not open!");
			return false;
		}
		return write(constructMsg(message));

	}

	private String constructMsg(GmmsMessage message) {
		StringBuilder sb = new StringBuilder();
		if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY.equalsIgnoreCase(message
				.getMessageType())) {
			sb.append("update QDQ ");
			sb.append(" set actioncode=-1, nextretrytime=now() ");
			sb.append("where inmsgid="
					+ makeSqlStrReplace(message.getInMsgID()));
			sb.append(";");
		}
		// else if (MessageMode.STORE_FORWARD == message.getMessageMode()) {
		if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(message
				.getMessageType())
				|| GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(message
						.getMessageType())) {
			if (message.getOriginalQueue() == null) {
				message.setActionCode(-1);
				message.setStatus(GmmsStatus.COMMUNICATION_ERROR);
				message.setNextRetryTime(new Date());
				sb.append("INSERT INTO RMQ SET ");
				sb.append(makeUpdateStmt(message));
				sb.append(";");
			} else {
				sb.append("update " + message.getOriginalQueue());
				sb.append(" set actioncode=-1, nextretrytime=now() ");
				sb.append("where inmsgid="
						+ makeSqlStrReplace(message.getInMsgID()));
				sb.append(";");
			}
		} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT
				.equalsIgnoreCase(message.getMessageType())) {
			if ("RDQ".equalsIgnoreCase(message.getOriginalQueue())) {
				sb.append("update " + message.getOriginalQueue());
				sb.append(" set actioncode=-1, nextretrytime=now() ");
				sb.append("where inmsgid="
						+ makeSqlStrReplace(message.getInMsgID()));
				sb.append(";");
			} else {
				message.setActionCode(-1);
				message.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT
						.getCode());
				message.setNextRetryTime(new Date());
				sb.append("INSERT INTO RDQ SET ");
				sb.append(makeUpdateStmt(message));
				sb.append(";");
			}
		}
		// }
		// else {
		// log.info(message, "message need not backup");
		// }
		return gmmsUtility.filterSpecialChara(sb.toString());
	}

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
		// sb.append("MessageMode=").append( (msg.getMessageMode() == null ?
		// MessageMode.STORE_FORWARD.getValue() :
		// msg.getMessageMode().getValue())).
		// append(", ");
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
		// sb.append("LocalTimeMark=").append(formatMySqlDate(msg.
		// getLocalTimeStamp())).append(", ");
		sb.append("ExpiryDate=").append(formatMySqlDate(msg.getExpiryDate()))
				.append(", ");
		sb.append("DeliveryReport=").append(
				(msg.getDeliveryReport() ? "1" : "0")).append(", ");
		// sb.append("ReadReply=").append( (msg.getReadReply() ? "1" : "0")).
		// append(", ");
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
		sb.append("InClientPull=").append(msg.getInClientPull()).append(", ");
		
		
		sb.append("OutClientPull=").append((msg.outClientPull() ? "1" : "0"))
				.append(", ");
		sb.append("DeliveryChannel=").append(
				makeSqlStr(msg.getDeliveryChannel())).append(",");
		sb.append("SenderAddrType=")
				.append(makeSqlStr(msg.getSenderAddrType())).append(",");
		// sb.append("BillingId=").append(makeSqlStr(msg.getBillingId())).append(
		// ",");
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
		// sb.append(", Subject=").append(makeSqlStrReplace(gmmsUtility.modifybackslash(msg.getSubject(),
		// 1)));
		sb.append(",").append("TextContent=").append(
				makeSqlStrReplace(gmmsUtility.modifybackslash(
						replaceBreakLine(msg.getTextContent()), 1)));
		// sb.append(",").append("Owner_A2P=").append(makeSqlStr(gmmsUtility.getServiceIP()));
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

	private String replaceBreakLine(String originalString) {
		if (originalString == null || originalString.length() == 0)
			return originalString;

		String tmp = originalString.replaceAll("\r\n", " ");

		return tmp.replaceAll("\n", " ");
	}

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

	private String formatMySqlDate(java.util.Date date) {
		if (date == null) {
			return null;
		}
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		return "'" + formatter.format(date) + "'";
	}

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

	private synchronized boolean write(String content) {
		boolean result = false;
		try {
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

	public void close() {
		outStream.close();
		isOpen = false;
	}
}
