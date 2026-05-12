package com.king.gmms.protocol.smpp;

import java.text.SimpleDateFormat;
import java.util.*;

import com.king.db.DataManagerException;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.*;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.processor.CsmUtility;
import com.king.gmms.protocol.smpp.pdu.*;
import com.king.gmms.protocol.smpp.pdu.tlv.TLV;
import com.king.gmms.protocol.smpp.pdu.tlv.TLVException;
import com.king.gmms.protocol.smpp.pdu.tlv.TLVOctets;
import com.king.gmms.protocol.smpp.util.Data;
import com.king.gmms.protocol.smpp.util.SmppByteBuffer;
import com.king.gmms.protocol.smpp.util.TimeFormatter;
import com.king.gmms.protocol.smpp.version.*;
import com.king.gmms.routing.*;
import com.king.gmms.util.charset.Convert;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageIdGenerator;

/**
 * <p>
 * Title: Smpp
 * </p>
 * <p>
 * Description: This class provides some shared methods for smpp client and
 * server
 * </p>
 * <p>
 * Copyright: Copyright (c) 2004
 * </p>
 * <p>
 * Company: King
 * </p>
 * 
 * @author Jesse Duan
 * @version 6.1
 */
public class Smpp {
	private static SystemLogger log = SystemLogger.getSystemLogger(Smpp.class);
	private GmmsUtility gmmsUtility = null;
	private A2PCustomerManager ctm = null;
	private TimeZone local;
	private String serviceType = null;
	private byte sourceAddrTon = 0x01;
	private byte sourceAddrNpi = 0x01;
	private byte destAddrTon = 0x01;
	private byte destAddrNpi = 0x01;
	private byte priorityFlag = 0x01;
	private byte replaceIfFlag = 0x00;
	private byte protocolId = 0x00;
	private String connectionID = null;
	private boolean initial = false;
	private IOSMSOperatorRouter smsOperatorRouter = new IOSMSOperatorRouter(
			false);
	private int moduleIndex = 0;
	private short smppServiceTypeIDTag = 0;

	public Smpp() {
		this.local = TimeZone.getDefault();
		this.gmmsUtility = GmmsUtility.getInstance();
		this.ctm = gmmsUtility.getCustomerManager();
		String moduleName = System.getProperty("module");
		if (moduleName != null) {
			moduleIndex = ModuleManager.getInstance()
					.getModuleIndex(moduleName);
		}
		smppServiceTypeIDTag = gmmsUtility.getSmppServiceTypeIDTag();
	}

	/**
	 * change Submit message to GmmsMessage
	 * 
	 * @param msg
	 *            SubmitSM
	 * @param msgId
	 *            String
	 * @param sourceSystemId
	 *            String
	 * @param cst
	 *            GmmsCustomer
	 * @param transaction
	 *            TransactionURI
	 * @return GmmsMessage
	 */
	@SuppressWarnings("deprecation")
	public GmmsMessage createGmmsMessage(SubmitSM msg, String msgId,
			A2PCustomerInfo cst, TransactionURI transaction) {
		GmmsMessage gmmsMsg = new GmmsMessage();
		gmmsMsg.setTransaction(transaction);
		gmmsMsg.setConnectionID(connectionID);
		gmmsMsg.setMsgID(MessageIdGenerator.generateCommonMsgID(cst.getSSID(),
				moduleIndex));

		gmmsMsg.setPriority(msg.getPriorityFlag());

		gmmsMsg.setSenderAddrType(String.valueOf(msg.getSourceAddr().getNpi()));
		gmmsMsg.setRecipientAddrType(String.valueOf(msg.getDestAddr().getNpi()));
		gmmsMsg.setSenderAddrTon(String.valueOf(msg.getSourceAddr().getTon()));
		gmmsMsg.setRecipientAddrTon(String.valueOf(msg.getDestAddr().getTon()));
		
		
		gmmsMsg.setProtocolVersion(String.valueOf(msg.getProtocolId()));

		parseEsmClass(cst, msg, gmmsMsg, msgId);

		if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(gmmsMsg
				.getMessageType())) {
			gmmsMsg.setRSsID(cst.getSSID());
			gmmsMsg.setOutTransID(String.valueOf(msg.getSequenceNumber()));
			if (cst.isDrSwapAddr()) {
				gmmsMsg.setSenderAddress(msg.getDestAddr().getAddress());
				gmmsMsg.setRecipientAddress(msg.getSourceAddr().getAddress());
			} else {
				gmmsMsg.setSenderAddress(msg.getSourceAddr().getAddress());
				gmmsMsg.setRecipientAddress(msg.getDestAddr().getAddress());
			}
		} else {
			gmmsMsg.setOSsID(cst.getSSID());
			gmmsMsg.setInMsgID(msgId);
			gmmsMsg.setInTransID(String.valueOf(msg.getSequenceNumber()));

			gmmsMsg.setSenderAddress(msg.getSourceAddr().getAddress());
			gmmsMsg.setRecipientAddress(msg.getDestAddr().getAddress());

			int drMode = ctm.getDRMode(cst.getSSID());
			switch (drMode) {
			case 0:
				gmmsMsg.setDeliveryReport(false);
				break;
			case 1:
				gmmsMsg.setDeliveryReport(true);
				break;
			default:
				gmmsMsg.setDeliveryReport(msg.getRegisteredDelivery() != 0);
			}

			// set timestamp and transfer local timestamp by timestamp
			long now = new Date().getTime();
			long diff = local.getRawOffset();
			if (local.inDaylightTime(new Date(now))) {
				diff += local.getDSTSavings();
			}
			long gmtNow = now - diff;
			gmmsMsg.setTimeStamp(new Date(gmtNow));
			
			// ##### set expire date begin #########
			// get customer validity period
			if (cst.isParseValidityPeriod()) {
		    	if (log.isTraceEnabled()) {
		    		log.trace(gmmsMsg, "validityPeriod is {}", msg.getValidityPeriod());
		    	}
				Date validityPeriod = null;
				try {
					// parse
					validityPeriod = TimeFormatter.parse(msg.getValidityPeriod(), local);
					if (validityPeriod != null) {
						// check
						if (gmmsUtility.checkExpiryDateFromCust(validityPeriod)) {
							gmmsMsg.setExpiryDate(new Date(validityPeriod.getTime()));
							
						} else {
							gmmsMsg.setExpiryDate(new Date(validityPeriod.getTime()));
							if (log.isInfoEnabled()) {
								log.info(gmmsMsg, "CheckExpiryDateFromCust failed: ({})", validityPeriod.toGMTString());
							}
						}
					}
				} catch (Exception e) {
					if (log.isInfoEnabled()) {
						log.info(gmmsMsg, "parse validityPeriod failed ({})", msg.getValidityPeriod());
					}
				}
			}
			
			if (gmmsMsg.getExpiryDate() == null) {
				// generate from customer conf
				if (cst.getExpireTime() > 0) {
					int expireTime = cst.getExpireTime();
					gmmsMsg.setExpiryDate(new Date(gmtNow + expireTime * 60 * 1000));
				} else if (cst.getFinalExpireTime() > 0) {
					int expireTime = cst.getFinalExpireTime() * 3 / 4;
					gmmsMsg.setExpiryDate(new Date(gmtNow + expireTime * 60 * 1000));
				}
			}
			// ##### set expiry date end #########
			
			// set ScheduleDeliveryTime
			if (cst.isParseScheduleDeliveryTime()) {
				if (log.isTraceEnabled()) {
		    		log.trace(gmmsMsg, "ScheduleDeliveryTime is {}", msg.getScheduleDeliveryTime());
		    	}
				Date scheduleDate = null;
				try {
					// parse
					scheduleDate = TimeFormatter.parse(msg.getScheduleDeliveryTime(), local);
					if (scheduleDate != null) {
						// check
						if (gmmsUtility.checkScheduleDeliveryTimeFromCust(scheduleDate)) {
							gmmsMsg.setScheduleDeliveryTime(new Date(scheduleDate.getTime() - diff));
							
						} else {
							if (log.isInfoEnabled()) {
								log.info(gmmsMsg, "checkScheduleDeliveryTimeFromCust failed: ({})", scheduleDate.toGMTString());
							}
							gmmsMsg.setStatus(GmmsStatus.INVALID_SCHEDULED_TIME);
							return gmmsMsg;
						}
					}
				} catch (Exception e) {
					if (log.isInfoEnabled()) {
						log.info(gmmsMsg, "parse scheduleDeliveryTime failed: ({})", msg.getScheduleDeliveryTime());
					}
					gmmsMsg.setStatus(GmmsStatus.INVALID_SCHEDULED_TIME);
					return gmmsMsg;
				}
			}
			
			try {
				if (msg.hasSarMsgRefNum() && msg.hasSarSegmentSeqnum()
						&& msg.hasSarTotalSegments()) {
					gmmsMsg.setSarMsgRefNum(CsmUtility.short2Hex(msg
							.getSarMsgRefNum()));
					gmmsMsg.setSarSegmentSeqNum(msg.getSarSegmentSeqnum());
					gmmsMsg.setSarTotalSegments(msg.getSarTotalSegments());
				}
			} catch (Exception exp) {
				log.info("Parse CSMS SarMsg parameters error", exp);
			}
			
			// process serviceTypeID
			if (cst.isParseServiceTypeID()) {
				try {
					TLVOctets tlv = (TLVOctets) msg.getExtraOptional(smppServiceTypeIDTag);
					if (tlv != null) {
						int length = tlv.getLength();
						String serviceTypeID = tlv.getValue().removeString(length, GmmsMessage.AIC_CS_ASCII);
						gmmsMsg.setServiceTypeID(Integer.valueOf(serviceTypeID));
					}
				} catch (Exception e) {
					log.info(gmmsMsg, "Parse ServiceTypeID error", e);
					gmmsMsg.setStatus(GmmsStatus.INVALID_SERVICETYPEID);
					return gmmsMsg;
				}
			}

			//handleTransparency(cst, msg, gmmsMsg);
		}
		return gmmsMsg;
	}

	/**
	 * translate deliver message to GmmsMessage
	 * 
	 * @param msg
	 *            DeliverSM
	 * @param msgId
	 *            String
	 * @param systemId
	 *            String
	 * @param cst
	 *            String
	 * @param transaction
	 *            TransactionURI
	 * @return GmmsMessage
	 * @throws Exception
	 */
	public GmmsMessage createGmmsMessage(DeliverSM msg, String msgId,
			String systemId, A2PCustomerInfo cst, TransactionURI transaction)
			throws Exception {
		GmmsMessage gmmsMsg = new GmmsMessage();
		gmmsMsg.setTransaction(transaction);
		gmmsMsg.setConnectionID(connectionID);
		gmmsMsg.setGmmsMsgType(GmmsMessage.AIC_MSG_TYPE_TEXT);
		gmmsMsg.setMsgID(MessageIdGenerator.generateCommonMsgID(cst.getSSID(),
				moduleIndex));
		gmmsMsg.setSenderAddrType(String.valueOf(msg.getSourceAddr().getNpi()));
		gmmsMsg.setRecipientAddrType(String.valueOf(msg.getDestAddr().getNpi()));
		gmmsMsg.setSenderAddrTon(String.valueOf(msg.getSourceAddr().getTon()));
		gmmsMsg.setRecipientAddrTon(String.valueOf(msg.getDestAddr().getTon()));
		
		
		gmmsMsg.setProtocolVersion(String.valueOf(msg.getProtocolId()));
		gmmsMsg.setPriority(msg.getPriorityFlag());

		// parse the esm_class
		parseEsmClass(cst, msg, gmmsMsg, msgId);

		if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(gmmsMsg
				.getMessageType())) {
			gmmsMsg.setRSsID(cst.getSSID());
			gmmsMsg.setOutTransID(String.valueOf(msg.getSequenceNumber()));
			if (cst.isDrSwapAddr()) {
				gmmsMsg.setSenderAddress(msg.getDestAddr().getAddress());
				gmmsMsg.setRecipientAddress(msg.getSourceAddr().getAddress());
			} else {
				gmmsMsg.setSenderAddress(msg.getSourceAddr().getAddress());
				gmmsMsg.setRecipientAddress(msg.getDestAddr().getAddress());
			}
		} else {
			gmmsMsg.setOSsID(cst.getSSID());
			gmmsMsg.setInMsgID(msgId);
			gmmsMsg.setInTransID(String.valueOf(msg.getSequenceNumber()));

			gmmsMsg.setSenderAddress(msg.getSourceAddr().getAddress());
			gmmsMsg.setRecipientAddress(msg.getDestAddr().getAddress());

			// set timestamp and local timestamp
			long now = new Date().getTime();
			long diff = local.getRawOffset();
			if (local.inDaylightTime(new Date(now))) {
				diff += local.getDSTSavings();
			}
			long gmtNow = now - diff;
			gmmsMsg.setTimeStamp(new Date(gmtNow));
			if (cst.getExpireTime() > 0) {
				int expireTime = cst.getExpireTime();
				gmmsMsg
						.setExpiryDate(new Date(gmtNow + expireTime * 60 * 1000));
			} else if (cst.getFinalExpireTime() > 0) {
				int expireTime = cst.getFinalExpireTime() * 3 / 4;
				gmmsMsg
						.setExpiryDate(new Date(gmtNow + expireTime * 60 * 1000));
			}
			int drMode = ctm.getDRMode(cst.getSSID());
			switch (drMode) {
			case 0:
				gmmsMsg.setDeliveryReport(false);
				break;
			case 1:
				gmmsMsg.setDeliveryReport(true);
				break;
			default:
				gmmsMsg.setDeliveryReport(msg.getRegisteredDelivery() != 0);
			}

			try {
				if (msg.hasSarMsgRefNum() && msg.hasSarSegmentSeqnum()
						&& msg.hasSarTotalSegments()) {
					gmmsMsg.setSarMsgRefNum(CsmUtility.short2Hex(msg
							.getSarMsgRefNum()));
					gmmsMsg.setSarSegmentSeqNum(msg.getSarSegmentSeqnum());
					gmmsMsg.setSarTotalSegments(msg.getSarTotalSegments());
				}
			} catch (Exception exp) {
				log.info("Parse CSMS SarMsg parameters error", exp);
			}
			
			// process serviceTypeID
			if (cst.isParseServiceTypeID()) {
				try {
					TLVOctets tlv = (TLVOctets) msg.getExtraOptional(smppServiceTypeIDTag);
					if (tlv != null) {
						int length = tlv.getLength();
						String serviceTypeID = tlv.getValue().removeString(length, GmmsMessage.AIC_CS_ASCII);
						gmmsMsg.setServiceTypeID(Integer.valueOf(serviceTypeID));
					}
				} catch (Exception e) {
					log.info(gmmsMsg, "Parse ServiceTypeID error", e);
					gmmsMsg.setStatus(GmmsStatus.INVALID_SERVICETYPEID);
					return gmmsMsg;
				}
			}

			//handleTransparency(cst, msg, gmmsMsg);
		}

		return gmmsMsg;
	}

	/**
	 * translate GmmsMessage to submit message
	 * 
	 * @param gmmsMsg
	 *            GmmsMessage
	 * @param cst
	 *            String
	 * @return SubmitSM
	 */
	public SubmitSM createSmppSubmitSM(GmmsMessage gmmsMsg, A2PCustomerInfo cst) {
		byte esmClass = 0x00;
		SubmitSM request = null;
		String cstNameShort = null;
		if (cst != null && cst.getShortName() != null) {
			cstNameShort = cst.getShortName();
		}
		try {
			request = new SubmitSM();
			String[] addr = addPrefixForCustomer(gmmsMsg, cst);

			String sender = gmmsMsg.getSenderAddress();
			String recipient = gmmsMsg.getRecipientAddress();
			if (addr != null && addr.length > 0) {
				sender = addr[0];
				recipient = addr[1];
			}

			request.setServiceType(serviceType);
			//add option msg type parameter by king 2024.07.16
			if(cst.getSMSOptionHttpCustomParameter()!=null && !"".equalsIgnoreCase(cst.getSMSOptionHttpCustomParameter())) {
				byte[] msgTypeValue = cst.getSMSOptionHttpCustomParameter().getBytes();
				SmppByteBuffer msgTypeByte = new SmppByteBuffer(msgTypeValue);
				request.setMessageType(msgTypeByte);
			}
			

			Address sourceAddress = initSourceAddress(sender, gmmsMsg);
			request.setSourceAddr(sourceAddress);
			Address destAddress = initRecipicentAddress(recipient, gmmsMsg);
			request.setDestAddr(destAddress);

			SmppByteBuffer byteBuffer = createCallBack(cstNameShort, gmmsMsg
					.getSenderAddress(), sourceAddress);
			if (byteBuffer != null && byteBuffer.getBuffer().length > 0) {
				request.setCallbackNum(byteBuffer);
			}

			// replace (default), replaceIfFlag = 0
			request.setReplaceIfPresentFlag(replaceIfFlag); // configurable
			
			// set validityPeriod
			if (cst.isTransferValidityPeriod()) {
				Date expireDate = gmmsMsg.getExpiryDate();
				if (expireDate == null) {
					Date now = new Date();
					long diff = local.getRawOffset();
					if (local.inDaylightTime(now)) {
						diff += local.getDSTSavings();
					}
					long gmtNow = now.getTime() - diff;

					int defaultExpire = gmmsUtility.getExpireTimeInMinute();
					expireDate = new Date(gmtNow + defaultExpire * 60 * 1000);
				}
				String validityPeriod = TimeFormatter.toAbsoluteFormat(expireDate);
				if(cst.isRelateValidityPeriod()) {
					long relateTime = expireDate.getTime()-System.currentTimeMillis();
					long ss = (relateTime/1000)%60;
					long min = (relateTime/1000/60)%60;
					long hour = (relateTime/1000/60/60)%24;
					String sss = (""+ss).length()==1?("0"+ss):""+ss;
					String mins = (""+min).length()==1?("0"+min):""+min;
					String hours = (""+hour).length()==1?("0"+hour):""+hour;
					validityPeriod = "000000"+hours+mins+sss+"000R";
				}
				if (validityPeriod != null) {
					if (log.isTraceEnabled()) {
						log.trace(gmmsMsg, "validityPeriod format to: {}", validityPeriod);
					}
					request.setValidityPeriod(validityPeriod);
				}
			}

			byte[] udh = null;
			if (gmmsMsg.getSarTotalSeqments() > 1) {
				if (!cst.isUdhConcatenated()) {// Mode is 3 optional parameters
					try {
						short pduRefNum = Short.parseShort(gmmsMsg
								.getSarMsgRefNum(), 16);
						request.setSarMsgRefNum(pduRefNum);
					} catch (Exception e) {
						request.setSarMsgRefNum((short) 1);
					}
					request.setSarSegmentSeqnum((short) gmmsMsg
							.getSarSegmentSeqNum());
					request.setSarTotalSegments((short) gmmsMsg
							.getSarTotalSeqments());

				}
			}

			if (gmmsMsg.getUdh() != null && gmmsMsg.getUdh().length > 0) {
				if(log.isDebugEnabled()){
					log.debug(gmmsMsg, "Set esmClass to 0x40 as UDH size is: "
						+ gmmsMsg.getUdh().length);
				}
				esmClass = (byte) 0x40;
				udh = gmmsMsg.getUdh();
			}

			// when set the short message, the sm_length will be set.
			String contentType = gmmsMsg.getContentType().trim();
			String textContent = null;
			if (gmmsMsg.getTextContent() != null) {
				textContent = gmmsMsg.getTextContent();
			} else if (udh == null) {
				textContent = " ";
			}

			if (GmmsMessage.AIC_MSG_TYPE_BINARY.equalsIgnoreCase(gmmsMsg
					.getGmmsMsgType())) {
				// add by bruce for support binary message has UDH
				if (udh != null) {
					byte[] content = gmmsMsg.getMimeMultiPartData();
					byte[] byteData = new byte[udh.length + content.length];
					System.arraycopy(udh, 0, byteData, 0, udh.length);
					System.arraycopy(content, 0, byteData, udh.length,
							content.length);
					request.setBinShortMessage(byteData);
				} else {
					request.setBinShortMessage(gmmsMsg.getMimeMultiPartData());
				}
				contentType = GmmsMessage.AIC_MSG_TYPE_BINARY;
			} else {
				if (contentType == null
						|| "SMSCdefaultCharset".equalsIgnoreCase(contentType)) {
					contentType = GmmsMessage.AIC_CS_ASCII;
				}

				byte[] content = null;
				if (udh != null) {
					if (textContent == null) {
						request.setBinShortMessage(udh);
					} else {
						if(gmmsMsg.isGsm7bit()){
							if(cst.getSmsOptionOutgoingGSM7bit()==1){
								content = Convert.convert2GSM(textContent);
							}else if(cst.getSmsOptionOutgoingGSM7bit()==2){
								 int udhLen = udh.length;
								 int fillBits = (7 - (udhLen)*8%7)%7;								 
								content = Convert.encode7bit(Convert.convert2GSM(textContent),fillBits);
							}else{
								content = textContent.getBytes(contentType);
							}
						}else{
							content = textContent.getBytes(contentType);
						}
						
						byte[] byteData = new byte[udh.length + content.length];
						System.arraycopy(udh, 0, byteData, 0, udh.length);
						System.arraycopy(content, 0, byteData, udh.length, content.length);						
						request.setBinShortMessage(byteData);
					}
				} else {
					if(gmmsMsg.isGsm7bit()){
						if(cst.getSmsOptionOutgoingGSM7bit()==1){
							content = Convert.convert2GSM(textContent);
						}else if(cst.getSmsOptionOutgoingGSM7bit()==2){							 
							content = Convert.encode7bit(Convert.convert2GSM(textContent),0);
						}else{
							content = textContent.getBytes(contentType);
						}
					}else{
						content = textContent.getBytes(contentType);
					}
					request.setBinShortMessage(content);
				}				
			}

			// the default esmClass is 00000000
			request.setEsmClass(esmClass);
			if (esmClass != 0) {
				if(log.isDebugEnabled()){
	        		log.debug(gmmsMsg, "esmClass is {} , set it in SubmitSM." , esmClass);
				}
			}

			request.setProtocolId(protocolId);
			if ("Iusacell_MX".equalsIgnoreCase(cstNameShort)
					|| "APT_TW".equalsIgnoreCase(cstNameShort)) {
				request.setPriorityFlag(Byte.parseByte("0"));
			} else if ("SMT_HK".equalsIgnoreCase(cst.getShortName())) {
				request.setPriorityFlag((byte) 0);
			} else {
				if (gmmsMsg.getPriority() != -1) {
					request.setPriorityFlag(Byte.parseByte(String
							.valueOf(gmmsMsg.getPriority())));
				} else { // Level 1 priority is normal
					request.setPriorityFlag(priorityFlag);
				}
			}

			// modified by Neal on 08.03.25
			byte supportDR = gmmsUtility.getCustomerManager()
					.isRssidNotSupportDR(gmmsMsg.getRSsID()) ? (byte) 0x00
					: (byte) 0x01;
			request.setRegisteredDelivery(supportDR);

			// Due to the contentType of those messages which come from CP,
			// we need to judge whether the contentType is defined in SMPP3.4
			// spec.

			// SMSC Default Alphabet is 0.
			if((cst.getSmsOptionOutgoingGSM7bit()==0)
					||gmmsMsg.isGsm7bit()) {
				contentType = "SMSCdefaultCharset";
			}
			String specialDcs = gmmsMsg.getSpecialDataCodingScheme();//todo
			if (cst.isSupportDCS() && specialDcs != null
					&& !"".equals(specialDcs)) {// added by Jianming in v1.0.1
				byte coding = 0;
				int ct = 0;
				try {
					coding = hexToByte(specialDcs);
					if("SMSCdefaultCharset".equalsIgnoreCase(contentType)){
						ct = coding & 0xf3;
					}else if(GmmsMessage.AIC_MSG_TYPE_BINARY.equalsIgnoreCase(contentType)){
						ct = (coding & 0xf3)+0x04;
					}else{
						ct  = coding;
					}
				} catch (Exception e) {
					log.error("specialDataCodingScheme is an invalid value:"
							+ specialDcs, e);
				}
				coding = (byte)(ct & 0xff);
				request.setDataCoding(coding);
			} else {
				byte coding = getDataCoding(contentType, cstNameShort);
				request.setDataCoding(coding);
			}

		} catch (Exception e) {
			log.error(
					"Occur error when create SubmitSM smpp message, inMsgid = "
							+ gmmsMsg.getInMsgID() + ", sender = "
							+ gmmsMsg.getSenderAddress() + ", recipient = "
							+ gmmsMsg.getRecipientAddress() + ", content = "
							+ gmmsMsg.getTextContent(), e);
			gmmsMsg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);// added in A2P
																// v1.1.2
																// ,Jianming
			return null;
		}
		return request;
	}

	/**
	 * create Submit response from GmmsMessage
	 * 
	 * @param gmmsMsg
	 * @param cst
	 * @return
	 */
	public SubmitSMResp createSmppSubmitSMResp(GmmsMessage gmmsMsg,
			A2PCustomerInfo cst) {
		SubmitSMResp submitResponse = new SubmitSMResp();
		try {
			submitResponse.setSequenceNumber(Integer.parseInt(gmmsMsg
					.getInTransID()));
			submitResponse.setMessageId(gmmsMsg.getInMsgID());
			submitResponse.setCommandStatus(SMPPRespStatus
					.getSmppStatus(gmmsMsg.getStatusCode()));
			if (gmmsMsg.getStatusCode() < 0
					&& ctm.getTransparency(cst.getSSID()) > 0) {
				String rMncMccS = gmmsMsg.getRMncMcc();
				String[] rMncMcc = null;
				if (rMncMccS == null) {
					rMncMcc = ctm.getMncMccbySsid(gmmsMsg.getRoperator());//mnc,mcc
				} else {
					if (rMncMccS.length() == 6) {
						rMncMcc = new String[2];
						rMncMcc[0] = rMncMccS.substring(0, 3);//mnc
						rMncMcc[1] = rMncMccS.substring(3);//mcc
					}
				}
				
				if (rMncMcc != null) {
					int rMCCMNCLength = 5;					
					A2PCustomerInfo rop = ctm.getCustomerBySSID(gmmsMsg.getRoperator());
					if(rop != null){
						rMCCMNCLength = rop.getMCCMNCLength();
					}
					String mnc = rMncMcc[0];
					if(rMCCMNCLength == 5 && mnc.startsWith("0")){
						 mnc = mnc.substring(1, 3) + "0";
					}
					SmppByteBuffer bb = new SmppByteBuffer();
					short tag = (short) 0x0203;
					bb.appendByte((byte) 0xA0);
					bb.appendString(rMncMcc[1]);
					bb.appendString(mnc);
					submitResponse.setExtraOptional(tag, bb);
				}
			}
		} catch (WrongLengthOfStringException e) {
			log.warn(gmmsMsg, "MsgIdCreator created an invalid ID ({}) for SMPP SubmitResp.",gmmsMsg.getInMsgID(), e);
			submitResponse.setCommandStatus(Data.ESME_RX_T_APPN); // temp
																	// application
																	// error
			gmmsMsg.setStatus(GmmsStatus.INSUBMIT_RESP_FAILED);
		} catch (Exception e) {
			log.warn(gmmsMsg,"when create gmms message with SubmitSM occur error, exception is{}", e);
			submitResponse.setCommandStatus(Data.ESME_RX_T_APPN); // temp
																	// application
																	// error
			gmmsMsg.setStatus(GmmsStatus.INSUBMIT_RESP_FAILED);
		}
		return submitResponse;
	}

	/**
	 * create Submit response from GmmsMessage
	 * 
	 * @param gmmsMsg
	 * @param cst
	 * @return
	 */
	public SubmitSMResp createSmppSubmitSMResp4dr(GmmsMessage gmmsMsg,
			int respCode) {
		SubmitSMResp submitResponse = new SubmitSMResp();
		try {
			submitResponse.setSequenceNumber(Integer.parseInt(gmmsMsg
					.getOutTransID()));
			submitResponse.setMessageId(gmmsMsg.getOutMsgID());
			submitResponse.setCommandStatus(respCode);
		} catch (WrongLengthOfStringException e) {
			log.warn(gmmsMsg, "MsgIdCreator created an invalid ID ({}) for SMPP SubmitResp.", gmmsMsg.getMsgID(),e);
			submitResponse.setCommandStatus(Data.ESME_RX_T_APPN); // temp
																	// application
																	// error
			gmmsMsg.setStatus(GmmsStatus.SERVER_ERROR);
		} catch (Exception e) {
			log.warn(gmmsMsg, "when create gmms message with SubmitSM occur error, exceptions is {}", e);
		}
		return submitResponse;
	}

	/**
	 * create Submit response from GmmsMessage
	 * 
	 * @param gmmsMsg
	 * @param cst
	 * @return
	 */
	public DeliverSMResp createSmppDeliverSMResp(GmmsMessage gmmsMsg,
			A2PCustomerInfo cst) {
		DeliverSMResp deliverResponse = new DeliverSMResp();
		try {
			deliverResponse.setSequenceNumber(Integer.parseInt(gmmsMsg
					.getInTransID()));
			deliverResponse.setMessageId(gmmsMsg.getInMsgID());
			deliverResponse.setCommandStatus(SMPPRespStatus
					.getSmppStatus(gmmsMsg.getStatusCode()));
			if (gmmsMsg.getStatusCode() < 0
					&& ctm.getTransparency(cst.getSSID()) > 0) {
				String rMncMccS = gmmsMsg.getRMncMcc();
				String[] rMncMcc = null;
				if (rMncMccS == null) {
					rMncMcc = ctm.getMncMccbySsid(gmmsMsg.getRoperator());
				} else {
					if (rMncMccS.length() == 6) {
						rMncMcc = new String[2];
						rMncMcc[1] = rMncMccS.substring(0, 3);
						rMncMcc[0] = rMncMccS.substring(3);
					}
				}

				if (rMncMcc != null) {
					SmppByteBuffer bb = new SmppByteBuffer();
					short tag = (short) 0x0203;
					bb.appendByte((byte) 0xA0);
					bb.appendString(rMncMcc[1]);
					bb.appendString(rMncMcc[0]);
					deliverResponse.setExtraOptional(tag, bb);
				}
			}

		} catch (WrongLengthOfStringException e) {
			log.warn(gmmsMsg, "MsgIdCreator created an invalid ID ({}) for SMPP SubmitResp.",gmmsMsg.getInMsgID(), e);
			deliverResponse.setCommandStatus(Data.ESME_RX_T_APPN); // temp
																	// application
																	// error
			gmmsMsg.setStatus(GmmsStatus.SERVER_ERROR);
		} catch (Exception e) {
			log.warn(gmmsMsg, "when create gmms message with SubmitSM occur error", e);
		}
		return deliverResponse;
	}

	public DeliverSMResp createSmppDeliverSMResp4dr(GmmsMessage gmmsMsg,
			int respCode) {
		DeliverSMResp deliverResponse = new DeliverSMResp();
		try {
			deliverResponse.setSequenceNumber(Integer.parseInt(gmmsMsg
					.getOutTransID()));
			deliverResponse.setMessageId(gmmsMsg.getOutMsgID());
			deliverResponse.setCommandStatus(respCode);
		} catch (WrongLengthOfStringException e) {
			log.warn(gmmsMsg, "MsgIdCreator created an invalid ID ({}) for SMPP SubmitResp.",gmmsMsg.getOutMsgID(), e);
			deliverResponse.setCommandStatus(Data.ESME_RX_T_APPN); // temp
																	// application
																	// error
			gmmsMsg.setStatus(GmmsStatus.SERVER_ERROR);
		} catch (Exception e) {
			log.warn(gmmsMsg,"when create gmms message with SubmitSM occur error", e);
		}
		return deliverResponse;
	}

	/**
	 * Modified by Neal, to implement WebEx 2-way message. 2005.03.30 create
	 * normal deliverSM pdu
	 * 
	 * @param gmmsMsg
	 *            GmmsMessage
	 * @param authedCustms
	 *            GmmsCustomer
	 * @return DeliverSM
	 */
	public DeliverSM createSmppDeliverSM(GmmsMessage gmmsMsg,
			A2PCustomerInfo authedCustms) {
		String cstNameShort = authedCustms.getShortName();
		DeliverSM request = null;
		try {
			request = new DeliverSM();
			byte esmClass = 0x00;

			String[] addr = addPrefixForCustomer(gmmsMsg, authedCustms);

			String sender = gmmsMsg.getSenderAddress();
			String recipient = gmmsMsg.getRecipientAddress();
			if (addr != null && addr.length > 0) {
				sender = addr[0];
				recipient = addr[1];
			}

			request.setServiceType(serviceType);

			if ("STelecom_VN".equalsIgnoreCase(cstNameShort)) {
				byte[] senderAddr = sender.getBytes();
				byte[] header = { 1, 1, 1 };
				byte[] callBackNum = new byte[header.length + senderAddr.length];
				callBackNum[0] = 1;
				callBackNum[1] = 1;
				callBackNum[2] = 1;
				for (int i = 0; i < senderAddr.length; i++) {
					callBackNum[i + 3] = senderAddr[i];
				}

				SmppByteBuffer byteBuffer = new SmppByteBuffer(callBackNum);
				request.setCallbackNum(byteBuffer);
			}

			Address sourceAddress = initSourceAddress(sender, gmmsMsg);
			request.setSourceAddr(sourceAddress);

			Address destAddress = initRecipicentAddress(recipient, gmmsMsg);
			request.setDestAddr(destAddress);
			request.setProtocolId(protocolId);

			if (gmmsMsg.getPriority() != -1) {
				request.setPriorityFlag(Byte.parseByte(String.valueOf(gmmsMsg
						.getPriority())));
			} else { // Level 1 priority is normal
				request.setPriorityFlag(priorityFlag);
			}

			// modified by Neal on 08.03.25
			byte supportDR = gmmsUtility.getCustomerManager()
					.isRssidNotSupportDR(gmmsMsg.getRSsID()) ? (byte) 0x00
					: (byte) 0x01;
			request.setRegisteredDelivery(supportDR);

			byte[] udh = null;
			if (gmmsMsg.getSarTotalSeqments() > 1) {
				if (!authedCustms.isUdhConcatenated()) {// Mode is 3 optional
														// parameters
					try {
						short pduRefNum = Short.parseShort(gmmsMsg
								.getSarMsgRefNum(), 16);
						request.setSarMsgRefNum(pduRefNum);
					} catch (Exception e) {
						request.setSarMsgRefNum((short) 1);
					}

					request.setSarSegmentSeqnum((short) gmmsMsg
							.getSarSegmentSeqNum());
					request.setSarTotalSegments((short) gmmsMsg
							.getSarTotalSeqments());
				}
			}

			if (gmmsMsg.getUdh() != null && gmmsMsg.getUdh().length > 0) {
				esmClass = (byte) 0x40;
				udh = gmmsMsg.getUdh();
			}

			// when set the short message, the sm_length will be set.
			String contentType = gmmsMsg.getContentType().trim();
			String textContent = null;
			if (gmmsMsg.getTextContent() != null) {
				textContent = gmmsMsg.getTextContent();
			} else if (udh == null) {
				textContent = " ";
			}

			if (GmmsMessage.AIC_MSG_TYPE_BINARY.equalsIgnoreCase(gmmsMsg
					.getGmmsMsgType())) {
				// add by bruce for support binary message has UDH
				if (udh != null) {
					byte[] content = gmmsMsg.getMimeMultiPartData();
					byte[] byteData = new byte[udh.length + content.length];
					System.arraycopy(udh, 0, byteData, 0, udh.length);
					System.arraycopy(content, 0, byteData, udh.length,
							content.length);
					request.setBinShortMessage(byteData);
				} else {
					request.setBinShortMessage(gmmsMsg.getMimeMultiPartData());
				}
				contentType = GmmsMessage.AIC_MSG_TYPE_BINARY;
			} else {
				if (contentType == null
						|| "SMSCdefaultCharset".equalsIgnoreCase(contentType)) {
					contentType = GmmsMessage.AIC_CS_ASCII;
				}

				byte[] content =null;
				if (udh != null) {
					if (textContent == null) {
						request.setBinShortMessage(udh);
					} else {
						if(gmmsMsg.isGsm7bit()){
							if(authedCustms.getSmsOptionOutgoingGSM7bit()==1){
								content = Convert.convert2GSM(textContent);
							}else if(authedCustms.getSmsOptionOutgoingGSM7bit()==2){
								 int udhLen = udh.length;
								 int fillBits = (7 - (udhLen)*8%7)%7;								 
								content = Convert.encode7bit(Convert.convert2GSM(textContent),fillBits);								
							}else{
								content = textContent.getBytes(contentType);
							}
						}else{
							content = textContent.getBytes(contentType);
						}						
						byte[] byteData = new byte[udh.length + content.length];
						System.arraycopy(udh, 0, byteData, 0, udh.length);
						System.arraycopy(content, 0, byteData, udh.length, content.length);	
						request.setBinShortMessage(byteData);
					}
				} else {
					if(gmmsMsg.isGsm7bit()){
						if(authedCustms.getSmsOptionOutgoingGSM7bit()==1){
							content = Convert.convert2GSM(textContent);
						}else if(authedCustms.getSmsOptionOutgoingGSM7bit()==2){
							content = Convert.encode7bit(Convert.convert2GSM(textContent),0);
						}else{
							content = textContent.getBytes(contentType);
						}
					}else{
						content = textContent.getBytes(contentType);
					}
					request.setBinShortMessage(content);
				}
			}

			request.setEsmClass(esmClass);
			if (esmClass != 0) {
				if(log.isDebugEnabled()){
	        		log.debug(gmmsMsg, "esmClass is {}, set it in SubmitSM." , esmClass);
				}
			}
			// added by Jianming in v1.0.1
			// SMSC Default Alphabet is 0.
			if((authedCustms.getSmsOptionOutgoingGSM7bit()==0&&GmmsMessage.AIC_CS_ASCII.equalsIgnoreCase(contentType))
					||gmmsMsg.isGsm7bit()) {
				contentType = "SMSCdefaultCharset";
			}
			String specialDcs = gmmsMsg.getSpecialDataCodingScheme();
			if (authedCustms.isSupportDCS() && specialDcs != null
					&& !"".equals(specialDcs)) {// added by Jianming in v1.0.1
				byte coding = 0;
				int ct = 0;
				try {
					coding = hexToByte(specialDcs);
					if("SMSCdefaultCharset".equalsIgnoreCase(contentType)){
						ct = coding & 0xf3;
					}else if(GmmsMessage.AIC_MSG_TYPE_BINARY.equalsIgnoreCase(contentType)){
						ct = (coding & 0xf3)+0x04;
					}else{
						ct  = coding;
					}
				} catch (Exception e) {
					log.error("specialDataCodingScheme is an invalid value:"
							+ specialDcs, e);
				}
				coding = (byte)(ct & 0xff);
				request.setDataCoding(coding);
			} else {
				byte coding = getDataCoding(contentType, cstNameShort);
				request.setDataCoding(coding);
			}
		} catch (Exception e) {
			log.error(gmmsMsg,
					"Occur error when create DeliverSM smpp message, inMsgid = "
							+ gmmsMsg.getInMsgID() + ", sender = "
							+ gmmsMsg.getSenderAddress() + ", recipient = "
							+ gmmsMsg.getRecipientAddress() + ", content = "
							+ gmmsMsg.getTextContent(), e);
			gmmsMsg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);// added in A2P
																// v1.1.2
																// ,Jianming
			return null;
		}
		return request;
	}

	public SubmitSM createSmppSubmitSM4dr(GmmsMessage gmmsMsg,
			A2PCustomerInfo cst) {
		return createSmppSubmitSM4dr(gmmsMsg, cst, null);
	}

	private String mappingErrorCode4DR(int statusCode, A2PCustomerInfo cst) {
		String errcod = "000";
		if (!cst.isSmppMapErrCod4DR()) {
			return errcod;
		}
		errcod = getErrCodFromStatusCode(statusCode);

		return errcod;
	}

	/**
	 * For send delivery report,create SubmitSM pdu
	 * 
	 * @param gmmsMsg
	 *            GmmsMessage
	 * @param cstNameShort
	 *            String
	 * @return DeliverSM
	 */
	public SubmitSM createSmppSubmitSM4dr(GmmsMessage gmmsMsg,
			A2PCustomerInfo cst, SMPPVersion version) {
		String cstNameShort = cst.getShortName();
		SubmitSM request = null;
		try {
			request = new SubmitSM();
			byte esmClasse = 0x08;
			int stateCode = gmmsMsg.getStatusCode();
			String state = null;
			if (stateCode < 10000) {
				// GMD don't receive the delivery report from next side
				// then send out delivery report initactively
				if (stateCode == 0) {
					state = "DELIVRD";
					gmmsMsg.setStatusCode(10000);
				} else {
					state = "UNDELIV";
					gmmsMsg.setStatusCode(10400);
				}
			} else {
				if (stateCode == GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT
						.getCode()) {
					state = gmmsMsg.getStatusText();
					gmmsMsg.setStatusCode(getCodeFromState(state));
				} else {
					state = getStateTextFromCode(stateCode);
				}
			}

			 // Use GMT Time for done_date of DR PDU				
			Date doneDate = gmmsUtility.getGMTTime(gmmsMsg.getDateIn());
			
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyMMddHHmm");
			String shortMsg = null;
			StringBuffer sb = new StringBuffer();
			sb.append("id:");
			if ("CU_CN".equalsIgnoreCase(cstNameShort)) { // CU
				sb.append(Long.parseLong(gmmsMsg.getInMsgID(), 16));
				sb.append(" submit date:");
				sb.append(dateFormat.format(gmmsMsg.getTimeStamp()));
				sb.append(" done date:");
				sb.append(dateFormat.format(doneDate));
				sb.append(" stat:");
				sb.append(state);
			} else { // not CU
				String verid = null;
				if (version != null) {
					verid = Integer.toString(version.getVersionID());
				} else {
					verid = Integer.toString(cst.getSMPPVersion()
							.getVersionID());
				}

				if (!"52".equals(verid)) {
					String tempMsgId = Long.toString(Long.parseLong(gmmsMsg
							.getInMsgID(), 16));
					if (tempMsgId.length() <= 10) {
						while (tempMsgId.length() < 10) {
							tempMsgId = "0" + tempMsgId;
						}
						sb.append(tempMsgId);
					} else {
						sb.append(tempMsgId.substring(0, 10));
					}
				} else {// V3.4
					String msgId = parseHexMsgId(gmmsMsg.getInMsgID(), cst);
					sb.append(msgId);
				}
				sb.append(" sub:");
				sb.append("001");
				sb.append(" dlvrd:");
				sb.append("001");
				sb.append(" submit date:");
				sb.append(dateFormat.format(gmmsMsg.getTimeStamp()));
				sb.append(" done date:");
				sb.append(dateFormat.format(doneDate));
				sb.append(" stat:");
				sb.append(state);
				sb.append(" err:");
				sb.append(this
						.mappingErrorCode4DR(gmmsMsg.getStatusCode(), cst));

				sb.append(" text:");
				try {
					if (gmmsMsg.getTextContent() == null) {
						sb.append(" ");
					} else if (gmmsMsg.getTextContent().getBytes(
							gmmsMsg.getContentType()).length <= 20) {
						sb.append(gmmsMsg.getTextContent());
					} else {
						sb.append(gmmsMsg.getTextContent().substring(0, 20));
					}
				} catch (Exception e) {
					// log.warn(e,e);
					sb.append(" ");
				}
			}

			shortMsg = sb.toString();
			gmmsMsg.setTextContent(shortMsg);
			request.setEsmClass(esmClasse);
			try {
				request.setShortMessage(shortMsg);
			} catch (Exception e) {
				log.error(
						"when create deliverSM (delivery report) occur error.",
						e);
				return null;
			}

			request.setServiceType(serviceType);

			String sender = gmmsMsg.getSenderAddress();
			String recipient = gmmsMsg.getRecipientAddress();
			String[] addr = addPrefixForCustomer(gmmsMsg, cst);

			if (addr != null && addr.length > 0) {
				sender = addr[0];
				recipient = addr[1];
			}
			Address sourceAddress = initSourceAddress(sender, gmmsMsg);
			Address destAddress = initRecipicentAddress(recipient, gmmsMsg);

			if (cst.isDrSwapAddr()) {
				request.setSourceAddr(destAddress);
				request.setDestAddr(sourceAddress);
			} else {
				request.setSourceAddr(sourceAddress);
				request.setDestAddr(destAddress);
			}
			request.setProtocolId(protocolId);
			request.setPriorityFlag(Byte.parseByte(String.valueOf(gmmsMsg
					.getPriority())));
			request.setRegisteredDelivery(Byte.parseByte("0"));
			request.setDataCoding(Byte.parseByte("0"));
		} catch (Exception e) {
			log.error(
					"Occur error when create DeliverSM4dr smpp message, inMsgid = "
							+ gmmsMsg.getInMsgID() + ", sender = "
							+ gmmsMsg.getSenderAddress() + ", recipient = "
							+ gmmsMsg.getRecipientAddress(), e);
			gmmsMsg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);// added in A2P
																// v1.1.2
																// ,Jianming
			request = null;
		}
		return request;
	}

	/**
	 * For send delivery report,create DeliverSM pdu
	 * 
	 * @param gmmsMsg
	 *            GmmsMessage
	 * @param cstNameShort
	 *            String
	 * @return DeliverSM
	 */

	public DeliverSM createSmppDeliverSM4dr(GmmsMessage gmmsMsg,
			A2PCustomerInfo cst) {
		if (cst == null) {
			return null;
		}
		String cstNameShort = cst.getShortName();

		DeliverSM request = null;
		try {
			request = new DeliverSM();
			byte esmClasse = 0x04;
			int stateCode = gmmsMsg.getStatusCode();
			String state = null;
			if (stateCode < 10000) {
				// GMD don't receive the delivery report from next side
				// then send out delivery report initactively
				if (stateCode == 0) {
					state = "DELIVRD";
					gmmsMsg.setStatusCode(10000);
				} else if (stateCode == 9000) {
					state = "UNKNOWN";
					gmmsMsg.setStatusCode(10900);
				} else {
					state = "UNDELIV";
					gmmsMsg.setStatusCode(10400);
				}
            } else if(stateCode == 10800 ){
                state = "DELETED";                 
			} else if(stateCode == 10300 ){
                state = "UNDELIV";                 
			} else {
				if (stateCode == GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT
						.getCode()) {
					state = gmmsMsg.getStatusText();
					gmmsMsg.setStatusCode(getCodeFromState(state));
				} else {
					state = getStateTextFromCode(stateCode);
				}
			}
			
			//support custom ali not support unknown status feature
			if(cst.isNotSupportUnknowDRStatus()) {
				if("UNKNOWN".equalsIgnoreCase(state) ||
						"DELETED".equalsIgnoreCase(state)) {
					state = "UNDELIV";
				}
			}
			//support dr 
			String showDRStatus = cst.changeDRStatus(state);
			if(showDRStatus!=null && !"".equalsIgnoreCase(showDRStatus)) {
				state = showDRStatus;
			}

			String shortMsg = null;
			/**
			 * modify by brush if configure the item of
			 * SMSOptionDRStatusIsOptionPara then set dr status to option para
			 * else set to short_message
			 */
			if (cst.getDRStatusIsOptionPara()) {
				setDRStatus2OptionPara(request, gmmsMsg);
			} else {
				if (cst.isNeedReceiptedMsgId()) {// added by Jianming
					String tempMsgId = gmmsMsg.getInMsgID();
					if (tempMsgId.length() > 56) {
						tempMsgId = tempMsgId.substring(0, 56);
					}

					request.setReceiptedMessageId(tempMsgId);// set message_id
																// to TLV
				}			
				
				 // Use GMT Time for done_date of DR PDU				
				Date doneDate = gmmsUtility.getGMTTime(gmmsMsg.getDateIn());
				
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyMMddHHmm");

				StringBuffer sb = new StringBuffer();
				sb.append("id:");
				if ("CU_CN".equalsIgnoreCase(cstNameShort)) { // CU
					sb.append(Long.parseLong(gmmsMsg.getInMsgID(), 16));
					sb.append(" submit date:");
					sb.append(dateFormat.format(gmmsMsg.getTimeStamp()));
					sb.append(" done date:");
					sb.append(dateFormat.format(doneDate));
					sb.append(" stat:");
					sb.append(state);
				} else { // not CU
					if (!"52".equals(gmmsMsg.getProtocolVersion())) {
						String tempMsgId = Long.toString(Long.parseLong(gmmsMsg
								.getInMsgID(), 16));
						if (tempMsgId.length() <= 10) {
							while (tempMsgId.length() < 10) {
								tempMsgId = "0" + tempMsgId;
							}
							sb.append(tempMsgId);
						} else {
							sb.append(tempMsgId.substring(0, 10));
						}
					} else {// V3.4
						String msgId = parseHexMsgId(gmmsMsg.getInMsgID(), cst);
						sb.append(msgId);
					}
					String errorcode = this.mappingErrorCode4DR(gmmsMsg.getStatusCode(),
							cst);
					if(cst.isNotSupportUnknowDRStatus()) {
						if("UNDELIV".equalsIgnoreCase(state) &&
								(errorcode.equalsIgnoreCase("004")|| errorcode.equalsIgnoreCase("002"))) {
							errorcode="003";
						}
					}					
					sb.append(" sub:");
					sb.append("001");
					sb.append(" dlvrd:");
					sb.append("001");
					sb.append(" submit date:");
					sb.append(dateFormat.format(gmmsMsg.getTimeStamp()));
					sb.append(" done date:");
					sb.append(dateFormat.format(doneDate));
					sb.append(" stat:");
					sb.append(state);
					sb.append(" err:");
					sb.append(errorcode);

					sb.append(" text:");
					try {
						if (gmmsMsg.getTextContent() == null) {
							sb.append(" ");
						} else if (gmmsMsg.getTextContent().getBytes(
								gmmsMsg.getContentType()).length <= 20) {
							sb.append(gmmsMsg.getTextContent());
						} else {
							sb
									.append(gmmsMsg.getTextContent().substring(
											0, 20));
						}
					} catch (Exception e) {
						sb.append(" ");
					}
				}
				shortMsg = sb.toString();
				gmmsMsg.setTextContent(shortMsg);
				try {
					request.setShortMessage(shortMsg);
				} catch (Exception e) {
					log
							.error(
									"when create deliverSM (delivery report) occur error.",
									e);
					return null;
				}
			}

			request.setEsmClass(esmClasse);
			request.setServiceType(serviceType);

			String sender = gmmsMsg.getSenderAddress();
			String recipient = gmmsMsg.getRecipientAddress();
			String[] addr = addPrefixForCustomer(gmmsMsg, cst);

			if (addr != null && addr.length > 0) {
				sender = addr[0];
				recipient = addr[1];
			}

			Address sourceAddress = initSourceAddress(sender, gmmsMsg, cst);
			Address destAddress = initRecipicentAddress(recipient, gmmsMsg);
			if (cst.isDrSwapAddr()) {
				request.setSourceAddr(destAddress);
				request.setDestAddr(sourceAddress);
			} else {
				request.setSourceAddr(sourceAddress);
				request.setDestAddr(destAddress);
			}
			request.setProtocolId(protocolId);
			request.setPriorityFlag(Byte.parseByte(String.valueOf(gmmsMsg
					.getPriority())));
			request.setRegisteredDelivery(Byte.parseByte("0"));
			request.setDataCoding(Byte.parseByte("0"));
		} catch (Exception e) {
			log.error(
					"Occur error when create DeliverSM4dr smpp message, inMsgid = "
							+ gmmsMsg.getInMsgID() + ", sender = "
							+ gmmsMsg.getSenderAddress() + ", recipient = "
							+ gmmsMsg.getRecipientAddress(), e);
			gmmsMsg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);// added in A2P
																// v1.1.2
																// ,Jianming
			return null;
		}
		return request;
	}

	private void setDRStatus2OptionPara(DeliverSM request, GmmsMessage gmmsMsg) {

		String tempMsgId = gmmsMsg.getInMsgID();
		if (tempMsgId.length() > 56) {
			tempMsgId = tempMsgId.substring(0, 56);
		}

		try {
			request.setReceiptedMessageId(tempMsgId);// set message_id to TLV
			request.setMessageState(getMessageStateFromState(gmmsMsg
					.getStatusCode()));
		} catch (Exception ex) {
			log.warn(gmmsMsg, ex, ex);
		}
	}

	private byte getMessageStateFromState(int state) {
		switch (state) {
		case 10000:
			return 2;
		case 10105:
			return 1;
		case 10200:
			return 3;
		case 10300:
			return 4;
		case 10400:
			return 5;
		case 10500:
			return 8;
		case 10900:
		default:
			return 7;
		}
	}

	/**
	 * handle china unicom's 00+ countrycode + phone# case.
	 * 
	 * @param addr
	 *            GmmsMessage
	 * @param prefix
	 *            String
	 * @return String
	 */
	public String addPrefix(String addr, String prefix) {
		if (prefix == null) {
			return addr;
		}
		if (addr != null) {
			return prefix + addr;
		}
		return addr;
	}

	public SmppByteBuffer createCallBack(String cstNameShort, String sender,
			Address sourceAddress) {
		SmppByteBuffer byteBuffer = null;
		byte[] header = new byte[3];
		byte[] callBackNum;
		byte[] senderAddr;
		if ("STelecom_VN".equalsIgnoreCase(cstNameShort)) {
			header[0] = 0x01;
			header[1] = 0x01;
			header[2] = 0x01;
			senderAddr = sender.getBytes();
			callBackNum = new byte[header.length + senderAddr.length];

			System.arraycopy(header, 0, callBackNum, 0, 3);
			for (int i = 0; i < senderAddr.length; i++) {
				callBackNum[i + 3] = senderAddr[i];
			}
			byteBuffer = new SmppByteBuffer(callBackNum);
		} else if ("APT_TW".equalsIgnoreCase(cstNameShort)) {
			header[0] = 0x01; // 1 indicates that the Call Back Number is sent
								// to the mobile encoded as ASCII digits
			header[1] = sourceAddress.getTon();// The 2nd octet contains the
												// Type of Number (TON).
			header[2] = sourceAddress.getNpi();// The third octet contains the
												// Numbering Plan Indicator
												// (NPI)

			if (sender.length() > 16)
				sender = sender.substring(0, 16);
			senderAddr = sender.getBytes();
			callBackNum = new byte[header.length + senderAddr.length];
			System.arraycopy(header, 0, callBackNum, 0, 3);
			for (int i = 0; i < senderAddr.length; i++) {
				callBackNum[i + 3] = senderAddr[i];
			}
			byteBuffer = new SmppByteBuffer(callBackNum);
		}
		return byteBuffer;
	}

	/**
	 * init source address
	 * 
	 * @param addr
	 *            String
	 * @param addrType
	 *            String
	 */
	private Address initSourceAddress(String addr, GmmsMessage gmmsMsg) {
		Address sourceAddress = new Address();
		try {

			if (gmmsMsg.getMessageType().equalsIgnoreCase(
					GmmsMessage.MSG_TYPE_DELIVERY_REPORT)) {
				String addrType = gmmsMsg.getSenderAddrType();
				if (addrType == null) {
					sourceAddress.setNpi(sourceAddrNpi);
				} else {
					sourceAddress.setNpi(Byte.parseByte(addrType));
				}
				String addrTon = gmmsMsg.getSenderAddrTon();
				if(addrTon == null){
					sourceAddress.setTon(sourceAddrTon);
				} else {
					sourceAddress.setTon(Byte.parseByte(addrTon));
				}
			} else {
				// sender number is Alphanumeric
				if (gmmsUtility.isAlphanumeric(addr)) {
					sourceAddress.setTon((byte)0x05);
					sourceAddress.setNpi((byte)0x00);
				} else {
					sourceAddress.setTon(sourceAddrTon);
					sourceAddress.setNpi(sourceAddrNpi);
				}
			}

			sourceAddress.setAddress(addr);
		} catch (Exception ex) {
			log.warn(ex, ex);
		}
		return sourceAddress;
	}
	
	private Address initSourceAddress(String addr, GmmsMessage gmmsMsg, A2PCustomerInfo cst) {
		Address sourceAddress = new Address();
		try {

			if (gmmsMsg.getMessageType().equalsIgnoreCase(
					GmmsMessage.MSG_TYPE_DELIVERY_REPORT)) {
				if(cst.getdSmppParaTonFlag() ==1){
					if (gmmsUtility.isAlphanumeric(addr)) {
						sourceAddress.setTon((byte)0x05);
						sourceAddress.setNpi((byte)0x00);
					} else {
						sourceAddress.setTon(sourceAddrTon);
						sourceAddress.setNpi(sourceAddrNpi);
					}
				}else if (cst.getdSmppParaTonFlag() ==2) {
					sourceAddress.setTon((byte)0x00);
					sourceAddress.setNpi((byte)0x00);
				} else {
					String addrType = gmmsMsg.getSenderAddrType();
					if (addrType == null) {
						sourceAddress.setNpi(sourceAddrNpi);
					} else {
						sourceAddress.setNpi(Byte.parseByte(addrType));
					}
					String addrTon = gmmsMsg.getSenderAddrTon();
					if(addrTon == null){
						sourceAddress.setTon(sourceAddrTon);
					} else {
						sourceAddress.setTon(Byte.parseByte(addrTon));
					}
				}
			} else {
				// sender number is Alphanumeric
				if (gmmsUtility.isAlphanumeric(addr)) {
					sourceAddress.setTon((byte)0x05);
					sourceAddress.setNpi((byte)0x00);
				} else {
					sourceAddress.setTon(sourceAddrTon);
					sourceAddress.setNpi(sourceAddrNpi);
				}
			}

			sourceAddress.setAddress(addr);
		} catch (Exception ex) {
			log.warn(ex, ex);
		}
		return sourceAddress;
	}

	/**
	 * init dest address
	 * 
	 * @param addr
	 *            String
	 * @param addrType
	 *            String
	 */
	private Address initRecipicentAddress(String addr, GmmsMessage gmmsMsg, A2PCustomerInfo cst) {
		Address destAddress = new Address();
		try {
			if (gmmsMsg.getMessageType().equalsIgnoreCase(
					GmmsMessage.MSG_TYPE_DELIVERY_REPORT)) {
				
				if(cst.getdSmppParaTonFlag() ==1){
					if (gmmsUtility.isAlphanumeric(addr)) {
						destAddress.setTon((byte)0x05);
						destAddress.setNpi((byte)0x00);
					} else {
						destAddress.setTon(sourceAddrTon);
						destAddress.setNpi(sourceAddrNpi);
					}
				}else if (cst.getdSmppParaTonFlag() ==2) {
					destAddress.setTon((byte)0x00);
					destAddress.setNpi((byte)0x00);
				} else {
					String addrType = gmmsMsg.getRecipientAddrType();
					if (addrType == null) {
						destAddress.setNpi(destAddrNpi);					
					} else {
						destAddress.setNpi(Byte.parseByte(addrType));
					}
					String addrTon = gmmsMsg.getRecipientAddrTon();
					if(addrTon == null){
						destAddress.setTon(destAddrTon);
					} else {
						destAddress.setTon(Byte.parseByte(addrTon));
					}
				}				
			} else {
				destAddress.setTon(destAddrTon);
				destAddress.setNpi(destAddrNpi);
			}
			destAddress.setAddress(addr);
		} catch (Exception ex) {
			log.warn(ex, ex);
		}
		return destAddress;
	}
	
	private Address initRecipicentAddress(String addr, GmmsMessage gmmsMsg) {
		Address destAddress = new Address();
		try {
			if (gmmsMsg.getMessageType().equalsIgnoreCase(
					GmmsMessage.MSG_TYPE_DELIVERY_REPORT)) {
				String addrType = gmmsMsg.getRecipientAddrType();
				if (addrType == null) {
					destAddress.setNpi(destAddrNpi);
				} else {
					destAddress.setNpi(Byte.parseByte(addrType));
				}
				String addrTon = gmmsMsg.getRecipientAddrTon();
				if(addrTon == null){
					destAddress.setTon(destAddrTon);
				} else {
					destAddress.setTon(Byte.parseByte(addrTon));
				}
			} else {
				destAddress.setTon(destAddrTon);
				destAddress.setNpi(destAddrNpi);
			}
			destAddress.setAddress(addr);
		} catch (Exception ex) {
			log.warn(ex, ex);
		}
		return destAddress;
	}
	
	

	/**
	 * check the fields of bindrequest, now only check following fields.
	 * 
	 * @param request
	 *            BindRequest
	 * @return SMPPStatus
	 */
	public int checkBindRequest(BindRequest request) {
		if (request.getSystemId() == null) {
			log.trace("BindRequest absent SystemId,sequenceNumber="
					+ request.getSequenceNumber());
			return Data.ESME_RINVSYSID;
		}
		if (request.getPassword() == null) {
			log.trace("BindRequest absent Password,sequenceNumber="
					+ request.getSequenceNumber());
			return Data.ESME_RINVPASWD;
		}
		return Data.ESME_ROK;
	}

	/**
	 * check if the request is valid
	 * 
	 * @param request
	 *            Outbind
	 * @return SMPPStatus
	 */
	public int checkOutBindRequest(Outbind request) {
		if (request.getSystemId() == null) {
			return Data.ESME_RINVSYSID;
		}
		if (request.getPassword() == null) {
			return Data.ESME_RINVPASWD;
		}
		return Data.ESME_ROK;
	}

	/**
	 * check if the request is valid
	 * 
	 * @param request
	 *            SubmitSM
	 * @return SMPPStatus
	 */
	public int checkSubmitRequest(SubmitSM request) {
		if (request.getDestAddr() == null) {
			return Data.ESME_RINVDSTADR;
		}
		/*if(request.get){
			
		}*/
		return Data.ESME_ROK;
	}

	/**
	 * check if the request is valid
	 * 
	 * @param request
	 *            DeliverSM
	 * @return SMPPStatus
	 */
	public int checkDeliverRequest(DeliverSM request) {
		if (request.getDestAddr() == null) {
			return Data.ESME_RINVDSTADR;
		}
		return Data.ESME_ROK;
	}

	/**
	 * parse esm class
	 * 
	 * @param cst
	 *            GmmsCustomer
	 * @param request
	 *            DeliverSM
	 * @param msg
	 *            GmmsMessage
	 * @param messsagId
	 *            String
	 */
	public void parseEsmClass(A2PCustomerInfo cst, Request request,
			GmmsMessage msg, String messsagId) {
		byte esmClass = 0;
		int temp = 0;

		if (request.getCommandId() == Data.SUBMIT_SM) {
			SubmitSM submitRequest = (SubmitSM) request;
			esmClass = submitRequest.getEsmClass();
			temp = esmClass & 0x08;
			if (temp > 0) { // it's devery receipt
				try {
					msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
					String shortMsg;
					if (submitRequest.getShortMessage() != null) {
						shortMsg = submitRequest.getShortMessage();
						handleDRContent(msg, shortMsg, request, cst);
					} else if (submitRequest.hasMessagePayload()) {
						shortMsg = new String(submitRequest.getMessagePayload()
								.getBuffer());
						handleDRContent(msg, shortMsg, request, cst);
					} else {// use TLV
						log.warn(msg, "The shortmessage is null in SUBMIT_SM.");
					}
				} catch (Exception e) {
					log.error(msg,
							"fail to parse the shortmessage of Submit_SM", e);
					return;
				}
			} else { // new msg
				msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT);
				boolean isUdh = (esmClass & 0x40) > 0;
				if (isUdh) {
					if(log.isInfoEnabled()){
						log.info(msg,
							" SUBMIT_SM is a UDH message, and esmClass is {}",esmClass);
					}
				}
				handleContent(cst, request, msg, isUdh);
			}
		} else if (request.getCommandId() == Data.DELIVER_SM) { // Deliver_SM
			DeliverSM deliverRequest = (DeliverSM) request;
			esmClass = deliverRequest.getEsmClass();
			temp = esmClass & 0x04;
			if (temp > 0) {
				// it's devery receipt
				try {
					msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
					String shortMsg;
					/**
					 * add by brush for support dr in option para.
					 */
					if (cst != null && cst.getDRStatusIsOptionPara()) {
						this.getDRStatusFromOptionPara(request, msg);
					} else {
						if (((DeliverSM) request).getShortMessage() != null) {
							shortMsg = deliverRequest.getShortMessage();
							handleDRContent(msg, shortMsg, request, cst);
						} else if (deliverRequest.hasMessagePayload()) {
							shortMsg = new String(deliverRequest
									.getMessagePayload().getBuffer());
							handleDRContent(msg, shortMsg, request, cst);
						} else { // use TLV
							this.getDRStatusFromOptionPara(request, msg);
						}// end of check TLV
					}
					
					try {
						SmppByteBuffer tlv = ((DeliverSM) request).getNetworkErrorCode();
						if (tlv != null) {
							String netErrorcode = bytesToHexString(tlv.getBuffer());
							log.info(msg, "get the network error code is:{}", netErrorcode);
							if(cst.getSmsOptionDRNoCreditCode().contains(netErrorcode)){
								msg.setStatus(GmmsStatus.REJECTED_BYNC);
							}							
						}
					} catch (Exception e) {
						//log.info(msg, "Parse NetworkErrorCode error", e);
						
					}

				}// end of try
				catch (Exception e) {
					log.error(msg,
							"fail to parse the shortmessage of deliver_SM", e);
					return;
				}
			} else { // it's normal deliver message
				msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT);
				boolean isUdh = (esmClass & 0x40) > 0;
				if (isUdh) {
					if(log.isDebugEnabled()){
						log.debug(msg,
							" DELIVER_SM is a UDH message, and esmClass is {}", esmClass);
					}
				}
				handleContent(cst, request, msg, isUdh);
			}
		} else {
			if(log.isInfoEnabled()){
				log.info(msg, "A2P does not support the PDU and PDU Command is {}",request.getCommandId());
			}
		}

	}
	
	public static final String bytesToHexString(byte[] bArray) {
		  StringBuffer sb = new StringBuffer(bArray.length);
		  String sTemp;
		  for (int i = 0; i < bArray.length; i++) {
		   sTemp = Integer.toHexString(0xFF & bArray[i]);
		   if (sTemp.length() < 2)
		    sb.append(0);
		   sb.append(sTemp.toUpperCase());
		  }
		  return sb.toString();
	}

	private void getDRStatusFromOptionPara(Request request, GmmsMessage msg) {
		 if(log.isTraceEnabled()){
			 log.trace(msg, "The DELIVER_SM get DR status from option para!");
		 }
		try {
			if (((DeliverSM) request).hasReceiptedMessageId()) {
				String msgId = ((DeliverSM) request).getReceiptedMessageId();
				msg.setOutMsgID(msgId);
			} else {
				return;
			}

			if (((DeliverSM) request).hasMessageState()) {
				byte code = ((DeliverSM) request).getMessageState();
				GmmsStatus status;
				switch (code) {
				case 1:
					status = GmmsStatus.ENROUTE;
					break;
				case 2:
				case 6:
					status = GmmsStatus.DELIVERED;
					break;
				case 3:
					status = GmmsStatus.EXPIRED;
					break;
				case 4:
					status = GmmsStatus.DELETED;
					break;
				case 5:
					status = GmmsStatus.UNDELIVERABLE;
					break;
				case 8:
					status = GmmsStatus.REJECTED;
					break;
				default:
					status = GmmsStatus.UNKNOWN;
				}
				msg.setStatus(status);
			}// end of if hasMessageState()
			else if (((DeliverSM) request).hasNetworkErrorCode()) {
				msg.setStatus(GmmsStatus.UNDELIVERABLE);
			} else {
				msg.setStatus(GmmsStatus.DELIVERED);
				log
						.trace(
								msg,
								"not have the option para for DR status! but can get outMsgId so set delivered.");
			}
		} catch (Exception ex) {
			log.error(msg, "fail to parse the option parameter of deliver_SM",
					ex);
		}
	}

	/**
	 * return charset
	 * 
	 * @param dataCoding
	 *            int
	 * @param cstNameShort
	 *            String
	 * @return String
	 */
	private String getCharset(GmmsMessage msg, byte dataCoding,
			String cstNameShort) {
		String charset = GmmsMessage.AIC_CS_ASCII;
		// added by Jianming for Polar wireless in v1.0.1
		if ((dataCoding & -12) == -12) {// 1111 x1xx,1111 0100=-12
			String specialDataCodingScheme = this.byteToHex(dataCoding);// Integer.toHexString(dataCoding);
			msg.setSpecialDataCodingScheme(specialDataCodingScheme);
			return GmmsMessage.AIC_MSG_TYPE_BINARY; // transfer to Smpp 8-bit
													// format
		} else if ((dataCoding & -16) == -16) {// 1111 x0xx,1111 0000=-16
			String specialDataCodingScheme = byteToHex(dataCoding);// Integer.toHexString(dataCoding);
			msg.setSpecialDataCodingScheme(specialDataCodingScheme);
			return GmmsMessage.AIC_CS_ASCII; // transfer to default format
		}
		// end add
		switch (dataCoding) {
		case 0:
		case 1:
			charset = GmmsMessage.AIC_CS_ASCII;
			break;
		case 3:
			charset = GmmsMessage.AIC_CS_ISO8859_1;
			break;
		case 2:
		case 4: {
			if ("STelecom_VN".equalsIgnoreCase(cstNameShort)) {
				charset = GmmsMessage.AIC_CS_ASCII;
			} else {
				charset = GmmsMessage.AIC_MSG_TYPE_BINARY;
			}
			break;
		}
		case 6:
			charset = GmmsMessage.AIC_CS_ISO8859_5;
			break;
		case 7:
			charset = GmmsMessage.AIC_CS_ISO8859_8;
			break;
		case 8:
			charset = GmmsMessage.AIC_CS_UCS2;
			break;
		case 10:
			charset = GmmsMessage.AIC_CS_ISO2022_JP;
			break;
		case 14:
			charset = GmmsMessage.AIC_CS_KSC5601;
			break;
		default:
			charset = GmmsMessage.AIC_CS_ASCII;
		}
		return charset;
	}

	/**
	 * return data coding
	 * 
	 * @param charset
	 *            String
	 * @return byte
	 */
	private byte getDataCoding(String charset, String cstNameShort) {

		if (GmmsMessage.AIC_MSG_TYPE_BINARY.equalsIgnoreCase(charset)) {
			return 4;
		}
		if (GmmsMessage.AIC_CS_ISO8859_1.equalsIgnoreCase(charset)) {
			return 3;
		}
		if (GmmsMessage.AIC_CS_ISO8859_5.equalsIgnoreCase(charset)) {
			return 6;
		}
		if (GmmsMessage.AIC_CS_ISO8859_8.equalsIgnoreCase(charset)) {
			return 7;
		}
		if (GmmsMessage.AIC_CS_UCS2.equalsIgnoreCase(charset)) {
			return 8;
		}
		if ("SMSCdefaultCharset".equalsIgnoreCase(charset)) {
			return 0;
		}
		if (GmmsMessage.AIC_CS_ISO2022_JP.equalsIgnoreCase(charset)) {
			return 10;
		}
		if (GmmsMessage.AIC_CS_KSC5601.equalsIgnoreCase(charset)
				|| GmmsMessage.AIC_CS_EUCKR.equalsIgnoreCase(charset)) {
			return 14;
		}
		// ASCII
		if (GmmsMessage.AIC_CS_ASCII.equalsIgnoreCase(charset)) {
			return 1;
		}
		return 0;
	}

	/**
	 * get the statecode from state of deliveryreport
	 * 
	 * @param state
	 *            String
	 * @return int
	 */
	public int getCodeFromState(String state) {
		if (state == null) {
			return 10900;
		} else if ("DELIVRD".equalsIgnoreCase(state)) {
			return 10000;
		} else if ("EXPIRED".equalsIgnoreCase(state)) {
			return 10200;
		} else if ("DELETED".equalsIgnoreCase(state)) {
			return 10300;
		} else if ("UNDELIV".equalsIgnoreCase(state)) {
			return 10400;
		} else if ("ACCEPTD".equalsIgnoreCase(state)) {
			return 10001;
		} else if ("REJECTD".equalsIgnoreCase(state)) {
			return 10500;
		} else if ("UNKNOWN".equalsIgnoreCase(state)) {
			return 10900;
		}else if ("Undelivery by msg format error".equalsIgnoreCase(state)) {
			return 10401;
		}else if ("Undelivery by recipient error".equalsIgnoreCase(state)) {
			return 10402;
		}else if ("Undelivery by daily limit control".equalsIgnoreCase(state)) {
			return 10403;
		}else if ("Undelivery by sender error".equalsIgnoreCase(state)) {
			return 10404;
		}else if ("Undelivery by network error".equalsIgnoreCase(state)) {
			return 10405;
		}else if ("Undelivery by throttling control".equalsIgnoreCase(state)) {
			return 10406;
		}else if ("Reject by Insufficient balance".equalsIgnoreCase(state)) {
			return 10502;
		}else if ("Reject by template fail".equalsIgnoreCase(state)) {
			return 10503;
		} else {
			return 10900;
		}
	}
	

	/**
	 * return status text by status code
	 * 
	 * @param code
	 *            int
	 * @return String
	 */
	public String getStateTextFromCode(int code) {
		switch (code) {
		case 10000:
			return "DELIVRD";
		case 10001:
			return "ACCEPT";
		case 10105:
			return "DELIVRD";
		case 10200:
			return "EXPIRED";
		case 10300:
			return "DELETED";
		case 10400:
			return "UNDELIV";
		case 10401:
			return "UNDELIV";
		case 10402:
			return "UNDELIV";
		case 10403:
			return "UNDELIV";
		case 10404:
			return "UNDELIV";
		case 10405:
			return "UNDELIV";
		case 10406:
			return "UNDELIV";
		case 10500:
			return "REJECTD";
		case 10502:
			return "REJECTD";
		case 10503:
			return "REJECTD";
		case 10900:
		default:
			return "UNKNOWN";
		}
	}

	private String getErrCodFromStatusCode(int code) {
		switch (code) {
		case 10000:
			return "000";
		case 10105:
			return "000";
		case 10200:
			return "001";
		case 10300:
			return "002";
		case 10400:
			return "003";
		case 10401:
			return "011";
		case 10402:
			return "012";
		case 10403:
			return "013";
		case 10404:
			return "014";
		case 10405:
			return "016";
		case 10406:
			return "017";
		case 10500:
			return "005";
		case 10501:
			return "006";
		case 10502:
			return "020";
		case 10503:
			return "011";
		case 10800:
			return "008";
		case 10801:
			return "014";
		case 10802:
			return "014";
		case 10803:
			return "019";
		case 10804:
			return "019";
		case 10900:
		default:
			return "004";
		}
	}

	public String[] getMncMccBySub(SubmitSM request, short tag) {
		try {
			SmppByteBuffer bb = null;
			if (tag == (short) 0x202) {
				bb = new SmppByteBuffer(request.getSourceSubaddress()
						.getBuffer());
			} else if (tag == (short) 0x203) {
				bb = new SmppByteBuffer(request.getDestSubaddress().getBuffer());
			}

			if (bb == null) {
				return null;
			}
			bb.removeByte();
			String mcc = bb.removeString(3, GmmsMessage.AIC_CS_ASCII);
			String mnc = bb.removeString(3, GmmsMessage.AIC_CS_ASCII);
			return new String[] { mnc, mcc };
		} catch (Exception ex) {
			return null;
		}
	}

	public String[] getMncMccBySub(DeliverSM request, short tag) {
		try {
			SmppByteBuffer bb = null;
			if (tag == (short) 0x202) {
				bb = request.getSourceSubaddress();
			} else if (tag == (short) 0x203) {
				bb = request.getDestSubaddress();
			}

			if (bb == null) {
				return null;
			}
			bb.removeByte();
			String mcc = bb.removeString(3, GmmsMessage.AIC_CS_ASCII);
			String mnc = bb.removeString(3, GmmsMessage.AIC_CS_ASCII);
			return new String[] { mnc, mcc };
		} catch (Exception ex) {
			return null;
		}
	}

	public void handleTransparency(A2PCustomerInfo cst, Request request,
			GmmsMessage msg) {
		try {
			if (handleOTransparncy(cst, request, msg)) {
				handleRTransparncy(cst, request, msg);
			}
		} catch (Exception e) {
			log.error(e, e);
		}
	}

	private boolean handleOTransparncy(A2PCustomerInfo cst, Request request,
			GmmsMessage msg) throws DataManagerException {
		OperatorRouter or;

		or = smsOperatorRouter;

		short tag;
		String[] oMncMcc = null;
		int o_op;
		int oMncMccLength ;
		
		tag = (short) 0x0202;
		if (ctm.isOperator(cst.getSSID())) { // ossid is an operator
			if(log.isTraceEnabled()){
				log.trace(msg,"The ossid is an operator, get the MNC/MCC from CCB.");
			}
			msg.setOoperator(cst.getSSID());
			if (ctm.getTransparency(cst.getSSID()) > 0) {
				oMncMcc = ctm.getMncMccbySsid(cst.getSSID());
				if (oMncMcc != null) {
					msg.setOMncMcc(oMncMcc[0], oMncMcc[1]);
				} else if (ctm.getTransparency(cst.getSSID()) == 1) {
					msg.setStatus(GmmsStatus.SENDER_ADDR_ERROR);
					return false;
				} else {
					if(log.isInfoEnabled()){
						log.info(msg,"Did not get the MCC/MNC from CCB for:{}", cst.getSSID());
					}
				}
			}
		} else { // ossid is a hub
			if (hasMncMcc(request, tag)) { // MNC/MCC of sender is in the pdu
				if (request.getCommandId() == Data.SUBMIT_SM) {
					oMncMcc = getMncMccBySub((SubmitSM) request, tag);
				} else if (request.getCommandId() == Data.DELIVER_SM) {
					oMncMcc = getMncMccBySub((DeliverSM) request, tag);
				}
				if (oMncMcc != null) {
					if(log.isInfoEnabled()){
						log.info(msg,"Get the MCC:{}, MNC:{} of sender from pdu.",
							oMncMcc[1], oMncMcc[0]);
					}
					String mnc = oMncMcc[0];
					if (mnc.endsWith("0")) {
						mnc = "0" + mnc.substring(0, 2);
						o_op = ctm.getSmsSsidByMncMcc(mnc + "_" + oMncMcc[1]);
						if (o_op <= 0) {
							mnc = oMncMcc[0];
							o_op = ctm.getSmsSsidByMncMcc(oMncMcc[0] + "_"
									+ oMncMcc[1]);
						}else{
                        	oMncMccLength = ctm.getCustomerBySSID(o_op).getMCCMNCLength();
                            if(6 == oMncMccLength){// mnc reset process:                    
                            	mnc = oMncMcc[0];
                            	o_op = ctm.getSmsSsidByMncMcc(oMncMcc[0] + "_" + oMncMcc[1]);
                            	if(log.isTraceEnabled()){
                            		log.trace(msg,"get o_op with oMncMccLength=6 in handleOTransparncy() function.");
                            	}
                            }
                        }
					} else {
						o_op = ctm.getSmsSsidByMncMcc(oMncMcc[0] + "_"
								+ oMncMcc[1]);
					}
					msg.setOMncMcc(mnc, oMncMcc[1]);
					msg.setOoperator(o_op);

					if (o_op <= 0)
						return true;
				} else if (ctm.getTransparency(cst.getSSID()) == 1) {
					msg.setStatus(GmmsStatus.SENDER_ADDR_ERROR);
					return false;
				}
			} else { // MNC/MCC of sender is not in the pdu
				o_op = or.getOoperatorSYN(msg);				
				if (o_op > 0) {
					oMncMcc = ctm.getMncMccbySsid(o_op);
					oMncMccLength = ctm.getCustomerBySSID(o_op).getMCCMNCLength();
					if (oMncMcc != null && oMncMcc.length == 2
							&& Integer.parseInt(oMncMcc[0]) > 0
							&& Integer.parseInt(oMncMcc[1]) > 0) {
						String mnc = oMncMcc[0];
						if(5 == oMncMccLength && mnc.endsWith("0")){   	
                            mnc = "0" + mnc.substring(0, 2);
                            if(log.isTraceEnabled()){
                            	log.trace(msg,"mnc = "+mnc+" with oMncMccLength=5 in handleOTransparncy() function.");
                            }
                        }
						msg.setOMncMcc(mnc, oMncMcc[1]);
					} else if (ctm.getTransparency(cst.getSSID()) == 1) {
						msg.setStatus(GmmsStatus.SENDER_ADDR_ERROR);
						if(log.isInfoEnabled()){
							log.info(msg, "O Hub require tranparency feature, but do not transfer the original information.");
						}
						return false;
					}
				}
			}
		}
		return true;
	}

	private void handleRTransparncy(A2PCustomerInfo cst, Request request,
			GmmsMessage msg) throws TLVException {
		OperatorRouter or;

		or = smsOperatorRouter;

		short tag;
		tag = (short) 0x0203;
		String[] rMncMcc = null;
		int rMncMccLength;
		if (hasMncMcc(request, tag)) { // MNC/MCC of recipient is in the request
			if(log.isTraceEnabled()){
				log.trace("has mncmccc of recipient");
			}
			if (request.getCommandId() == Data.SUBMIT_SM) {
				rMncMcc = getMncMccBySub((SubmitSM) request, tag);
			} else if (request.getCommandId() == Data.DELIVER_SM) {
				rMncMcc = getMncMccBySub((DeliverSM) request, tag);
			}
			if (rMncMcc != null) {
				if(log.isInfoEnabled()){
					log.info(msg,"Get the MCC:{}, MNC:{} of recipient from pdu.",
						rMncMcc[1], rMncMcc[0]);
				}
				String mnc = rMncMcc[0];
				int r_op;
				if (mnc.endsWith("0")) {
					mnc = "0" + mnc.substring(0, 2);
					r_op = ctm.getSmsSsidByMncMcc(mnc + "_" + rMncMcc[1]);
					if (r_op <= 0) {
						mnc = rMncMcc[0];
						r_op = ctm.getSmsSsidByMncMcc(rMncMcc[0] + "_"
								+ rMncMcc[1]);
					}else{
						rMncMccLength = ctm.getCustomerBySSID(r_op).getMCCMNCLength();
	                    if(6 == rMncMccLength){// mnc reset process:     
	                    	mnc = rMncMcc[0];
	                    	r_op = ctm.getSmsSsidByMncMcc(rMncMcc[0] + "_" + rMncMcc[1]);
	                    	if(log.isTraceEnabled()){
	                    		log.trace(msg,"get r_op with rMncMccLength=6 in handleRTransparncy() function.");
	                    	}                        	
	                    }
					}
				} else {
					r_op = ctm.getSmsSsidByMncMcc(mnc + "_" + rMncMcc[1]);
				}
				if (r_op <= 0) {
					log.warn("Can't get the operator by MNC/MCC.");
					msg.setStatus(GmmsStatus.RECIPIENT_ADDR_ERROR);
					return;
				}
				msg.setRMncMcc(mnc, rMncMcc[1]);
				msg.setRoperator(r_op);
			} else {
				log.warn(msg,"Parse error for rMncMcc from PDU.");
				msg.setStatus(GmmsStatus.RECIPIENT_ADDR_ERROR);
				return;
			}
		} else if (cst.getMessageMode() == MessageMode.STORE_FORWARD
				&& ctm.getTransparency(cst.getSSID()) > 0) {
			// MNC/MCC of recipient is not in the request, and store_forward
			if(log.isInfoEnabled()){
				log.info(msg,"Did not get the MCC/MNC of recipient from pdu.");
			}
			if (ctm.getTransparency(cst.getSSID()) == 1) {
				if(log.isInfoEnabled()){
					log.info(msg,"TransparencyMode is 1, get MCC/MNC by query DNS.");
				}
				int r_op = or.getRoperatorSYN(msg);
				rMncMcc = ctm.getMncMccbySsid(r_op);				
				if (rMncMcc == null || rMncMcc.length < 2
						|| Integer.parseInt(rMncMcc[0]) < 0
						|| Integer.parseInt(rMncMcc[1]) < 0 || r_op <0) {
					log.warn("Did not get the MCC/MNC from DNS for:"
							+ msg.getRecipientAddress());
					msg.setStatus(GmmsStatus.RECIPIENT_ADDR_ERROR);
					return;
				} else {
					String mnc = rMncMcc[0];
					rMncMccLength = ctm.getCustomerBySSID(r_op).getMCCMNCLength();
					if(5 == rMncMccLength && mnc.endsWith("0")){
	                	mnc = "0" + mnc.substring(0, 2);
	                	if(log.isTraceEnabled()){
	                		log.trace(msg,"mnc = "+mnc+" with rMncMccLength=5 in handleRTransparncy() function.");
	                	}                	
	                }
					msg.setRMncMcc(mnc, rMncMcc[1]);
					msg.setRoperator(r_op);
				}
			}
		}
	}

	public boolean hasMncMcc(Request request, short tag) {
		try {
			if (tag == (short) 0x0202) {
				if (request.getCommandId() == Data.SUBMIT_SM) {
					return ((SubmitSM) request).hasSourceSubaddress();
				}
				if (request.getCommandId() == Data.DELIVER_SM) {
					return ((DeliverSM) request).hasSourceSubaddress();
				}
			} else if (tag == (short) 0x0203) {
				if (request.getCommandId() == Data.SUBMIT_SM) {
					return ((SubmitSM) request).hasDestSubaddress();
				}
				if (request.getCommandId() == Data.DELIVER_SM) {
					return ((DeliverSM) request).hasDestSubaddress();
				}
			}
			return false;
		} catch (Exception ex) {
			log.error(ex, ex);
			return false;
		}
	}

	/**
	 * when received a message, remove prefix for the customers
	 * 
	 * @param gmmsMsg
	 *            GmmsMessage
	 * @param cst
	 *            GmmsCustomer
	 */
	public void removePrefixForCustomer(GmmsMessage gmmsMsg, A2PCustomerInfo cst) {
		if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(gmmsMsg
				.getMessageType())) {
			return;
		}

		if ("CU_CN".equalsIgnoreCase(cst.getShortName())
				|| "STelecom_VN".equalsIgnoreCase(cst.getShortName())) {
			removePrefix(gmmsMsg, "00");
		} else if ("KTF_KR".equalsIgnoreCase(cst.getShortName())
				&& (!gmmsMsg.getSenderAddress().startsWith("82"))) {
			gmmsMsg
					.setSenderAddress(addPrefix(gmmsMsg.getSenderAddress(),
							"82"));
		} else if ("Skytel_MN".equalsIgnoreCase(cst.getShortName())) {
			String Skytel_Prefix = gmmsUtility.getCommonProperty(
					"Skytel_Prefix", "001");
			if (Skytel_Prefix != null && !Skytel_Prefix.equalsIgnoreCase("")) {
				removePrefix(gmmsMsg, Skytel_Prefix);
			}
			String sender = gmmsMsg.getSenderAddress();
			if (sender != null && sender.startsWith("96")) {
				sender = sender.replaceFirst("96", "976");
				gmmsMsg.setSenderAddress(sender);
			}
		} else if ("Indigo_JP".equalsIgnoreCase(cst.getShortName())) {
			String recipAdd = gmmsMsg.getRecipientAddress();
			if (recipAdd.startsWith("282") || recipAdd.startsWith("582")) {
				recipAdd = recipAdd.substring(1);
				gmmsMsg.setRecipientAddress(recipAdd);
			}
		} else if ("Unitel_MN".equalsIgnoreCase(cst.getShortName())) {
			String recipAdd = gmmsMsg.getRecipientAddress();
			if (recipAdd.startsWith("00")) {
				recipAdd = recipAdd.substring(3);
				gmmsMsg.setRecipientAddress(recipAdd);
			}
		} else if (A2PCustomerInfo.Iusacell_ShortName.equalsIgnoreCase(cst
				.getShortName())) {
			if (!gmmsMsg.getSenderAddress().startsWith("52")) {
				gmmsMsg.setSenderAddress("52" + gmmsMsg.getSenderAddress());
			}
			if (gmmsMsg.getRecipientAddress().startsWith("-00")) {
				gmmsMsg.setRecipientAddress(gmmsMsg.getRecipientAddress()
						.substring("-00".length()));
			}
		}
		/**
		 * add by brush for Eagle mobile sub-prefix
		 */
		else if ("Eaglemobile_AL".equalsIgnoreCase(cst.getShortName())) {

			String subfix = cst.getRemoveRecPrefix();
			String recipent = gmmsMsg.getRecipientAddress();
			if (subfix != null && !subfix.equals("") && recipent != null
					&& recipent.startsWith(subfix)) {
				gmmsMsg
						.setRecipientAddress(recipent
								.substring(subfix.length()));
			}

		}

	}

	/**
	 * handle china unicom's 00+ countrycode + phone# case. handle Skytel's
	 * 001/002 + phone# case.
	 * 
	 * @param msg
	 *            GmmsMessage
	 * @param prefix
	 *            String
	 */
	public void removePrefix(GmmsMessage msg, String prefix) {
		if (msg == null) {
			return;
		}
		String recipient = msg.getRecipientAddress();
		String sender = msg.getSenderAddress();
		if (recipient != null && recipient.startsWith(prefix)) {
			msg.setRecipientAddress(recipient.substring(prefix.length()));
		}
		if (sender != null && sender.startsWith(prefix)) {
			msg.setSenderAddress(sender.substring(prefix.length()));
		}
	}

	public void addMncMccForRequest(GmmsMessage msg, Request request)
	throws TLVException {
			String[] mncMcc = null;
			// add mnc/mcc of the sender in the pdu
			String oMncMcc = msg.getOMncMcc();
			String mnc = null;
			if (oMncMcc == null || "".equals(oMncMcc)) {
				if(log.isTraceEnabled()){
					log.trace(msg,"The MNC/MCC of the sender in the msg is null, get it by CCB");
				}
				int oMncMccLength;
				A2PCustomerInfo o_Customer = ctm.getCustomerBySSID(msg.getOoperator());
				if(o_Customer != null){
					oMncMccLength = o_Customer.getMCCMNCLength();
				}else{
					oMncMccLength = 5;
				}
				
				mncMcc = ctm.getMncMccbySsid(msg.getOoperator());
				if (mncMcc != null && mncMcc.length == 2) {
					mnc = mncMcc[0];
					if (oMncMccLength==5 && mnc.startsWith("0")) {
			            mnc = mnc.substring(1, 3) + "0";
			            if(log.isTraceEnabled()){
			            	log.trace(msg,"mnc = "+mnc+" with oMncMccLength=5 in addMncMccForRequest() function.");
			            }                    
			        }
					oMncMcc = mnc + mncMcc[1];
					if(log.isTraceEnabled()){
						log.trace(msg,"change mnc to: {}", oMncMcc);
					}
				}
			}
			SmppByteBuffer bb = null;
			if (oMncMcc == null || oMncMcc.length() != 6) {
				if(log.isDebugEnabled()){
					log.debug(msg,"The MNC/MCC of {} is error!", msg.getOoperator());
				}
			} else {
				if(log.isTraceEnabled()){
					log.trace(msg,
							"Append the MCC:{}, MNC:{} of the sender in the PDU.",
							oMncMcc.substring(3), oMncMcc.substring(0, 3));
				}
				bb = new SmppByteBuffer();
				bb.appendByte((byte) 0xA0);
				bb.appendString(oMncMcc.substring(3));
				bb.appendString(oMncMcc.substring(0, 3));
				request.setExtraOptional((short) 0x0202, bb);
			}
			if (!ctm.isVirtualOperator(msg.getRoperator())) { 
				String rMncMcc = msg.getRMncMcc();
				if (rMncMcc == null || "".equals(rMncMcc)) {
					if(log.isTraceEnabled()){
						log.trace(msg,"The MNC/MCC of the recipient in the msg is null, get it by CCB");
					}
					mncMcc = ctm.getMncMccbySsid(msg.getRoperator());
					
					int rMncMccLength;
					A2PCustomerInfo r_Customer = ctm.getCustomerBySSID(msg.getRoperator());
					if(r_Customer != null){
						rMncMccLength = r_Customer.getMCCMNCLength();
					}else{
						rMncMccLength = 5;
					}
					
					if (mncMcc != null && mncMcc.length == 2) {
						mnc = mncMcc[0];
						 if (5 == rMncMccLength && mnc.startsWith("0")) {
			                    mnc = mnc.substring(1, 3) + "0";
			                    if(log.isTraceEnabled()){
			                    	log.trace(msg,"change mnc to: " + mnc+" with rMncMccLength = 5 in addMncMccForRequest() function.");
			                    }
			                }
						rMncMcc = mnc + mncMcc[1];
						if(log.isTraceEnabled()){
							log.trace(msg,"change mnc to: {}", rMncMcc);
						}
					}
				}
				if (rMncMcc == null || rMncMcc.length() != 6) {
					log.warn(msg,"The MNC/MCC of {} is error!", msg.getRoperator());
				} else {
					if(log.isTraceEnabled()){
						log.trace(msg,"Append the MCC:{}, MNC:{} of the Recipient in the PDU.",
									rMncMcc.substring(3), rMncMcc.substring(0, 3));
					}
					bb = new SmppByteBuffer();
					bb.appendByte((byte) 0xA0);
					bb.appendString(rMncMcc.substring(3));
					bb.appendString(rMncMcc.substring(0, 3));
					request.setExtraOptional((short) 0x0203, bb);
				}
			}
	}

	/**
	 * when send a request, add prefix for the customers
	 * 
	 * @param gmmsMsg
	 *            GmmsMessage
	 * @param cst
	 *            GmmsCustomer
	 * @return String[]
	 */
	private String[] addPrefixForCustomer(GmmsMessage gmmsMsg,
			A2PCustomerInfo cst) {

		String sender = gmmsMsg.getSenderAddress();
		String recipient = gmmsMsg.getRecipientAddress();
		String cstNameShort = cst.getShortName();

		String Skytel_Prefix = GmmsUtility.getInstance().getCommonProperty(
				"Skytel_Prefix", "001");

		if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(gmmsMsg
				.getMessageType())) {
			if ("CU_CN".equalsIgnoreCase(cstNameShort)) {
				sender = addPrefix(sender, "00");
				recipient = addPrefix(recipient, "00");
			} else if ("Skytel_MN".equalsIgnoreCase(cstNameShort)) {
				if (sender != null && sender.startsWith("976")) {
					sender = sender.replaceFirst("976", "96");
				}
				recipient = addPrefix(recipient, Skytel_Prefix);
			}
		} else { // send a new message to customer
			if ("CU_CN".equalsIgnoreCase(cstNameShort)) {
				sender = addPrefix(sender, "00");
				recipient = addPrefix(recipient, "00");
			} else if ("Skytel_MN".equalsIgnoreCase(cstNameShort)) { // added by
																		// Amy
																		// to
																		// add
																		// prefix
																		// for
																		// Skytel
				sender = addPrefix(sender, Skytel_Prefix);
			} else if ("STelecom_VN".equalsIgnoreCase(cstNameShort)) {
				sender = addPrefix(sender, "00");
			} else if ("Indigo_JP".equalsIgnoreCase(cstNameShort)) {
				if (sender.startsWith("82")) {
					String prefix = gmmsUtility.getCommonProperty(
							"Indigo_prefix", "2");
					sender = addPrefix(sender, prefix);
				}
			} else if ("APT_TW".equalsIgnoreCase(cstNameShort)) {
				String cpid = gmmsUtility.getCommonProperty("APBW_CPID");
				String sid = gmmsUtility.getCommonProperty("APBW_SID");
				sender = addPrefix(sender, sid);
				sender = addPrefix(sender, cpid);
				String tem = "00000000000000000000";
				sender = sender + tem;
				sender = sender.substring(0, 20);

				recipient = addPrefix(recipient, "1415");
			} else if (A2PCustomerInfo.Iusacell_ShortName
					.equalsIgnoreCase(cstNameShort)) {
				if (sender != null && !sender.trim().startsWith("00")) {
					sender = "00" + sender;
					if(log.isTraceEnabled()){
						log.trace("{} add 00 to the source address",
							A2PCustomerInfo.Iusacell_ShortName);
					}
				}
				if (recipient != null && recipient.trim().startsWith("52")) {
					recipient = recipient.substring("52".length());
					if(log.isTraceEnabled()){
						log.trace("{} remove 52 from the recipient address",
								A2PCustomerInfo.Iusacell_ShortName);
					}
					
				}
			}
		}

		return new String[] { sender, recipient };

	}

	private void handleContent(A2PCustomerInfo cst, Request request,
			GmmsMessage msg, boolean udh) {
		try {
			byte dataCoding = 0;
			byte[] btContent = null;
			String charset = null;
			if (request.getCommandId() == Data.SUBMIT_SM) {
				SubmitSM smRequest = (SubmitSM) request;
				dataCoding = smRequest.getDataCoding();								
				if (smRequest.getSmLength() <= 0
						&& smRequest.getShortMessage() == null) {
					if (!smRequest.hasMessagePayload()
							|| (smRequest.getMessagePayload() == null)
							|| (smRequest.getMessagePayload().length() == 0)) {// content
																				// is
																				// null
						if (!udh) {
							msg.setContentType(GmmsMessage.AIC_CS_ASCII);
							msg.setTextContent(" ");
							msg.setMessageSize(1);
							log.warn(msg, "The message is a null message.");
							return;
						}
					} else {// content in payload
						btContent = smRequest.getMessagePayload().getBuffer();
					}
				} else { // content in short_message
					btContent = smRequest.getBinShortMessage();
				}
			} else if (request.getCommandId() == Data.DELIVER_SM) {
				DeliverSM deRequest = (DeliverSM) request;
				dataCoding = deRequest.getDataCoding();				
				if (deRequest.getSmLength() <= 0
						&& deRequest.getShortMessage() == null) {
					if (!deRequest.hasMessagePayload()
							|| (deRequest.getMessagePayload() == null)
							|| (deRequest.getMessagePayload().length() == 0)) {// content
																				// is
																				// null
						if (!udh) {
							msg.setContentType(GmmsMessage.AIC_CS_ASCII);
							msg.setTextContent(" ");
							msg.setMessageSize(1);
							log.warn(msg, "The message is a null message.");
							return;
						}
					} else {// content in payload
						btContent = deRequest.getMessagePayload().getBuffer();
					}
				} else { // content in short_message
					btContent = deRequest.getBinShortMessage();
				}
			}
			charset = getCharset(msg, dataCoding, cst.getShortName());
			//judge dataCoding = 0 or 00xx 0000, or 1111 x0xx for specialDataCodingScheme 
			if(dataCoding==0||((dataCoding & -16) == -16)){
				if(cst.getSmsOptionIncomingGSM7bit()==1||cst.getSmsOptionIncomingGSM7bit()==2){
					this.handleGSM7bitMessageContent(msg, udh, btContent, cst.getSmsOptionIncomingGSM7bit());
				}else{
					charset = GmmsMessage.AIC_CS_ASCII;
					handleMessageContent(msg, btContent, charset, udh);
				}
			}else{			
				if (charset == null
						|| "SMSCdefaultCharset".equalsIgnoreCase(charset)) {
					charset = GmmsMessage.AIC_CS_ASCII;
				}
				handleMessageContent(msg, btContent, charset, udh);
			}	
			
			//remove udh for handler abnormal customer concatenate msg
			if (msg.getUdh()!=null && cst.isSmsOptionRemoveUdh()) {
				log.info(msg, "remove udh by 'SMSOptionRemoveUdh'={}", cst.isSmsOptionRemoveUdh());
				msg.setUdh(null);
			}
			
		} catch (Exception ex) {
			log.error(ex, ex);
			msg.setContentType(GmmsMessage.AIC_CS_ASCII);
			msg.setTextContent(" ");
			msg.setMessageSize(1);
		}
	}

	private byte[] removeUdhFromContent(GmmsMessage msg, byte[] btContent,
			boolean udh) {

		if (!udh) {
			return btContent;
		}
		int udhLen;
		byte[] messageDataNew;
		 if(log.isTraceEnabled()){
			 log.trace(msg, "The content contains UDH.");
		 }
		udhLen = btContent[0] & 0xff;
		udhLen = udhLen + 1;
		if (btContent.length == udhLen) { // only has udh, no other content
			msg.setUdh(btContent);
			messageDataNew = new byte[0];
			log.warn(msg, "The message has only udh content is null!");
		} else {
			byte[] udhData = new byte[udhLen];
			System.arraycopy(btContent, 0, udhData, 0, udhLen);
			msg.setUdh(udhData);
			messageDataNew = new byte[btContent.length - udhLen];
			System.arraycopy(btContent, udhLen, messageDataNew, 0,
					messageDataNew.length);
		}
		return messageDataNew;
	}

	/**
	 * Decode 7-bit data 
	 * 
	 * @param msg
	 *            GmmsMessage
	 * @param btContent
	 *            byte[]
	 * @throws Exception
	 */
	private void handleGSM7bitMessageContent(GmmsMessage msg, boolean udh, byte[] btContent, int smsOptionIncomingGSM7bit)
			throws Exception {
		
		boolean iso = false;
		boolean ucs2 = false;		
		byte[] newContent  = removeUdhFromContent(msg, btContent, udh);	
		int contentLength = newContent.length; 
		if(newContent==null||newContent.length==0){
			msg.setTextContent("");
			msg.setMessageSize(0);
			msg.setContentType(GmmsMessage.AIC_CS_ASCII);
			return;
		}
			if(smsOptionIncomingGSM7bit ==2){
				int fillBits = 0;
				if(udh){
					int udhLen = btContent[0] & 0xff;
					fillBits = (7 - (udhLen+1)*8%7)%7;
				}				 
				 newContent = Convert.decodeGSM7Bit(newContent, fillBits);
				}
				int []chars = new int[newContent.length];
				int i = 0;
				int j = 0;
				for(i =0, j=0; i<newContent.length;i++,j++){
					//mapping ASCII characters
					int temp = newContent[i];
					int value = -1;
					if(temp<128&&temp>=0){
						value = Convert.gsm2ASCII[temp];
						if(value ==-1){                         //mapping ISO-8859-1 characters
							value = Convert.gsm2ISO[temp];
							if(value == -1){                    //mapping UCS2 characters
								if(temp<28){
									value = Convert.gsm2UCS2[temp];
									if(value ==-1){
										// mapping extended characters
										if(temp==27){
											int c = newContent[i+1];
											switch(c){
											case 10: value=12; break;
											case 20: value=94; break;
											case 40: value=123;  break;
											case 41: value=125; break;
											case 47: value=92; break;
											case 60: value=91; break;
											case 61: value=126; break;
											case 62: value=93;  break;
											case 64: value=124;  break;
											case 101: value=8364; ucs2=true; break;
											default: value =-1;									 
											}
											if(value ==-1){
												 value = 63;
											 }else{										 
												 i++;
											 }
										}else{
											value = 63;
										}
									}else if(!ucs2){
										ucs2=true;
									}//end ucs2
								}else{
									value = 63;
								}								
							}else if(!iso){
								iso=true;
							}//end iso
						}
					}else{
						value = 63;
					}					
					chars[j]=value;
				}
				int tempContent[] = new int[j]; 
				System.arraycopy(chars, 0, tempContent, 0, j);
				String contentType = GmmsMessage.AIC_CS_ASCII;
				if(ucs2){
					contentType = GmmsMessage.AIC_CS_UCS2;
				}else if(iso){
					contentType = GmmsMessage.AIC_CS_ISO8859_1;
				}
				
				String textContent = new String(Convert.intToChar(tempContent));							
				if (textContent.length() > Data.SM_MSG_LEN) { // as it is length of
																// database, one
																// char is length 1
					textContent = textContent.substring(0, Data.SM_MSG_LEN);
				}
				if(log.isTraceEnabled()){
					log.trace("after convert 7-bit to {}, msg content is:{}",
						contentType,textContent);
				}
				msg.setContentType(contentType);
				msg.setTextContent(textContent);
				msg.setGsm7bit(true);
				msg.setMessageSize(contentLength);			
				
	}

	/**
	 * 
	 * @param msg
	 *            GmmsMessage
	 * @param btContent
	 *            byte[]
	 * @param charset
	 *            String
	 * @param udh
	 *            boolean
	 * @throws Exception
	 */
	private void handleMessageContent(GmmsMessage msg, byte[] btContent,
			String charset, boolean udh) throws Exception {
		if (btContent == null) {
			log.warn(msg, "Content is null!");
			return;
		}
		String textContent = null;
		// modify by bruce for support binary message with UDH.

		byte[] content = removeUdhFromContent(msg, btContent, udh);

		if (GmmsMessage.AIC_MSG_TYPE_BINARY.equalsIgnoreCase(charset)) {
			msg.setGmmsMsgType(GmmsMessage.AIC_MSG_TYPE_BINARY);
			msg.setContentType(GmmsMessage.AIC_MSG_TYPE_BINARY); // added by
																	// neal on
																	// 08.03.11
			msg.setMimeMultiPartData(content);
			msg.setMessageSize(content.length);
			return;
		}
		msg.setContentType(charset);
		if (content.length != 0) {
			textContent = new String(content, charset);
			if (textContent.length() > Data.SM_MSG_LEN) { // as it is length of
															// database, one
															// char is length 1
				textContent = textContent.substring(0, Data.SM_MSG_LEN);
			}
			msg.setTextContent(textContent);
			msg.setMessageSize(textContent.getBytes(charset).length);
		} else {
			msg.setContentType(GmmsMessage.AIC_CS_ASCII);
			msg.setTextContent(" ");
			msg.setMessageSize(1);
		}

	}

	/**
	 * 
	 * @param msg
	 *            GmmsMessage
	 * @param shortMsg
	 *            String
	 * @param request
	 *            Request
	 * @param cst
	 *            GmmsCustomer
	 */
	private void handleDRContent(GmmsMessage msg, String shortMsg,
			Request request, A2PCustomerInfo cst) throws Exception {
		msg.setTextContent(shortMsg);

		// parse msgId
		int length = shortMsg.length();
		int beginindex = shortMsg.indexOf("id:");
		int endindex = shortMsg.indexOf("sub");
		String msgId = null;
		String state = null;
		// if can get the sub: string ,then handle all the messageid length.
		if (endindex > beginindex) {
			msgId = shortMsg.substring(beginindex + 3, endindex);
			try {
				if ("Mobicom_MN".equalsIgnoreCase(cst.getShortName())) {
					msgId = msgId.trim();
					int diff = 10 - msgId.length();
					for (int i = 0; i < diff; i++) {
						msgId = "0" + msgId;
					}
				}
			} catch (Exception ex) {
				log.error(ex, ex);
			}
		}
		// else get the 10
		else if (beginindex >= 0 && beginindex + 13 <= length) {
			msgId = shortMsg.substring(beginindex + 3, beginindex + 13);
		}
		msgId = msgId.trim();
		// from hex to decimalist
		if ((request.getVersion()).getVersionID() == 51) {

			long tempMsgId = Long.parseLong(msgId);
			msgId = Long.toHexString(tempMsgId);
			int lenDiff = 8 - msgId.length();
			if ("Skytel_MN".equalsIgnoreCase(cst.getShortName()) && lenDiff > 0) {
				for (int i = 0; i < lenDiff; i++) {
					msgId = "0" + msgId;
				}
			}
		}
		msg.setOutMsgID(msgId);
		// parse status
		beginindex = shortMsg.indexOf("stat:");
		if (beginindex >= 0 && beginindex + 12 <= length) {
			state = shortMsg.substring(beginindex + 5, beginindex + 12);
			int code = getCodeFromState(state);
			msg.setStatusCode(code);
			msg.setStatusText(GmmsStatus.getStatus(code).getText());
			log.trace(msg, "messageId and status is:{} {}" , msgId , state);
		}
		//add error code mapping for Ammex stc
		beginindex = shortMsg.indexOf("err:");
		if((!cst.getSmsOptionDRNoCreditCode().isEmpty()) 
				&& beginindex>= 0 && beginindex + 7 <= length) {
			String netErrorcode = shortMsg.substring(beginindex + 4, beginindex + 7);
			if(cst.getSmsOptionDRNoCreditCode().contains(netErrorcode)){
				msg.setStatus(GmmsStatus.REJECTED_BYNC);
			}
		}
	}

	public void initSmppPara(A2PCustomerInfo server) {
		initSmppPara(server, null);
	}

	/**
	 * init parameters of smpp server
	 * 
	 * @param server
	 *            Server
	 */
	public void initSmppPara(A2PCustomerInfo server,
			ConnectionInfo connectionInfo) {

		if (initial) {
			return;
		}
		if (server == null) {
			return;
		}

		if (connectionInfo != null) {
			this.connectionID = connectionInfo.getConnectionName();
		}

		if (server != null) {
			serviceType = server.getServiceType();
			sourceAddrTon = server.getSourceAddrTon();
			sourceAddrNpi = server.getSourceAddrNpi();
			destAddrTon = server.getDestAddrTon();
			destAddrNpi = server.getDestAddrNpi();
			priorityFlag = server.getPriorityFlag();
			replaceIfFlag = server.getReplaceIfFlag();
			protocolId = server.getProtocolId();// modified by Jianming in
												// 20110726
		}
		initial = true;

	}

	/**
	 * parse Hex msgId to Decimal
	 * 
	 * @param hexMsgId
	 * @param cst
	 * @return
	 */
	private String parseHexMsgId(String hexMsgId, A2PCustomerInfo cst) {
		String msgId = hexMsgId;
		if (cst.getSMPPIsGenHexMsgId()) {// transfer hex to dec format,added by
											// Jianming
			long decMsgId = Long.parseLong(hexMsgId, 16);
			String decMsgIdStr = String.valueOf(decMsgId);
			int msgIdlen = decMsgIdStr.length();
			if (cst.getSMPPIsPadZero4SR() && msgIdlen < 10) {// pad zero
				String zeroStr = "0000000000";
				zeroStr = zeroStr.substring(msgIdlen);
				msgId = zeroStr + decMsgIdStr;
			} else if (msgIdlen <= 10) {
				msgId = decMsgIdStr;
			} else {
				//msgId = decMsgIdStr.substring(0, 10);
			}
		} else {
			if (hexMsgId.length() <= 10) {
				msgId = hexMsgId;
			} else {
				//msgId = hexMsgId.substring(0, 10);
			}
		}
		return msgId;
	}

	/**
	 * Convert a byte to Hex.
	 * 
	 * @param b
	 * @return
	 */
	private static String byteToHex(byte data) {
		String hex = Integer.toHexString(data & 0xFF).toUpperCase();
		return hex;
	}

	/**
	 * Convert Hex to byte.
	 * 
	 * @param b
	 * @return
	 */
	private static byte hexToByte(String hexNumber) {
		byte data = (byte) Integer.parseInt(hexNumber, 16);
		return data;
	}

	public static void main(String[] args) throws Exception {
		String content = "中国";
		String shortMsg = "sub:001 dlvrd:001 submit date:2407190905 done date:2407191705 stat:UNDELIV err:000 text:dlink/veri";
		int beginindex = shortMsg.indexOf("err:");
		String netErrorcode = shortMsg.substring(beginindex + 4, beginindex + 7);
		System.out.println("error: "+netErrorcode);
		GmmsMessage msg = new GmmsMessage();
		msg.setTextContent(content);
		msg.setMessageSize(content.getBytes().length);
		System.out.println(msg.getMessageSize());
		msg.setContentType("UnicodeBigUnmarked");
		msg.setRecipientAddress("82134567");
		A2PSingleConnectionInfo cst = new A2PSingleConnectionInfo();
		cst.setNameShort("CU_CN");
		// handleContentForCU(msg,cst, "82");
		/*System.out.println(msg.getContentType());
		System.out.println(msg.getTextContent());
		System.out.println(msg.getMessageSize());*/
		String msgId = "123456789";
		long tempMsgId = Long.parseLong(msgId);
		msgId = Long.toHexString(tempMsgId);
		String addr = "abcdef123";
		System.out.println(addr.getBytes(GmmsMessage.AIC_CS_UCS2).length);
		System.out.println(msgId);
		
	}

}
