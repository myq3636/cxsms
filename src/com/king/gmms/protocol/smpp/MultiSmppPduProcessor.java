package com.king.gmms.protocol.smpp;

import java.util.Map;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.session.*;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.protocol.smpp.pdu.DeliverSM;
import com.king.gmms.protocol.smpp.pdu.DeliverSMResp;
import com.king.gmms.protocol.smpp.pdu.PDU;
import com.king.gmms.protocol.smpp.pdu.Request;
import com.king.gmms.protocol.smpp.pdu.Response;
import com.king.gmms.protocol.smpp.pdu.SubmitSM;
import com.king.gmms.protocol.smpp.pdu.SubmitSMResp;
import com.king.gmms.protocol.smpp.pdu.WrongLengthOfStringException;
import com.king.gmms.protocol.smpp.util.Data;
import com.king.gmms.protocol.smpp.version.SMPPVersion;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageIdGenerator;

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
public class MultiSmppPduProcessor {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(MultiSmppPduProcessor.class);

	private Smpp smpp;
	private GmmsUtility gmmsUtility;
	private A2PCustomerInfo authedCustms;
	private MultiSmppSession session;
	private SMPPVersion smppVersion = SMPPVersion.getDefaultVersion();
	private boolean dealExceptionDR = true;
	private boolean isInit = false;
	private int moduleIndex = 0;
	private MessageIdGenerator messageIdGenerator = MessageIdGenerator
			.getInstance();

	public MultiSmppPduProcessor(MultiSmppSession session) {
		this.gmmsUtility = GmmsUtility.getInstance();
		this.session = session;
		authedCustms = session.getCustomerInfo();
		smppVersion = session.getSmppVersion();
		dealExceptionDR = authedCustms.isDealExceptionDR();
		smpp = session.getSmpp();
		moduleIndex = session.getModuleIndex();
		isInit = true;
	}

	public MultiSmppPduProcessor() {
		this.gmmsUtility = GmmsUtility.getInstance();
	}

	public void init(MultiSmppSession session) {
		if (isInit == true)
			return;

		this.session = session;
		authedCustms = this.session.getCustomerInfo();
		smppVersion = this.session.getSmppVersion();
		dealExceptionDR = authedCustms.isDealExceptionDR();
		smpp = this.session.getSmpp();
		moduleIndex = session.getModuleIndex();
		isInit = true;
	}


	/**
	 * handle deliver short message
	 * 
	 * @param request
	 *            Request
	 * @return Response
	 */
	public GmmsMessage handleDeliverSM(Request request) {
		GmmsMessage msg = null;
		int status = smpp.checkDeliverRequest((DeliverSM) request);
		if (status != Data.ESME_ROK) {
			log.warn("Paramater Error in DeliverSM Request,sequence={}",
					request.getSequenceNumber());
		}
		String msgId;
		if (request.getVersion().getVersionID() == 0x34) {
			msgId = messageIdGenerator.generateDecID(moduleIndex);
		} else {
			msgId = Integer.toHexString(messageIdGenerator
					.generateHexID(moduleIndex));
		}

		try {
			// should judge the message type: deliver messaage, delivery report.
			msg = smpp.createGmmsMessage((DeliverSM) request, msgId, session
					.getSourceSysID(), authedCustms, session
					.getTransactionURI());

			if (msg != null && msg.getStatusCode() > 0
					&& status != Data.ESME_ROK) { // need change smpp status
													// code to gmms code
				msg.setStatus(SMPPRespStatus.getGmmsStatus(status));
			}
		} catch (WrongLengthOfStringException e) {
			log.warn("MsgIdCreator created an invalid ID (" + msgId
					+ ") for SMPP SubmitResp.", e);
			msg.setStatus(GmmsStatus.INVALID_MSG_FIELD);
		} catch (Exception e) {
			// log.warn("when create gmms message with DeliverSM occur error",
			// e);
			log.warn(
					"Create GmmsMessage from DeliverSM error,DeliverSM sequence:"
							+ request.getSequenceNumber(), e);
		}
		return msg;
	}

	public GmmsMessage handleSubmitSM(Request request) {
		GmmsMessage msg = null;
		int status = smpp.checkSubmitRequest((SubmitSM) request);
		if (status != Data.ESME_ROK) {
			log.warn("Paramater Error in Submit Request,sequence={}", request
					.getSequenceNumber());
		}

		String msgId = null;
		if (request.getVersion().getVersionID() == 0x34) {
			if (authedCustms.getSMPPIsGenHexMsgId()) {
				int decMsgId = messageIdGenerator.generateHexID(moduleIndex);
				msgId = Integer.toHexString(decMsgId);
			} else {
				if(authedCustms.isSupportMaxInMsgID()) {
					msgId = messageIdGenerator.generateMaxDecID(moduleIndex);
				}else {
					msgId = messageIdGenerator.generateDecID(moduleIndex);
				}
				
			}
		} else {
			msgId = Integer.toHexString(messageIdGenerator
					.generateHexID(moduleIndex));
		}
		try {
			msg = smpp.createGmmsMessage((SubmitSM) request, msgId,
					authedCustms, session.getTransactionURI());

			if (msg != null && msg.getStatusCode() > 0
					&& status != Data.ESME_ROK) { // need change smpp status
													// code to gmms code
				msg.setStatus(SMPPRespStatus.getGmmsStatus(status));
			}
		} catch (Exception e) {
			log.warn("when create gmms message with SubmitSM occur error", e);
		}
		return msg;

	}

	public void handleSubmit_SM_Resp(Response response, GmmsMessage msg) {
		SubmitSMResp resp = (SubmitSMResp) response;
		Integer sequence = resp.getSequenceNumber();
		if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(msg
				.getMessageType())) {
			msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP);
			if (resp.getCommandStatus() != 0) {
				// not received the DeliverSMResp
				msg.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT
						.getCode());
			}
		} else {
			msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
			if (resp.getMessageId() != null && resp.getMessageId().length() > 0) {
				String outMsgID = resp.getMessageId();
				// added by Neal to parse HEX to DEC for SMPP 3.4
				try {
					if (this.smppVersion.getVersionID() == 52) {
						if (authedCustms.isChlSMPPMsgIDParse()) {
							outMsgID = Long
									.toString(Long.valueOf(outMsgID, 16));
						}
						if (authedCustms.getSMPPIsPadZero4SR()) {
							int lenDiff = 10 - outMsgID.length();
							if (lenDiff > 0) {
								for (int i = 0; i < lenDiff; i++) {
									outMsgID = "0" + outMsgID;
								}
							}
						}
					}
				} catch (Exception ex) {
					log.error(ex, ex);
				}
				msg.setOutMsgID(outMsgID);
			}

			msg.setOutTransID(Integer.toString(sequence));
			Map<Integer,Integer> submitNotPaidStatus = authedCustms.getSubmitNotPaidStatusMapping();
			// set state after submiting
			setCommandState(msg, resp, submitNotPaidStatus);
			msg.setAttachment(resp.getCommandStatus());
		}
		if(log.isInfoEnabled()){
			log.info(msg, "SubmitSM receivies response, pdu status:{}, and msg statuscode:{}",
				resp.getCommandStatus(),msg.getStatusCode());
		}
	}

	public GmmsMessage handleDeliver_SM4Client(PDU pdu) {
		GmmsMessage msg = null;
		DeliverSM request = (DeliverSM) pdu;

		String msgId = null;
		if (smppVersion.getVersionID() == 0x34) {
			if (authedCustms.getSMPPIsGenHexMsgId()) {
				msgId = Integer.toHexString(messageIdGenerator
						.generateHexID(moduleIndex));
			} else {
				msgId = messageIdGenerator.generateDecID(moduleIndex);
			}
		} else {
			try {
				if ("Skytel_MN".equalsIgnoreCase(authedCustms.getShortName()))
					msgId = Integer.toHexString(messageIdGenerator
							.generateHexID(moduleIndex) + 1000000000);
				else
					msgId = Integer.toHexString(messageIdGenerator
							.generateHexID(moduleIndex));
			} catch (NumberFormatException ex) {
				msgId = "ABCDEF01";
				log.error(ex, ex);
			}
		}

		try {
			// added to handle SMPP 3.3 dr by Neal 06.04.19
			if (smppVersion.getVersionID() != SMPPVersion.getDefaultVersion()
					.getVersionID()) {
				request.setVersion(smppVersion);
			}

			// if it's real deliver msg, it will be inserted into messagestore
			msg = smpp.createGmmsMessage((DeliverSM) request, msgId, session
					.getSourceSysID(), authedCustms, session
					.getTransactionURI());

			if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(msg
					.getMessageType())) { // deliver_report
				msg.setTransaction(session.getTransactionURI());
				if(log.isInfoEnabled()){
					log.info(msg, "Received DR,outmsgid={},status code={}" , msg.getOutMsgID(),msg.getStatusCode());
				}
			}
		} catch (WrongLengthOfStringException e) {
			log.error("MsgIdCreator created an invalid ID (" + msgId
					+ ") for SMPP SubmitResp.", e);
		} catch (Exception e) {
			log.error("When create gmms message occur error.", e);
		}

		return msg;

	}

	public void handleDeliver_SM_Resp(DeliverSMResp response, GmmsMessage msg) {
		DeliverSMResp resp = (DeliverSMResp) response;
		if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(msg.getMessageType())
				|| GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP.equalsIgnoreCase(msg.getMessageType())) {
			msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP);
			if (response.getCommandStatus() != 0) { // the status of resp is not
													// successful
				msg.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT
						.getCode());
			}
		} else {
			msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
			SMPPVersion ver = this.smppVersion;
			if (ver != null && ver.getVersionID() == 0x33) {
				String outMsgID = ((DeliverSMResp) response).getMessageId();
				msg.setOutMsgID(outMsgID);
			}

			GmmsStatus status = SMPPRespStatus.getGmmsStatus(response
					.getCommandStatus());
			msg.setAttachment(response.getCommandStatus());
			msg.setStatus(status);
		}
		if(log.isInfoEnabled()){
			log.info(msg, "DeliverSM receivies response, pdu status:{}, and msg statuscode:{}",
				resp.getCommandStatus(),msg.getStatusCode());
		}
	}

	/**
	 * mapping the smpp error code to King error code
	 * 
	 * @param msg
	 *            GmmsMessage
	 * @param response
	 *            SubmitSMResp
	 */
	private void setCommandState(GmmsMessage msg, Response response, Map<Integer,Integer> submitNotPaidStatus) {
		int errorCode = response.getCommandStatus();
		if (errorCode == 0) { // no error
			msg.setOutClientPull(false);
		}
		
		GmmsStatus status = SMPPRespStatus.getGmmsStatus(errorCode);
		msg.setStatus(status);
		if(submitNotPaidStatus!=null && !submitNotPaidStatus.isEmpty()) {
			Integer statuscode =  submitNotPaidStatus.get(errorCode);
			if(statuscode!=null && statuscode!=0) {
				GmmsStatus errstatus = GmmsStatus.getStatus(statuscode);
				msg.setStatus(errstatus);
			}		
		}
		
	}
	
	public static void main(String[] args) {
		System.out.println(Long.toString(Long.valueOf("22A6DFE8", 16)));
	}

}
