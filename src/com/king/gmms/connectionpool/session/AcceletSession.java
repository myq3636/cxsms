package com.king.gmms.connectionpool.session;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.king.framework.*;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageIdGenerator;

import java.io.UnsupportedEncodingException;
import java.net.URL;

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
public class AcceletSession extends HttpSession {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(AcceletSession.class);
	private String cell = "01";

	public AcceletSession(A2PCustomerInfo info) {
		super(info);
		if (gmmsUtility != null) {
			cell = gmmsUtility.getModuleProperty("OptionCell", "01");
		}
	}

	/**
	 * submit
	 * 
	 * @param msg
	 *            GmmsMessage
	 * @return boolean
	 * @throws IOException
	 * @todo Implement.gmms.connectionpool.session.Session
	 *       method
	 */
	public boolean submit(GmmsMessage msg) throws IOException {
		boolean result = false;

		serverAddress = verifyUrl(serverAddress);
		if (serverId.size() != 0 && serverId.get(0).toString() != null) {
			sysId = serverId.get(0).toString();
		}

		if(log.isTraceEnabled()){
			log.trace("AcctID={} PWD={} URL={} charset={}", systemId, password,
				serverAddress, charset);
			log.trace("apiId={}", super.sysId);
		}
		msg.setRSsID(ssId);

		if (GmmsMessage.MSG_TYPE_SUBMIT.equals(msg.getMessageType())) {
			result = deliverNewMessage(msg);
		} else {
			if(log.isInfoEnabled()){
				log.info(msg, "Unknown Message Type:{}" , msg.getMessageType());
			}
			msg.setStatus(GmmsStatus.UNKNOWN_ERROR);
		}

		return result;
	}

	private boolean deliverNewMessage(GmmsMessage message) {
		boolean result = false;
		try {
			StringBuffer sbPostData = new StringBuffer();
			message.setOutTransID(String.valueOf(System.currentTimeMillis()));
			message.setOutMsgID(MessageIdGenerator
					.generateCommonOutMsgID(message.getRSsID()));

			sbPostData = appendData(message);

			if (!submitToGateway("send2.jsp", sbPostData.toString(), null)) {
				if(log.isInfoEnabled()){
					log.info(message, "SMS sending error :{}" , m_strStatus);
				}
				// need error mapping
				message.setStatus(GmmsStatus.COMMUNICATION_ERROR);
			} else {
				if (m_nRetCode != 0) {
					message.setStatus(GmmsStatus.UNKNOWN_ERROR);
				} else { // return_code = 0
					message.setStatus(GmmsStatus.SUCCESS);
				}
				if(log.isInfoEnabled()){
					log.info(message, "Submit ok,status= {}",message.getStatusCode());
				}
				result = true;
			} // end else
		} catch (Exception e) {
			log.warn(message, "Exception raised in Send", e);
			message.setStatus(GmmsStatus.UNKNOWN_ERROR);
			result = true;
		} finally {
			message.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
			if (!putGmmsMessage2RouterQueue(message)) {
				if(log.isInfoEnabled()){
					log.info(message, "Send response to core engine failed!");
				}
			}
		}

		return result;
	}

	protected boolean submitToGateway(String command, String data, String cookie) {
		try {
			String resp = null;
			URL url = new URL(serverAddress + command);
			if (httpMethod.equalsIgnoreCase("get")) {
				URL url1 = new URL(serverAddress + command + "?" + data);
					log.debug("http get mothed send url: ");
				resp = super.doGet(url1,false);
			} else {
				log.debug("http post mothed send url: ");
				resp = super.doPost(url, data, cookie,false);

			}
			return getRetCode(resp);
		}

		catch (IOException e) {
			m_strStatus = "Exception raise in SubmitTo Accelet Gateway :"
					+ e.getMessage() + ")";
			log.warn(m_strStatus, e);
			return false;
		}
	}

	protected boolean getRetCode(String strResponse) throws IOException {

		if (strResponse == null) {
			return false;
		}
		int i = -1;
		try {
			i = Integer.parseInt(strResponse.trim());
		} catch (Exception e) {
			log.debug(e, e);
			i = -1;
		}
		m_nRetCode = i;
		return true;
	}

	protected StringBuffer appendData(GmmsMessage message) {
		StringBuffer sbPostData = new StringBuffer();
		String toAddress = message.getRecipientAddress();

		try {
			sbPostData.append("circle=");
			sbPostData.append(urlEncode(systemId));
			sbPostData.append("&pwd=");
			sbPostData.append(urlEncode(password));
			sbPostData.append("&service=");
			sbPostData.append("BZ");
			sbPostData.append("&cell=");
			sbPostData.append(cell);
			sbPostData.append("&mobile=");
			sbPostData.append(urlEncode(removeCountryCode(toAddress)));
			sbPostData.append("&mtype=");
			sbPostData.append("XXXF");
			sbPostData.append("&message=");
			sbPostData.append(urlEncode(message.getTextContent()));
			sbPostData.append("&msgid=");
			sbPostData.append(urlEncode(message.getOutMsgID().trim()));
			return sbPostData;
		} catch (UnsupportedEncodingException e) {
			log.error(message, "Exception raised in append data", e);
			return null;
		}
	}

	/**
	 * Clean up phone number format by stripping off all charactors other
	 * 
	 * @param phoneNumber
	 *            String
	 * @return String
	 */
	protected String removeCountryCode(String phoneNumber) {
		if (phoneNumber == null) {
			return null;
		}
		if (phoneNumber.startsWith("86")) {
			phoneNumber = phoneNumber.substring(2);
		}
		return phoneNumber;
	}

	/**
	 * submitAndRec
	 * 
	 * @param msg
	 *            GmmsMessage
	 * @return ByteBuffer
	 * @throws IOException
	 * @todo Implement.gmms.connectionpool.session.Session
	 *       method
	 */
	public ByteBuffer submitAndRec(GmmsMessage msg) throws IOException {
		return null;
	}
}
