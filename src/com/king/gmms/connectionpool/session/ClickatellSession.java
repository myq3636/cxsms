package com.king.gmms.connectionpool.session;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

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
public class ClickatellSession extends HttpSession {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(ClickatellSession.class);
	private String m_strSessionId;
	private boolean delivAck = false;

	public ClickatellSession(A2PCustomerInfo info) {
		super(info);
	}

	/**
	 * appendData
	 * 
	 * @param message
	 *            GmmsMessage
	 * @return StringBuffer
	 * @todo Implement.gmms.connectionpool.session.HttpSession
	 *       method
	 */
	protected StringBuffer appendData(GmmsMessage message) {
		StringBuffer sbPostData = new StringBuffer();
		String senderAddr = message.getSenderAddress();
		String cliMsgId = message.getOutMsgID();
		String messageType = message.getGmmsMsgType();

		try {
			sbPostData.append("session_id=");
			sbPostData.append(m_strSessionId);
			sbPostData.append("&api_id=");
			sbPostData.append(super.sysId);
			sbPostData.append("&to=");
			sbPostData.append(message.getRecipientAddress());

			if (message.getExpiryDate() != null) {
				String expireoffset = Long.toString((long) ((message
						.getExpiryDate().getTime() - message.getTimeStamp()
						.getTime()) / (60 * 1000)));
				sbPostData.append("&validity=");
				sbPostData.append(expireoffset);
			}

			if (senderAddr != null) {
				sbPostData.append("&from=");
				sbPostData.append(senderAddr);
			}

			if (cliMsgId != null) {
				sbPostData.append("&climsgid=");
				sbPostData.append(cliMsgId);
			}

			if (messageType.equals(GmmsMessage.AIC_MSG_TYPE_BINARY)) { // text/binary
				String contentType = message.getContentType();
				if (contentType.indexOf(GmmsMessage.AIC_CT_NOKIA_CLI.substring(
						0, 9)) > -1) { // Nokia
					sbPostData.append("&msg_type=");
					if (contentType.equals(GmmsMessage.AIC_CT_NOKIA_OLOGO)) {
						sbPostData.append("SMS_NOKIA_OLOGO");
					} else if (contentType.equals(GmmsMessage.AIC_CT_NOKIA_CLI)) {
						sbPostData.append("SMS_NOKIA_GLOGO");
					} else if (contentType
							.equals(GmmsMessage.AIC_CT_NOKIA_PICTURE)) {
						sbPostData.append("SMS_NOKIA_PICTURE");
					} else if (contentType
							.equals(GmmsMessage.AIC_CT_NOKIA_RINGTONE)) {
						sbPostData.append("SMS_NOKIA_RINGTONE");
					} else if (contentType
							.equals(GmmsMessage.AIC_CT_NOKIA_RTTTL)) {
						sbPostData.append("SMS_NOKIA_RTTL");
					} else if (contentType
							.equals(GmmsMessage.AIC_CT_NOKIA_VCARD)) {
						sbPostData.append("SMS_NOKIA_VCARD");
					} else if (contentType
							.equals(GmmsMessage.AIC_CT_NOKIA_VCAL)) {
						sbPostData.append("SMS_NOKIA_VCAL");
					}
				} else { // EMS
					// modify by bruce for support binary message with udh.
					byte[] udh = message.getUdh();
					if (udh != null && udh.length > 0) { // has UDH
						sbPostData.append("&udh=");
						for (byte one : udh) {
							sbPostData.append(twoDigits(Integer
									.toHexString(one)));
						}
					}
				}
				sbPostData.append("&text=");
				byte[] content = message.getMimeMultiPartData();
				for (int i = 0; i < content.length; i++) {
					sbPostData
							.append(twoDigits(Integer.toHexString(content[i])));
				}
			} else { // text
				if (message.getContentType() == null
						|| "".equals(message.getContentType().trim())) {
					if(log.isInfoEnabled()){
						log.info(message, "The content type is null!");
					}
					message.setStatus(GmmsStatus.INVALID_MSG_FIELD);
					return null;
				} else {
					byte[] udh = message.getUdh();
					if (udh != null && udh.length > 0) { // has UDH
						sbPostData.append("&udh=");
						for (int i = 0; i < udh.length; i++) {
							sbPostData.append(twoDigits(Integer
									.toHexString(udh[i])));
						}
					}

					String content = null;
					if (message.getTextContent() != null) {
						content = message.getTextContent();
					} else if (udh != null && udh.length > 0) {
						content = " ";
					}

					if (content != null) {
						sbPostData.append("&text=");
						if (GmmsMessage.AIC_CS_ASCII.equalsIgnoreCase(message
								.getContentType())) {
							sbPostData.append(urlEncode(content.getBytes()));
						} else { // not ASCII, uses Unicode to handle it
							byte[] bt = content
									.getBytes(GmmsMessage.AIC_CS_UCS2);
							for (int i = 0; i < bt.length; i++) {
								sbPostData.append(twoDigits(Integer
										.toHexString(bt[i])));
							}
							sbPostData.append("&unicode=1");
						}
					}
				}
			}

			int priorityType = priorityType(message);
			if (priorityType != 3) {
				sbPostData.append("&queue=");
				sbPostData.append(Integer.toString(priorityType));
			}

			if (delivAck) {
				sbPostData.append("&deliv_ack=1");
			}

			return sbPostData;

		} catch (Exception e) {
			log.info(message, "Exception raised in append data ", e);
			message.setStatus(GmmsStatus.INVALID_MSG_FIELD);
			return null;
		}
	}

	private int priorityType(GmmsMessage message) {
		int i = 2;
		int priority = message.getPriority();
		switch (priority) {
		case -1:
			i = 1;
			break;
		case 1:
			i = 3;
			break;
		default:
			i = 2;
		}
		return i;
	}

	private static String twoDigits(String s) {
		if (s == null)
			return s;
		if (s.length() == 2)
			return s;
		if (s.length() == 1)
			return "0" + s;
		return s.substring(s.length() - 2);
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
		} else {
			log.warn(msg, "The SysId is null, return!");
			return result;
		}
		if(log.isTraceEnabled()){
			log.trace("AcctID={} PWD={} URL={} charset={}", systemId, password,
				serverAddress, charset);
			log.trace("apiId={}", super.sysId);
		}
		msg.setRSsID(ssId);
		msg.setOutClientPull(true);

		if (!connect()) {
			logFail(msg);
			return true;
		}

		if (msg.getMessageType().equals(GmmsMessage.MSG_TYPE_SUBMIT)) {
			deliverNewMessage(msg);
			msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
			if (!putGmmsMessage2RouterQueue(msg)) {
				if(log.isInfoEnabled()){
					log.info(msg, "Send response to core engine failed!");
				}
				result = false;
			} else {
				result = true;
			}
		} else if (msg.getMessageType().equals(
				GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY)) {
			deliveryReportQuery(msg);
			if(log.isInfoEnabled()){
				log.info(msg, "Received DR, outmsgid={},statuscode={}" , msg.getOutMsgID(), msg.getStatusCode());
			}
			msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY_RESP);
			if (!putGmmsMessage2RouterQueue(msg)) {
					log.info(msg, "Send response to core engine failed!");
				result = false;
			} else {
				result = true;
			}
		} else {
			if(log.isInfoEnabled()){
				log.info(msg, "Unknown Message Type:{}", msg.getMessageType());
			}
			msg.setStatus(GmmsStatus.UNKNOWN_ERROR);
			result = false;
		}

		return result;
	}

	public boolean connect() {
		return login(super.sysId, systemId, password);
	}

	/**
	 * @param api_id
	 *            String
	 * @param account
	 *            String
	 * @param passwd
	 *            String
	 * @return boolean
	 */
	private boolean login(String api_id, String account, String passwd) {
		try {
			log.trace("Logining to Clickatell Gateway ...");
			String strPostData = "api_id=" + api_id + "&user=" + account
					+ "&password=" + passwd;
			if (!submitToGateway("auth", strPostData, null)) {
				if(log.isInfoEnabled()){
					log.info("Login error :{}", m_strStatus);
				}
				return false;
			}
			if (m_nRetCode != 0) {
				if(log.isInfoEnabled()){
					log.info("Login failed,error code is:{}", m_strRetDescription);
				}
				return false;
			}
			m_strSessionId = m_strRetDescription;
    		if(log.isDebugEnabled()){
    			log.debug("Login successfully:{}", m_strRetDescription);
    		}
			return true;
		} catch (Exception e) {
			if (connection != null) {
				connection.disconnect();
				connection = null;
			}
			if(log.isInfoEnabled()){
				log.info("Exception raised in Login :", e);
			}
			return false;
		}
	}

	private void logFail(GmmsMessage message) {

		if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(message
				.getMessageType())) { // if submit
			message.setStatus(GmmsStatus.COMMUNICATION_ERROR);
			message.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
			if(log.isTraceEnabled()){
				log.trace(message, "set statuscode = {}" , message.getStatusCode());
			}
		} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY
				.equalsIgnoreCase(message.getMessageType())) { // if delivery
																// report query
			message
					.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY_RESP);
			message.setStatus(GmmsStatus.FAIL_QUERY_DELIVERREPORT);
			if(log.isTraceEnabled()){
				log.trace(message, "set statuscode = {}" , message.getStatusCode());
			}
		} else { // invalid message type
			if(log.isInfoEnabled()){
				log.info(message,
					"Unknown Message Type! when update the fail status");
			}
			message.setStatus(GmmsStatus.UNKNOWN_ERROR);
		}

		if (!putGmmsMessage2RouterQueue(message)) {
			if(log.isInfoEnabled()){
				log.info(message, "Send response to core engine failed!");
			}
		}

	}

	private boolean deliverNewMessage(GmmsMessage message) {
		boolean result = false;
		StringBuffer sbPostData = new StringBuffer();
		sbPostData = appendData(message);
		if (sbPostData != null) {
			if (!submitToGateway("sendmsg", sbPostData.toString(), null)) {
				if(log.isInfoEnabled()){
					log.info(message, "SMS sending error:{}" , m_strStatus);
				}
				// need error mapping
				message.setStatus(GmmsStatus.COMMUNICATION_ERROR);
			} else {
				// need to change status mapping logic ...
				if (m_nRetCode != 0) {
					dealRetCode(message, m_nRetCode);
				} else { // return_code = 0
					message.setOutMsgID(m_strRetDescription);
					message.setOutTransID(String.valueOf(System
							.currentTimeMillis()));
					message.setStatus(GmmsStatus.SUCCESS);
				}
				if(log.isInfoEnabled()){
					log.info(message, "Submit ok,response status:{}",message.getStatusCode());
				}
				result = true;
			} // end else
		}

		return result;
	}

	private void deliveryReportQuery(GmmsMessage message) {

		if (!query(message) || m_nRetCode != 0) {
			message.setStatus(GmmsStatus.FAIL_QUERY_DELIVERREPORT);
			return;
		}

		int nIndex = m_strRetDescription.indexOf("Status:");
		if (nIndex == -1) {
			if(log.isInfoEnabled()){
				log.info(message, "Querying Response parsing error!");
			}
			message.setStatus(GmmsStatus.UNKNOWN);
			return;
		}
		String status = m_strRetDescription.substring(nIndex + 7).trim();
		dealMsgStatus(message, Integer.parseInt(status));
		return;
	}

	private boolean query(GmmsMessage message) {
		try {
			 if(log.isTraceEnabled()){
				 log.trace(message, "Querying message!");
			 }
			String apiMsgId = message.getOutMsgID();
			String strPostData = "session_id=" + m_strSessionId + "&apimsgid="
					+ apiMsgId;
			if (!submitToGateway("querymsg", strPostData, null)) {
				if(log.isInfoEnabled()){
					log.info(message, "Query error:{}" , m_strStatus);
				}
				return false;
			}
			if(log.isTraceEnabled()){
				log.trace(message, "Query resp is: {}!" , m_strStatus);
			}
			return true;
		} catch (Exception e) {
			log.error(message, "Exception raised when Querying", e);
			return false;
		}
	}

	/**
	 * submitToGateway
	 * 
	 * @param command
	 *            String
	 * @param data
	 *            String
	 * @param cookie
	 *            String
	 * @return boolean
	 */
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
				resp = super.doPost(url, data, null,false);

			}
			return getRetCode(resp);
		} catch (IOException e) {
			log.debug(e, e);

			m_strStatus = "Exception raise in SubmitToGateway :"
					+ e.getMessage() + ")";
			return false;
		}
	}

	/**
	 * getRetCode
	 * 
	 * @param strResponse
	 *            String
	 * @return boolean
	 */
	protected boolean getRetCode(String strResponse) {
		String[] response = parseResponse(strResponse);
		if (response.length != 2) {
			m_strStatus = "Response parsing error :" + strResponse + ")";
			return false;
		}
		if ("ERR".equalsIgnoreCase(response[0].trim())) {
			m_strStatus = strResponse;
			m_strRetDescription = response[1];
			m_nRetCode = Integer.parseInt(response[1].split(",")[0].trim());
			return true;
		} else if ("ID".equalsIgnoreCase(response[0].trim())
				|| "OK".equalsIgnoreCase(response[0].trim())) {
			m_nRetCode = 0;
			m_strStatus = strResponse;
			m_strRetDescription = response[1];
			lastActivity = System.currentTimeMillis();
			return true;
		} else {
			m_strStatus = "Incorrect resp from Clickatell.";
			return false;
		}
	}

	private String[] parseResponse(String response) {
		String[] resp = new String[2];
		int nIndex = response.indexOf(":");
		if (nIndex == -1) {
			return resp;
		}
		resp[0] = response.substring(0, nIndex).trim();
		resp[1] = response.substring(nIndex + 1).trim();

		return resp;
	}

	private void dealMsgStatus(GmmsMessage message, int retCode) {
		switch (retCode) {
		case 1:
			message.setStatus(GmmsStatus.ENROUTE);
			break;
		case 2:
			message.setStatus(GmmsStatus.ENROUTE);
			break;
		case 3:
			message.setStatus(GmmsStatus.DELIVERED);
			break;
		case 4:
			message.setStatus(GmmsStatus.DELIVERED);
			break;
		case 5:
			message.setStatus(GmmsStatus.REJECTED);
			break;
		case 6:
			message.setStatus(GmmsStatus.DELETED);
			break;
		case 7:
			message.setStatus(GmmsStatus.UNDELIVERABLE);
			break;
		case 8:
			message.setStatus(GmmsStatus.DELIVERED);
			break;
		case 9:
			message.setStatus(GmmsStatus.UNDELIVERABLE);
			break;
		case 10:
			message.setStatus(GmmsStatus.EXPIRED);
			break;
		case 11:
			message.setStatus(GmmsStatus.ENROUTE);
			break;
		case 12:
			message.setStatus(GmmsStatus.FAIL_QUERY_DELIVERREPORT);
			break;
		default: // Unkonwn Return Code
			log.warn(message, "Unknown return code:{}" , m_nRetCode);
			message.setStatus(GmmsStatus.UNKNOWN);
		} // end switch
	}

	private void dealRetCode(GmmsMessage message, int retCode) {
		switch (retCode) {
		case 1:
			message.setStatus(GmmsStatus.AUTHENTICATION_ERROR);
			break;
		case 2:
			message.setStatus(GmmsStatus.AUTHENTICATION_ERROR);
			break;
		case 3:
			message.setStatus(GmmsStatus.AUTHENTICATION_ERROR);
			break;
		case 4:
			message.setStatus(GmmsStatus.AUTHENTICATION_ERROR);
			break;
		case 5:
			message.setStatus(GmmsStatus.AUTHENTICATION_ERROR);
			break;
		case 101:
			message.setStatus(GmmsStatus.INVALID_MSG_FIELD);
			break;
		case 102:
			message.setStatus(GmmsStatus.INVALID_MSG_FIELD);
			break;
		case 103:
			message.setStatus(GmmsStatus.INVALID_MSG_FIELD);
			break;
		case 104:
			message.setStatus(GmmsStatus.INVALID_MSG_FIELD);
			break;
		case 105:
			message.setStatus(GmmsStatus.RECIPIENT_ADDR_ERROR);
			break;
		case 106:
			message.setStatus(GmmsStatus.SENDER_ADDR_ERROR);
			break;
		case 107:
			message.setStatus(GmmsStatus.INVALID_MSG_FIELD);
			break;
		case 108:
			message.setStatus(GmmsStatus.INVALID_MSG_FIELD);
			break;
		case 109:
			message.setStatus(GmmsStatus.INVALID_MSG_FIELD);
			break;
		case 110:
			message.setStatus(GmmsStatus.INVALID_MSG_FIELD);
			break;
		case 111:
			message.setStatus(GmmsStatus.INVALID_MSG_FIELD);
			break;
		case 112:
			message.setStatus(GmmsStatus.INVALID_MSG_FIELD);
			break;
		case 113:
			message.setStatus(GmmsStatus.INVALID_MSG_FIELD);
			break;
		case 114:
			message.setStatus(GmmsStatus.UNDELIVERED);
			break;
		case 115:
			message.setStatus(GmmsStatus.UNDELIVERED);
			break;
		case 116:
			message.setStatus(GmmsStatus.INVALID_MSG_FIELD);
			break;
		case 201:
			message.setStatus(GmmsStatus.SERVER_ERROR);
			break;
		case 202:
			message.setStatus(GmmsStatus.SERVER_ERROR);
			break;
		case 301:
			message.setStatus(GmmsStatus.SERVER_ERROR);
			break;
		case 302:
			message.setStatus(GmmsStatus.SERVER_ERROR);
			break;

		default: // Unkonwn Return Code
			if(log.isInfoEnabled()){
				log.info(message, "Unknown return code:{}" , m_nRetCode);
			}
			message.setStatus(GmmsStatus.UNKNOWN_ERROR);
		} // end switch
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
