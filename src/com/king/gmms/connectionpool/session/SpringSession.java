package com.king.gmms.connectionpool.session;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.nio.ByteBuffer;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageIdGenerator;

/**
 * 
 * @author willzhang
 * @version A2P 1.1.0
 */
public class SpringSession extends HttpSession {
	private static String SPRINGMAXLENGTH = "MaxLength";
	protected int maxLength = 138;

	private static SystemLogger log = SystemLogger
			.getSystemLogger(SpringSession.class);

	public SpringSession(A2PCustomerInfo info) {
		super(info);
		init();
	}

	private void init() {
		try {
			maxLength = Integer.valueOf(gmmsUtility.getModuleProperty(
					SPRINGMAXLENGTH, "138"));
		} catch (NumberFormatException e) {
			log.error("maxLength error!", e);
		}
	}

	// public SpringSession(String serverAddress,String sysId, String pwd){
	// super(serverAddress,sysId,pwd);
	// }

	@Override
	public boolean submit(GmmsMessage msg) throws IOException {
		msg.setOutMsgID(MessageIdGenerator.generateCommonOutMsgID(msg
				.getRSsID()));
		boolean bret = false;
		String post = this.appendData(msg).toString();

		String resp = null;
		try {
			// serverAddress = verifyUrl(serverAddress);
			URL url = new URL(serverAddress);			
			if (httpMethod.equalsIgnoreCase("get")) {
				URL url1 = new URL(serverAddress + "?" + post);
				if(log.isDebugEnabled()){
	        		log.debug("http get mothed send url: ");
				}
				resp = super.doGet(url1,false);
			} else {
				if(log.isDebugEnabled()){
	        		log.debug("http post mothed send url: ");
				}
				resp = super.doPost(url, post, null,false);

			}
			bret = true;
			if(log.isDebugEnabled()){
        		log.debug(msg, "resp = {},bret = {}" , resp , bret);
			}
			this.dealResp(msg, resp);
		} catch (IOException e) {
			msg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
			bret = true;
			log.error(msg, "send msg error!", e);
		} catch (Exception e) {
			msg.setStatus(GmmsStatus.UNKNOWN_ERROR);
			bret = true;
			log.error(msg, "send msg error!", e);
		}
		
		if(msg.getMessageType().equalsIgnoreCase(GmmsMessage.MSG_TYPE_SUBMIT)){
			msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
		}else if(msg.getMessageType().equalsIgnoreCase(GmmsMessage.MSG_TYPE_DELIVERY_REPORT)){
			msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		}else if(msg.getMessageType().equalsIgnoreCase(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY)){
			msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY_RESP);
		}
		if(!putGmmsMessage2RouterQueue(msg)){
			if(log.isInfoEnabled()){
				log.info(msg,"Send response to core engine failed!");
			}
		}
		return bret;
	}

	public void dealResp(GmmsMessage msg, String resp) {
		if (resp == null) {
			return;
		}
		String respStr = resp.trim();
		if ("OK".equalsIgnoreCase(respStr)) {
			msg.setStatus(GmmsStatus.SUCCESS);
		} else if ("INVALID_PHONE".equalsIgnoreCase(respStr)) {
			msg.setStatus(GmmsStatus.RECIPIENT_ADDR_ERROR);
		} else if ("NOACCESS".equalsIgnoreCase(respStr)) {
			msg.setStatus(GmmsStatus.SERVER_ERROR);
		} else if ("MISSING_PARAMETERS".equalsIgnoreCase(respStr)) {
			msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
		} else if ("ERROR".equalsIgnoreCase(respStr)) {
			msg.setStatus(GmmsStatus.SERVER_ERROR);
		} else if ("INTERNAL_ERROR".equalsIgnoreCase(respStr)) {
			msg.setStatus(GmmsStatus.SERVER_ERROR);
		} else if ("BLACKLISTED".equalsIgnoreCase(respStr)) {
			msg.setStatus(GmmsStatus.POLICY_DENIED);
		} else if ("NOT_ALLOWED".equalsIgnoreCase(respStr)) {
			msg.setStatus(GmmsStatus.POLICY_DENIED);
		} else if ("HTTP_NOT_FOUND".equalsIgnoreCase(respStr)) {
			msg.setStatus(GmmsStatus.UNKNOWN_ERROR);
		} else {
			msg.setStatus(GmmsStatus.UNKNOWN_ERROR);
		}
	}

	@Override
	public ByteBuffer submitAndRec(GmmsMessage msg) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected StringBuffer appendData(GmmsMessage message) {
		StringBuffer postData = new StringBuffer();
		try {
			postData.append("phone=").append(
					urlEncode(message.getRecipientAddress()));
			postData.append("&msgid=").append(urlEncode(message.getOutMsgID()));
			postData.append("&user=").append(urlEncode(super.systemId));
			postData.append("&pwd=").append(urlEncode(super.password));
			String messageText = message.getTextContent();
			if (messageText != null) {
				if (!message.getContentType().equalsIgnoreCase(
						GmmsMessage.AIC_CS_ASCII)) {
					messageText = new String(messageText.getBytes(message
							.getContentType()), GmmsMessage.AIC_CS_ASCII);
					int length = messageText.getBytes(GmmsMessage.AIC_CS_ASCII).length;
					message.setMessageSize(length);
				}
			}
			message.setContentType(GmmsMessage.AIC_CS_ASCII);
			if (messageText != null && messageText.length() > maxLength) {
				messageText = messageText.substring(0, maxLength);
			}
			postData.append("&msgtext=").append(urlEncode(messageText));
			postData.append("&senderid=").append(urlEncode(super.senderId));
			// postData.append("&msgtext=").append(
			// message.getTextContent());
		} catch (UnsupportedEncodingException e) {
			log.error(message, "urlEncode Error!", e);
		} catch (Exception e) {
			log.error(message, "", e);
		}
		if(log.isDebugEnabled()){
			log.debug("postData = {}", postData.toString());
		}
		
		return postData;
	}

	public static void main(String[] args) {
		// DOMConfigurator.configure("D:\\jars\\log4j-config.xml");
		// String url = "http://192.168.23.18:8080/spring/spring";
		// String systemId = "Dacom.gmms";
		// String password = "kdj8w3s";
		// String senderAddress = "8613312345678";
		// String recipientAddress = "82163301127";
		// String textContent = "te s  t";
		//
		// String outMsgId = Integer.toHexString(MessageIdGenerator
		// .generateHexID(1));
		// GmmsMessage gmmsMsg = new GmmsMessage();
		// gmmsMsg.setSenderAddress(senderAddress);
		// gmmsMsg.setRecipientAddress(recipientAddress);
		// gmmsMsg.setTextContent(textContent);
		// gmmsMsg.setGmmsMsgType(GmmsMessage.AIC_MSG_TYPE_TEXT);
		// gmmsMsg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT);
		// gmmsMsg.setDeliveryReport(true);
		// gmmsMsg.setContentType(GmmsMessage.AIC_CS_ASCII);
		// gmmsMsg.setOutMsgID(outMsgId);
		// SpringSession ss = new SpringSession(url,systemId,password);
		// ss.charset = "ASCII";
		// try {
		// ss.submit(gmmsMsg);
		// } catch (IOException e) {
		// e.printStackTrace();
		// }

	}

}
