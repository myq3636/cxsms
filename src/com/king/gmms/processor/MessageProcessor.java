package com.king.gmms.processor;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.customerconnectionfactory.InternalAgentConnectionFactory;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.milter.AntiSpamMilter;
import com.king.gmms.protocol.commonhttp.HttpUtils;
import com.king.gmms.util.charset.Convert;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

public class MessageProcessor {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(MessageProcessor.class);
	protected GmmsUtility gmmsUtility;
	protected InternalAgentConnectionFactory factory = null;
	private A2PCustomerManager ctm;
	private static final String SUFFIX_DEMO = "(1/1)";

	public MessageProcessor() {
		gmmsUtility = GmmsUtility.getInstance();
		ctm = gmmsUtility.getCustomerManager();
		factory = InternalAgentConnectionFactory.getInstance();
	}

	public boolean antiSpam(GmmsMessage msg, A2PCustomerInfo customerInfo) {
		if (customerInfo.isSupportOutgoingAntiSpam() && !msg.hasKeywordFilter()) {
			if (AntiSpamMilter.getInstance().checkAntiSpam(
					customerInfo.getSSID(), msg, false)) {
				if(log.isInfoEnabled()){
					log.info(msg, "reject send msg as rssid antispam.");
				}
				return true;
			}
			msg.setKeywordFilter(true);
		}
		return false;
	}

	public boolean antiBinary(GmmsMessage msg, A2PCustomerInfo server) {
		if (msg.getGmmsMsgType().equals(GmmsMessage.AIC_MSG_TYPE_BINARY)) {
			if (!server.isSupportOutgoingBinary()) {
				if(log.isInfoEnabled()){
					log.info(msg,"{} doesn't support binary SMS, so this SMS is terminated."
							     ,server.getServerID());
				}
				return true;
			}
		}
		return false;
	}

	public LinkedList<GmmsMessage> processBinaryOrTextMessage(
			GmmsMessage message, A2PCustomerInfo cst) {
		LinkedList<GmmsMessage> msgList = new LinkedList<GmmsMessage>();
		// judge r_op is support UDH, if not support, set UDH is null
		handleUdhInfo(message);

		// judge o_op and r_channel are supported to split
		if (ctm.allowSplit(message)) {			
			int max_length = getRopSupportLength(message);													
			int udh_length = this.getSplitMessageUdhLength(message, message
					.getRSsID(), max_length);

			if(message.isGsm7bit() && cst.getSmsOptionOutgoingGSM7bit()==2){
				//recalculate message max length:					
				max_length = max_length /8*7 +max_length %8; 
			}else if(udh_length!=0 && cst.isSmsOptionIsMaxASCIILenTo7BitFlag()){
				if((GmmsMessage.AIC_CS_ASCII.equalsIgnoreCase(message.getContentType()))
						|| (message.isGsm7bit() && cst.getSmsOptionOutgoingGSM7bit()==1)){
					//recalculate message max length:
					int ropBits = max_length%8; // it means there is how much bit does the message have
            		int udhBits = (7 - udh_length*8%7)%7;// it means there is how much bit does the UDH need
            		int rest = ropBits-udhBits>=0?0:1;// whether needs to allocate 1 byte for UDH
            		max_length =  max_length - rest ;
				}
			}
			
			int double_character_length = 0;
			if(message.isGsm7bit()){
				int a[] = Convert.charToint(message.getTextContent().toCharArray());
				for(int i = 0;i<a.length;i++){					
					// calculate the bytes count on the assumption that this
					switch(a[i]){
					//if double bytes;
					case 12:  double_character_length++; break;
					case 94:  double_character_length++; break;
					case 123: double_character_length++;  break;
					case 125: double_character_length++; break;
					case 92:  double_character_length++; break;
					case 91:  double_character_length++; break;
					case 126: double_character_length++; break;
					case 93:  double_character_length++;  break;
					case 124: double_character_length++;  break;
					case 8364:double_character_length++; break;
					default:;											
					}	
				}
				int mlength = 0;
				try {
					mlength = message.getTextContent().getBytes(message.getContentType()).length;
				} catch (Exception e) {
					log.warn(message, "get message length error!");
					message.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
					return msgList;
				}
				if(cst.getSmsOptionOutgoingGSM7bit()==1){					
					if(GmmsMessage.AIC_CS_UCS2.equalsIgnoreCase(message.getContentType())){
						message.setMessageSize(mlength/2+double_character_length);
					}else{
						message.setMessageSize(mlength+double_character_length);
					}
				}else if(cst.getSmsOptionOutgoingGSM7bit()==2){//todo
					if(GmmsMessage.AIC_CS_UCS2.equalsIgnoreCase(message.getContentType())){				
						int rest = (mlength/2+double_character_length)*7%8>0?1:0;
						int length = (mlength/2+double_character_length)*7/8 +rest; 
						message.setMessageSize(length);
					}else{
						int rest = (mlength+double_character_length)*7%8>0?1:0;
						int length = (mlength+double_character_length)*7/8 +rest; 
						message.setMessageSize(length);
					}
				}
			}
			if ((message.getMessageSize() + udh_length) > max_length) {
				// print log
			if(log.isDebugEnabled()){
					log.debug(message, "This message is allowed to be splitted, its size is {} and the ROP only support {}, and udh length is {}, so it's going to be splitted.",
						message.getMessageSize(), max_length, udh_length);
				}
				// message split
				GmmsMessage[] messages = null;
				if (GmmsMessage.AIC_MSG_TYPE_TEXT.equalsIgnoreCase(message
						.getGmmsMsgType())) {					
					messages = splitTextMessage(message, udh_length, max_length);
				} else if (GmmsMessage.AIC_MSG_TYPE_BINARY
						.equalsIgnoreCase(message.getGmmsMsgType())) {
					messages = splitBinaryMessage(message, udh_length);
				}
				if(log.isInfoEnabled()){
					log.info(message, "Been splitted to {}", messages.length);
				}
				String refNum;
				// judge current message has reference number
				if (message.getSarMsgRefNum() == null || message.getSarMsgRefNum().trim().length() == 0) {
					// construct reference number; 6.5 Optimization: use ThreadLocalRandom
					refNum = HttpUtils.format2Digits(Integer.toHexString(
							ThreadLocalRandom.current().nextInt(127))); // [0,127)

				} else {
					refNum = message.getSarMsgRefNum();
				}

				GmmsMessage msg = null;

				// r_channel support UDH: that means next hop supports message
				// with UDH
				if (!ctm.isNotSupportUDH(message.getRSsID())
						&& cst.isUdhConcatenated()) {
					if (message.getUdh() != null) { // update split message UDH
						for (int i = 0; i < messages.length; i++) {
							msg = messages[i];
							this.updateSplitMsg(msg, refNum, messages.length,
									i + 1, msg.getUdh());
							msgList.offer(msg);
						}
					} else {// construct new UDH
						for (int i = 0; i < messages.length; i++) {
							msg = messages[i];
							// the default value is 8 bit
							this.constructSplitMsgUDH(msg, refNum,
									messages.length, i + 1);
							msgList.offer(msg);
						}
					}
				} else {
					// set split message with 3 optional parameters and message
					// UDH is null
					for (int i = 0; i < messages.length; i++) {
						// set msgRefNum,totalSegments and segmentSeqNum
						msg = messages[i];
						msg.setSarMsgRefNum(refNum);
						msg.setSarSegmentSeqNum(i + 1);
						msg.setSarTotalSegments(messages.length);
						msgList.offer(msg);
					}
				}
			} else {// message length+udh_length < max_length

				msgList.offer(message);
			}
		} else {// message does not support split
			msgList.offer(message);
		}
		return msgList;

	}

	public void constructSplitMsgUDH(GmmsMessage msg, String refNum,
			int totalSegNum, int segmentSeqNum) {

		byte[] refNumByte = HttpUtils.getBytesByHexString(refNum);

		byte[] udh = null;
		if (refNumByte.length <= 1) {
			udh = new byte[6];
			udh[0] = 0x05;
			udh[1] = 0x00; // Concentrated message identifier
			udh[2] = 0x03; // UDH length
			udh[3] = refNumByte[0];
			udh[4] = (byte) totalSegNum;
			udh[5] = (byte) segmentSeqNum;
		} else {
			udh = new byte[7];
			udh[0] = 0x06;
			udh[1] = 0x08; // Concentrated message identifier
			udh[2] = 0x04; // UDH length
			udh[3] = refNumByte[0];
			udh[4] = refNumByte[1];
			udh[5] = (byte) totalSegNum;
			udh[6] = (byte) segmentSeqNum;
		}

		msg.setUdh(udh);
		// set 3 optional parameters:
		msg.setSarMsgRefNum(refNum);
		msg.setSarTotalSegments(totalSegNum);
		msg.setSarSegmentSeqNum(segmentSeqNum);
	}

	public void updateSplitMsg(GmmsMessage msg, String refNum, int totalSegNum,
			int segmentSeqNum, byte[] udh_byte) {

		// add a new element which is used to store concatenation
		int i = udh_byte.length;
		byte[] refNumByte = HttpUtils.getBytesByHexString(refNum);

		byte[] udh_target = null;
		if (refNumByte.length == 1) {
			udh_target = new byte[i + 5];
			System.arraycopy(udh_byte, 0, udh_target, 0, i);
			udh_target[i] = 0x00;
			udh_target[i + 1] = 0x03; // UDH length
			udh_target[i + 2] = refNumByte[0];
			udh_target[i + 3] = (byte) totalSegNum;
			udh_target[i + 4] = (byte) segmentSeqNum;
			// update UDH length
			udh_target[0] = (byte) (udh_target[0] + (byte) 5);
		} else if (refNumByte.length == 2){
			udh_target = new byte[i + 6];
			System.arraycopy(udh_byte, 0, udh_target, 0, i);
			udh_target[i] = 0x08;
			udh_target[i + 1] = 0x04; // UDH length
			udh_target[i + 2] = refNumByte[0];
			udh_target[i + 3] = refNumByte[1];
			udh_target[i + 4] = (byte) totalSegNum;
			udh_target[i + 5] = (byte) segmentSeqNum;
			// update UDH length
			udh_target[0] = (byte) (udh_target[0] + (byte) 6);
		}

		msg.setUdh(udh_target);
		// set 3 optional parameters:
		msg.setSarMsgRefNum(refNum);
		msg.setSarTotalSegments(totalSegNum);
		msg.setSarSegmentSeqNum(segmentSeqNum);
	}

	private void handleUdhInfo(GmmsMessage message) {

		if (message != null) {
			if (message.getUdh() != null
					&& ctm.isNotSupportUDH(message.getRSsID())) {
				if(log.isInfoEnabled()){
					log.info("rssid:{} is not support udh set null.", message
						.getRSsID());
				}

				message.setUdh(null);
			}
		} else {
			if(log.isDebugEnabled()){
				log.debug("message is null while handleUdhInfo!");
			}
		}
	}

	public void convertCharset(GmmsMessage message) {
		try {
			// Charset conversion
			int ssid = 0;
			if (!ctm.inCurrentA2P(message.getRA2P())) {
				ssid = message.getRA2P();
			} else {
				ssid = message.getRSsID();
			}
			int rop = message.getRoperator();
			List<String> ropRequiredCharsets = ctm.getRequiredCharsets(ssid,
					rop);
			A2PCustomerInfo cm = ctm.getCustomerBySSID(ssid);
			if(message.isGsm7bit()&&cm!=null){
				if(cm.getSmsOptionOutgoingGSM7bit()==0){
					if(cm.getSmsOptionForceTrans2ASCII()){
						message.setContentType(GmmsMessage.AIC_CS_ASCII);
						message.setGsm7bit(false);
						message.setMessageSize(message.getTextContent().getBytes(GmmsMessage.AIC_CS_ASCII).length);
						return ;
					}else{
						message.setGsm7bit(false);
						message.setMessageSize(message.getTextContent().getBytes(message.getContentType()).length);
					}
				}else {					
					return ;
				}
			}else if(cm!=null){
				if(GmmsMessage.AIC_CS_ASCII.equalsIgnoreCase(message.getContentType())){
						//||GmmsMessage.AIC_CS_ISO8859_1.equalsIgnoreCase(message.getContentType())
					    //remove ISO8859-1 convert to 7bit msg
					if(cm.getSmsOptionOutgoingGSM7bit()!=0&&cm.getSmsOptionConvert2GSM7bit()){
						message.setGsm7bit(true);
						message.setContentType(GmmsMessage.AIC_CS_ASCII);
						return ;
					}
				}
			}
			if (ropRequiredCharsets != null
					&& !ropRequiredCharsets.contains(message.getContentType())
					&& ((message.getCurrentA2P() == message.getRA2P()) || ctm.vpOnSameA2P(message.getCurrentA2P(), message.getRA2P()))) {
				// do charset convertion based on Roperator,if need do
				// peering,ignore this
				String contentType = ropRequiredCharsets.get(0);
				int length = message.getTextContent().getBytes(contentType).length;
				if(log.isInfoEnabled()){
					log.info(message, "Change charset from {} to {}, and change size from {} to {}.",
						message.getContentType(), contentType, message.getMessageSize(), length);
				}
				message.setContentType(contentType);
				message.setMessageSize(length);
			} else if (ropRequiredCharsets == null
					|| (message.getCurrentA2P() != message.getRA2P())
					|| !ctm.vpOnSameA2P(message.getCurrentA2P(), message.getRA2P())) {
				// do charset convertion based on channel,if need do
				// peering,only based on channel charset.
				List<String> supportedCharsets = ctm.getCustomerBySSID(ssid)
						.getSupportedCharsets();
				if (supportedCharsets != null
						&& !supportedCharsets
								.contains(message.getContentType())) {
					String contentType = supportedCharsets.get(0);
					int length = message.getTextContent().getBytes(contentType).length;
					if(log.isInfoEnabled()){
						log.info(message, "Change charset from {} to {}, and change size from {} to {}.",
							message.getContentType(), contentType, message.getMessageSize(), length);
					}
					message.setContentType(contentType);
					message.setMessageSize(length);
				}
			}
		} catch (Exception e) {
			log.error(message, e.getMessage());
		}
	}

	private int getSplitMessageUdhLength(GmmsMessage message, int rssid,
			int max_length) {//todo

		int udh_length = 0;
		A2PCustomerInfo cst = ctm.getCustomerBySSID(message.getRSsID());
        //get the msg size, 2022-12-15
		int double_character_length = 0;
		//for filter double character. 
		if(message.getMessageSize()>150 &&
				(message.isGsm7bit() || GmmsMessage.AIC_CS_ASCII.equalsIgnoreCase(message.getContentType()))){
			int a[] = Convert.charToint(message.getTextContent().toCharArray());
			for(int i = 0;i<a.length;i++){					
				// calculate the bytes count on the assumption that this
				switch(a[i]){
				//if double bytes;
				case 12:  double_character_length++; break;
				case 94:  double_character_length++; break;
				case 123: double_character_length++;  break;
				case 125: double_character_length++; break;
				case 92:  double_character_length++; break;
				case 91:  double_character_length++; break;
				case 126: double_character_length++; break;
				case 93:  double_character_length++;  break;
				case 124: double_character_length++;  break;
				case 8364:double_character_length++; break;
				default:;											
				}	
			}
		}
		int mlength = 0;
		try {
			mlength = message.getTextContent().getBytes(message.getContentType()).length;
		} catch (Exception e) {
			log.warn(message, "get message length error!");
			mlength = message.getMessageSize();
		}
		// set UDH length:
		if (mlength+double_character_length > max_length) {
			if ((!ctm.isNotSupportUDH(message.getRSsID()))
					&& cst.isUdhConcatenated()) {
				// udh length depend on original refNum, refNum is set to
				// hexString in last step
				if (message.getSarMsgRefNum() != null
						&& message.getSarMsgRefNum().trim().length() > 2) {
					if (message.getUdh() == null) {
						udh_length = 7;
					} else {
						udh_length = message.getUdh().length + 6;

						if (udh_length >= max_length) {
							message.setUdh(null);
							udh_length = 7;
						}
					}
				} else {
					if (message.getUdh() == null) {
						udh_length = 6;
					} else {
						udh_length = message.getUdh().length + 5;

						if (udh_length >= max_length) {
							message.setUdh(null);
							udh_length = 6;
						}
					}
				}

			}
		} else {
			if (message.getUdh() == null) {
				return 0;
			} else {
				udh_length = message.getUdh().length;
				if ((!ctm.isNotSupportUDH(message.getRSsID()))
						&& cst.isUdhConcatenated()) {
					// udh length depend on original refNum, refNum is set to
					// hexString in last step
					if (message.getSarMsgRefNum() != null
							&& message.getSarMsgRefNum().trim().length() > 2) {
						udh_length += 6;
					} else {
						udh_length += 5;
					}

				}
				if (udh_length >= max_length) {
					message.setUdh(null);
					udh_length = 0;
				}
			}
		}		
		return udh_length;
	}

	/**
	 * added by Neal for Forword 2006/01/19
	 * 
	 * @param message
	 *            GmmsMessage
	 */
	public void addForeword(GmmsMessage message, A2PCustomerInfo cst) {
		byte[] udh = message.getUdh();
		if (udh != null && udh.length > 0) // if has udh, don't do add Foreword
			return;

		String foreword = cst.getForeword();

		String content = message.getTextContent();
		if (foreword != null && !"no".equalsIgnoreCase(foreword)) {
			String modifiedContent = content;
			if ("All".equalsIgnoreCase(foreword)) {
				modifiedContent = "+" + message.getSenderAddress() + " says: "
						+ content;
			} else if (gmmsUtility.getCustomerManager().getServiceNameBySsid(
					message.getOSsID()).equalsIgnoreCase(foreword)) {
				modifiedContent = "+" + message.getSenderAddress() + " says: "
						+ content;
			}else {
				modifiedContent = foreword+ content;
			}
			try {
				if (modifiedContent.getBytes(message.getContentType()).length <= ctm
						.getRopSupportLength(message)) {
					if(log.isDebugEnabled()){
						log.debug(message,"Foreword is added to the message with InMsgId:{}", message.getInMsgID());
					}
				}				
				message.setMessageSize(modifiedContent.getBytes(message
						.getContentType()).length);
				message.setTextContent(modifiedContent);
				message.setContentIsChanged(true);
				message.setAddForeword(true);
			} catch (Exception e) {
				log.error(e, e);
			}
		}
	}

	public void addOttContentOaddr(GmmsMessage message, A2PCustomerInfo cst) {

		String ottContentAddOaddr = cst.getOttContentAddOaddr();

		String content = message.getTextContent();
		if (ottContentAddOaddr != null
				&& !"no".equalsIgnoreCase(ottContentAddOaddr)) {
			try {
				String originalSenderAddr = message.getOriginalSenderAddr();
				if (originalSenderAddr != null
						&& originalSenderAddr.trim().length() > 0) {
					String modifiedContent = originalSenderAddr + ": "
							+ content;

					message.setMessageSize(modifiedContent.getBytes(message
							.getContentType()).length);
					message.setTextContent(modifiedContent);
					message.setContentIsChanged(true);
					message.setOttContentAddOaddr(true);
					if(log.isDebugEnabled()){
						log.debug(message,
							"OriginalSenderAddr is added to the message");
					}
				}
			} catch (Exception e) {
				log.error(e, e);
			}
		}
	}
	
	public void addContentSignature(GmmsMessage message) {
		try {
			Map<String, String> signatureMap = ctm.getContentSignatureMap(message);
			if (signatureMap != null && signatureMap.keySet().size() > 0) {
				
				// get the first one in the map
				String sig_charset = signatureMap.keySet().iterator().next();
				String signature = signatureMap.get(sig_charset);

				if (signature != null) {
					// since almost all charset is compatible with ASCII(ANSI X3.4), 
					// So no need to change content type if sig_charset is ASCII
					if (GmmsMessage.AIC_CS_UCS2.equalsIgnoreCase(sig_charset)) {
						if (!GmmsMessage.AIC_CS_UCS2.equalsIgnoreCase(message.getContentType())) {
							if (log.isDebugEnabled()) {
								log.debug(message, "ContentType changed from {} to {} when addContentSignature", message.getContentType(), sig_charset);
							}
							message.setContentType(sig_charset);
						}
						
						if(message.isGsm7bit()){
							message.setGsm7bit(false);
						}
					}
						
					String modifiedContent = message.getTextContent() + signature;

					message.setMessageSize(modifiedContent.getBytes(message.getContentType()).length);
					message.setTextContent(modifiedContent);
					message.setContentIsChanged(true);
					message.setContentSignature(true);
					
					if (log.isDebugEnabled()) {
						if (GmmsMessage.AIC_CS_UCS2.equalsIgnoreCase(sig_charset)) {
							log.debug(message, "ContentSignature {} is added to the message", GmmsUtility.convert2HexFormat(signature));
						} else {
							log.debug(message, "ContentSignature {} is added to the message", signature);
							
						}
					} // end log
				} // end signature
			}
			
		} catch (Exception e) {
			log.error("Add ContentSignature error", e);
		}
	}
	
	
	public void addTemplateSignature(GmmsMessage message, String signaturekey) {
		try {
			Map<String, String> signatureMap = ctm.getContentTpl().getSignatureContentMaps(signaturekey);
			if (signatureMap != null && signatureMap.keySet().size() > 0) {
				
				// get the first one in the map
				String sig_charset = signatureMap.keySet().iterator().next();
				String signature = signatureMap.get(sig_charset);

				if (signature != null) {
					// since almost all charset is compatible with ASCII(ANSI X3.4), 
					// So no need to change content type if sig_charset is ASCII
					if (GmmsMessage.AIC_CS_UCS2.equalsIgnoreCase(sig_charset)) {
						if (!GmmsMessage.AIC_CS_UCS2.equalsIgnoreCase(message.getContentType())) {
							if (log.isDebugEnabled()) {
								log.debug(message, "ContentType changed from {} to {} when addContentSignature", message.getContentType(), sig_charset);
							}
							message.setContentType(sig_charset);
						}
						
						if(message.isGsm7bit()){
							message.setGsm7bit(false);
						}
					}
						
					String modifiedContent = message.getTextContent() + signature;

					message.setMessageSize(modifiedContent.getBytes(message.getContentType()).length);
					message.setTextContent(modifiedContent);
					message.setContentIsChanged(true);
					message.setContentSignature(true);
					
					if (log.isDebugEnabled()) {
						if (GmmsMessage.AIC_CS_UCS2.equalsIgnoreCase(sig_charset)) {
							log.debug(message, "ContentSignature {} is added to the message", GmmsUtility.convert2HexFormat(signature));
						} else {
							log.debug(message, "ContentSignature {} is added to the message", signature);
							
						}
					} // end log
				} // end signature
			}
			
		} catch (Exception e) {
			log.error("Add ContentSignature error", e);
		}
	}

	private GmmsMessage[] splitTextMessage(GmmsMessage message, int udhlen, int max_length) {
		try {
			List<GmmsMessage> messages = new LinkedList<GmmsMessage>();
			boolean suffix = ctm.supportSplitSuffix(message);
			String contentType = message.getContentType();
			String content = message.getTextContent();
			// the tempSplitResults holds the splitted contents in StringBuilder
			// format for further disposing
			List<StringBuilder> tempSplitResults = new LinkedList<StringBuilder>();
			// the sb holds the current content in disposing
			StringBuilder sb = new StringBuilder(content.length());
			int i = 0, currentBytes = 0;
			// the actual limit should be the difference of the CCB value and
			// the SUFFIX length
			int limit = 0;
			if(message.isGsm7bit()){				
				int ropLength = max_length - udhlen;
				int fillBits = 0;
				if(udhlen!=0){
					fillBits = (7 - (udhlen)*8%7)%7;
				}				
				int suffLength = suffix ? 5 : 0;				
				int gmsEncode = ctm.getCustomerBySSID(message.getRSsID()).getSmsOptionOutgoingGSM7bit();					
				boolean flag =false;
				int value =1;
				if(gmsEncode==1){
					limit = ropLength - suffLength;				
				}else if(gmsEncode==2){					
					limit = ropLength + (ropLength-fillBits)/7 -suffLength;   //todo
				}				
				for (int t = 0; t < content.length(); t++) {
					// dispose every character
					int c = (int)content.charAt(t);
					// calculate the bytes count on the assumption that this
					// characters has been appended to sb
					switch(c){
					//if double bytes;
					case 12: flag=true; value++; break;
					case 94: flag=true; value++; break;
					case 123: flag=true; value++;  break;
					case 125: flag=true; value++; break;
					case 92: flag=true; value++; break;
					case 91: flag=true; value++; break;
					case 126: flag=true; value++; break;
					case 93: flag=true; value++;  break;
					case 124: flag=true; value++;  break;
					case 8364: flag=true; value++; break;
					default: flag = false;											
					}						
					if (value > limit) {
						// the currentBytes overflow, so complete current sb and
						// reset all the counters
						tempSplitResults.add(sb);
						value = 0;
						sb = new StringBuilder(content.length());
						t--;
					}else {												
						// append this character into sb
					    sb.append((char)c);													
					}
					value++;
				}
				tempSplitResults.add(sb);
			}else{
				limit = max_length
				- (suffix ? SUFFIX_DEMO.getBytes(contentType).length : 0)
				- udhlen;
				for (i = 0; i < content.length(); i++) {
					// dispose every character
					String oneChar = String.valueOf(content.charAt(i));
					// calculate the bytes count on the assumption that this
					// characters has been appended to sb
					currentBytes = currentBytes
							+ oneChar.getBytes(contentType).length;
					if (currentBytes > limit) {
						// the currentBytes overflow, so complete current sb and
						// reset all the counters
						tempSplitResults.add(sb);
						sb = new StringBuilder(content.length());
						currentBytes = 0;
						i--;
					} else {
						// append this character into sb
						sb.append(oneChar);
					}
				}
				// complete the last sb
				tempSplitResults.add(sb);
			}						
			i = 0;
			StringBuilder sbb = null;
			int messageCount = tempSplitResults.size();
			for (int j = 0; j < tempSplitResults.size(); j++) {
				sbb = tempSplitResults.get(j);
				if (suffix) {
					// if the split suffix is required, append it
					sbb.append("(").append(++i).append("/")
							.append(messageCount).append(")").toString();
				}

				if (j == 0) {// the first split message:
					message.setTextContent(sbb.toString());
					message.setMessageSize(message.getTextContent().getBytes(
							contentType).length);
					message.setSplitStatus(j + 1);
					messages.add(message);
				} else { // split messages from second
					GmmsMessage newMsg = new GmmsMessage(message);
					newMsg.setOriginalQueue(null);
					newMsg.setSplitStatus(j + 1);
					newMsg.setTextContent(sbb.toString());
					newMsg.setMessageSize(newMsg.getTextContent().getBytes(
							contentType).length);
					messages.add(newMsg);
				}
			}// end of for

			return messages.toArray(new GmmsMessage[0]);
		} catch (Exception e) {
			log.error(message,
					"Exception occurs during splitting text message", e);
			message.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
			return new GmmsMessage[0];
		}
	}

	private GmmsMessage[] splitBinaryMessage(GmmsMessage message, int udhlen) {
		try {
			List<GmmsMessage> messages = new LinkedList<GmmsMessage>();
			byte[] content = message.getMimeMultiPartData();

			int i = 0, splitStatus = 1;
			int limit = ctm.getRopSupportLength(message) - udhlen;
			byte[] splitContent = new byte[limit];

			while (i < content.length) {
				if (i <= content.length - limit) {
					System.arraycopy(content, i, splitContent, 0, limit);
					this.setMessageList(message, messages, splitContent,
							splitStatus);
				} else {

					break;
				}
				splitContent = new byte[limit];
				i += limit;
				splitStatus++;
			}

			// complete the last split message
			if (content.length - i > 0) {

				splitContent = new byte[content.length - i];
				System.arraycopy(content, i, splitContent, 0, content.length
						- i);
				this.setMessageList(message, messages, splitContent,
						splitStatus);
			}

			return messages.toArray(new GmmsMessage[0]);
		} catch (Exception e) {
			log.error(message,
					"Exception occurs during splitting binary message", e);
			message.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
			return new GmmsMessage[0];
		}
	}

	public void setMessageList(GmmsMessage message, List<GmmsMessage> messages,
			byte[] splitContent, int splitStatus) {

		if (splitStatus == 1) {
			message.setSplitStatus(splitStatus);
			message.setMimeMultiPartData(splitContent);
			message.setMessageSize(message.getMimeMultiPartData().length);
			messages.add(message);
		} else {
			GmmsMessage newMsg = new GmmsMessage(message);
			newMsg.setOriginalQueue(null);
			newMsg.setSplitStatus(splitStatus);
			newMsg.setMimeMultiPartData(splitContent);
			newMsg.setMessageSize(newMsg.getMimeMultiPartData().length);
			messages.add(newMsg);
		}

	}

	protected int getRopSupportLength(GmmsMessage msg) {
		return ctm.getRopSupportLength(msg);
	}
}
