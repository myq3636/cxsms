package com.king.gmms.processor;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.message.gmms.GmmsMessage;

/**
 * Concatenated SMS utility
 * 
 * @author bensonchen
 * @version 1.0.0
 */
public class CsmUtility {

	private static SystemLogger log = SystemLogger.getSystemLogger(CsmUtility.class);

	public static String CSM_ID_SEPERATOR = ",";

	/**
	 * generate CsmKeyInfo from GmmsMessage
	 * 
	 * @param message
	 * @return
	 */
	public static CsmKeyInfo getCsmKeyInfoFromGmmsMessage(
			final GmmsMessage message) {
		CsmKeyInfo csmKeyInfo = new CsmKeyInfo();
		csmKeyInfo.setoSsID(message.getOSsID());
		csmKeyInfo.setSenderAddress(message.getSenderAddress());
		csmKeyInfo.setRecipientAddress(message.getRecipientAddress());
		csmKeyInfo.setSarMsgRefNum(message.getSarMsgRefNum());
		csmKeyInfo.setSarTotalSegments(message.getSarTotalSeqments());
		return csmKeyInfo;
	}

	/**
	 * generate CsmValueInfo from GmmsMessage
	 * 
	 * @param message
	 * @return
	 */
	public static CsmValueInfo getCsmValueInfoFromGmmsMessage(
			final GmmsMessage message) {
		CsmValueInfo csmValueInfo = new CsmValueInfo();
		csmValueInfo.setSarSegmentSeqNum(message.getSarSegmentSeqNum());
		csmValueInfo.setTextContent(message.getTextContent());
		csmValueInfo.setMimeMultiPartData(message.getMimeMultiPartData());
		csmValueInfo.setInMsgID(message.getInMsgID());
		csmValueInfo.setMsgID(message.getMsgID());
		csmValueInfo.setMessageSize(message.getMessageSize());
		csmValueInfo.setTimeStamp(message.getTimeStamp());
		csmValueInfo.setContentType(message.getContentType());
		return csmValueInfo;
	}

	/**
	 * assemble concatenated messages
	 * 
	 * @param csmValueInfoSet
	 * @param message
	 * @return
	 * @throws UnsupportedEncodingException
	 */
	public static GmmsMessage assembleCsm(
			CsmValueInfoMark csmValueInfoMark, final GmmsMessage message)
			throws UnsupportedEncodingException {
		if(log.isDebugEnabled()){
    		log.debug(message, "Before assembleCsm: {}" , message);
		}
		GmmsMessage retMsg = new GmmsMessage(message);
		
		Set<CsmValueInfo> csmValueInfoSet = csmValueInfoMark.getValueSet();
		StringBuilder textContent = new StringBuilder(1600);
		StringBuilder inMsgId = new StringBuilder(200);

		String msgType = message.getGmmsMsgType();

		int totalMessageSize = 0;
		if (GmmsMessage.AIC_MSG_TYPE_BINARY.equalsIgnoreCase(msgType)) {
			for (CsmValueInfo item : csmValueInfoSet) {
				totalMessageSize = totalMessageSize + item.getMessageSize();
			}
			byte[] mimeMultiPartData = new byte[totalMessageSize];
			int lastDestPos = 0;
			for (CsmValueInfo item : csmValueInfoSet) {
				// assemble binary content
				byte[] src = item.getMimeMultiPartData();
				System.arraycopy(src, 0, mimeMultiPartData, lastDestPos,
						src.length);
				lastDestPos = lastDestPos + src.length;

				// assemble inMsgId, MsgId
				inMsgId.append(item.getInMsgID()).append(CSM_ID_SEPERATOR);
			}
			// set content and size
			retMsg.setMimeMultiPartData(mimeMultiPartData);
			// set MessageSize
			retMsg.setMessageSize(totalMessageSize);
		} else if (GmmsMessage.AIC_MSG_TYPE_TEXT.equalsIgnoreCase(msgType)) {			
			if(retMsg.isGsm7bit()){
				boolean isUCS = false;
				boolean isISO = false;
				for(CsmValueInfo item : csmValueInfoSet){
					if(GmmsMessage.AIC_CS_UCS2.equalsIgnoreCase(item.getContentType())){						
						isUCS = true;
					}else if(GmmsMessage.AIC_CS_ISO8859_1.equalsIgnoreCase(item.getContentType())){						
						isISO = true;
					}
				}
				if(isUCS){
					retMsg.setContentType(GmmsMessage.AIC_CS_UCS2);
				}else if(isISO) {					
					retMsg.setContentType(GmmsMessage.AIC_CS_ISO8859_1);					
				}
			}
			for (CsmValueInfo item : csmValueInfoSet) {
				// assemble text content				
				textContent.append(item.getTextContent());				
				// assemble inMsgId
				inMsgId.append(item.getInMsgID()).append(CSM_ID_SEPERATOR);
			}
			// set content
			A2PCustomerManager ctm = GmmsUtility.getInstance()
					.getCustomerManager();
			String lastHopSplitSuffixFormat = ctm.getCustomerBySSID(
					message.getOSsID()).getLastHopSplitSuffixFormat();
			if (lastHopSplitSuffixFormat != null
					&& lastHopSplitSuffixFormat.trim().length() > 0) {
				// replace LastHopSplitSuffixFormat. e.g. \(\d:\d\), \(\d/\d\)
				Pattern pattern = Pattern.compile(lastHopSplitSuffixFormat);
				Matcher matcher = pattern.matcher(textContent.toString());
				String replacedContent = matcher.replaceAll("");
				retMsg.setTextContent(replacedContent);
				 if(log.isTraceEnabled()){
					 log.trace(message,
						"After replace lastHopSplitSuffixFormat content=: {}", replacedContent);
				 }
			} else {
				retMsg.setTextContent(textContent.toString());
			}
			// set MessageSize
			retMsg.setMessageSize(retMsg.getTextContent().getBytes(
					message.getContentType()).length);
		}

		// set InMsgID MsgID
		retMsg.setInMsgID(inMsgId.substring(0, inMsgId.length()
				- CSM_ID_SEPERATOR.length()));

		// reset sar_total_segments, sar_segment_seqnum, don't reset
		// sar_msg_ref_num, client split message may use it again
		retMsg.setSarSegmentSeqNum(0);
		retMsg.setSarTotalSegments(0);

		//set the DR Status
		retMsg.setDeliveryReport(csmValueInfoMark.isDeliverReport());
		
		if(log.isInfoEnabled()){
			log.info(retMsg, "After assembleCsm: {}" , retMsg);
		}
		return retMsg;
	}

	/**
	 * check whether the message is a concatenated message
	 * 
	 * @param msg
	 * @return
	 */
	public static boolean isConcatenatedMsg(final GmmsMessage msg) {
		if (msg.getSarMsgRefNum() != null
				&& msg.getSarMsgRefNum().trim().length() > 0
				&& msg.getSarTotalSeqments() > 0
				&& msg.getSarSegmentSeqNum() > 0) {
			return true;
		}
		return false;
	}

	/**
	 * Split concatenated SMS first DR according inMsgId, MsgId
	 * 
	 * @param message
	 * @param msgList
	 */
	public static void splitCsmDr(GmmsMessage message, List<GmmsMessage> msgList) {
		try {
			String inMsgId = message.getInMsgID();
			// split ID
			String[] inMsgIdArray = inMsgId.split(CsmUtility.CSM_ID_SEPERATOR);
			for (int i = 0; i < inMsgIdArray.length; i++) {
				GmmsMessage tempDrMsg = new GmmsMessage(message);
				// set in_msgId, msg_Id, segment_seqnum, totalSegments
				tempDrMsg.setInMsgID(inMsgIdArray[i]);
				tempDrMsg.setSarSegmentSeqNum(i + 1);
				tempDrMsg.setSarTotalSegments(inMsgIdArray.length);

				msgList.add(tempDrMsg);
			}
		} catch (Exception e) {
			log.error(message, e.toString());
		}
	}

	public static String short2Hex(short value) {
		if (value >= 0 && value <= 255) {
			return Integer.toHexString(value & 0x00FF | 0xFF00).toUpperCase()
					.substring(2, 4);
		} else {
			return Integer.toHexString(value & 0x00FFFF | 0xFF0000)
					.toUpperCase().substring(2, 6);
		}
	}
	
	public static boolean isValidCsms(GmmsMessage message) {
		// add check, in case of cc 02 01; cc 02 03; cc 02 02
		if (message.getSarSegmentSeqNum() > 0
				&& message.getSarSegmentSeqNum() <= message
						.getSarTotalSeqments()) {
			return true;
		}
		return false;
	}

}
