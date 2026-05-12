package com.king.gmms.protocol.tcp.peering20;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.protocol.smpp.util.Data;
import com.king.gmms.protocol.tcp.peering20.exception.NotEnoughDataInByteBufferException;
import com.king.gmms.protocol.tcp.peering20.exception.UnknownParameterIdException;
import com.king.gmms.util.BufferMonitor;
import com.king.gmms.util.charset.Convert;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.MessageIdGenerator;

public class CommandSubmit extends Request {
	private String msgId;
	private String sessionID;
	private int o_op = -1;
	private int r_op = -1;
	private int o_hub = -1;
	private int r_hub = -1;
	private int o_relay = -1;
	private int r_relay = -1;
	private String timeMark;
	private String timeExpiry;
	private String scheduleDeliveryTime;
	private String sender;
	private String recipient;
	private int data_coding;
	private byte[] content;
	private int deliveryReport;
	private String refNum;
	private int totalSegments;
	private int seqNum;
	private int milterActionCode;
	private int udhIndicator = 0;
	/**
	 * @deprecated
	 */
	private byte[] binaryContent;

	/**
	 * for 1.5way/ott to keep original value, CDR required
	 */
	private String original_sender;
	private String original_recipient;
	
	private int serviceTypeID = -1;

	private A2PCustomerManager ctm = gmmsUtility.getCustomerManager();

	public void setO_op(int o_op) {
		this.o_op = o_op;
	}

	public void setR_op(int r_op) {
		this.r_op = r_op;
	}

	public void setO_hub(int o_hub) {
		this.o_hub = o_hub;
	}

	public void setR_hub(int r_hub) {
		this.r_hub = r_hub;
	}

	public void setO_relay(int o_relay) {
		this.o_relay = o_relay;
	}

	public void setMsgId(String msgId) {
		this.msgId = msgId;
	}

	public void setRefNum(String refNum) {
		this.refNum = refNum;
	}

	public void setTotalSegments(int totalSegments) {
		this.totalSegments = totalSegments;
	}

	public void setSeqNum(int seqNum) {
		this.seqNum = seqNum;
	}

	public CommandSubmit() {
		this(Pdu.VERSION_2_0);
	}

	public CommandSubmit(int version) {
		if (header == null) {
			header = new PduHeader(version);
		}
		header.setCommandId(COMMAND_SUBMIT);
	}

	public void setR_relay(int r_relay) {
		this.r_relay = r_relay;
	}

	public void parsePduCommand(TcpByteBuffer buffer)
			throws NotEnoughDataInByteBufferException,
			UnsupportedEncodingException, UnknownParameterIdException {
		if (buffer == null) {
			return;
		}
		TcpByteBuffer tempBuffer = null;
		while (buffer.length() > 0) {
			int tag = buffer.removeBytesAsInt(1);
			int length = buffer.removeBytesAsInt(2);
			switch (tag) {
			case FIELD_MSGID:
				msgId = buffer.removeString(length);
				break;
			case FIELD_TIMEMARK:
				timeMark = buffer.removeString(length);
				break;
			case FIELD_TIMEEXPIRY:
				timeExpiry = buffer.removeString(length);
				break;
			case FIELD_SENDER:
				sender = buffer.removeString(length);
				break;
			case FIELD_RECIPIENT:
				recipient = buffer.removeString(length);
				break;
			case FIELD_SESSIONID:
				sessionID = buffer.removeString(length);
				break;
			case FIELD_OOPERATOR:
				o_op = buffer.removeBytesAsInt(length);
				break;
			case FIELD_ROPERATOR:
				r_op = buffer.removeBytesAsInt(length);
				break;
			case FIELD_OHUB:
				o_hub = buffer.removeBytesAsInt(length);
				break;
			case FIELD_RHUB:
				r_hub = buffer.removeBytesAsInt(length);
				break;
			case FIELD_ORELAY:
				o_relay = buffer.removeBytesAsInt(length);
				break;
			case FIELD_RRELAY:
				r_relay = buffer.removeBytesAsInt(length);
				break;
			case FIELD_DATACODING:
				data_coding = buffer.removeBytesAsInt(length);
				break;
			case FIELD_CONTENT:
				tempBuffer = buffer.removeBytes(length);
				if (tempBuffer != null) {
					content = tempBuffer.getBuffer();
				}
				break;
			case FIELD_NEED_DR:
				deliveryReport = buffer.removeBytesAsInt(length);
				break;
			case FIELD_SAR_ReferenceNumber:
				refNum = buffer.removeString(length);
				break;
			case FIELD_SAR_TotalSegments:
				totalSegments = buffer.removeBytesAsInt(length);
				break;
			case FIELD_SAR_SegementsSeqnum:
				seqNum = buffer.removeBytesAsInt(length);
				break;
			case FIELD_UDH_INDICATOR:
				udhIndicator = buffer.removeBytesAsInt(length);
				break;
			case FIELD_MILTERACTIONCODE:
				milterActionCode = buffer.removeBytesAsInt(length);
				break;
			case FIELD_BINARYCONTENT:
				tempBuffer = buffer.removeBytes(length);
				if (tempBuffer != null) {
					binaryContent = tempBuffer.getBuffer();
				}
				break;
			case FIELD_ORIGINAL_SENDER:
				original_sender = buffer.removeString(length);
				break;
			case FIELD_ORIGINAL_RECIPIENT:
				original_recipient = buffer.removeString(length);
				break;
			 case FIELD_SERVICETYPEID:
             	serviceTypeID = buffer.removeBytesAsInt(length);
                 break;
			 case FIELD_SCHEDULE_DELIVERY_TIME:
             	scheduleDeliveryTime = buffer.removeString(length);
             	break;
			default:
				log.warn("Cant find field with tag:{},len:{}", tag, length);
				buffer.removeBytes(length);
				break;
			}
		}
	}

	public GmmsMessage convertToMsg(BufferMonitor buffer) {
		if (header == null) {
			return null;
		}

		GmmsMessage msg = new GmmsMessage();
		try {
			msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT);
			msg.setMsgID(msgId);
			msg.setInMsgID(sessionID);
			msg.setOoperator(o_op);
			msg.setRoperator(r_op);
			msg.setOSsID(o_relay);
			msg.setRSsID(r_relay);
			if (o_hub > 0) {
				msg.setOA2P(o_hub);
			}
			if (r_hub > 0) {
				msg.setRA2P(r_hub);
				msg.setCurrentA2P(r_hub);
			}

			msg.setMilterActionCode(milterActionCode);
			TimeZone local = TimeZone.getDefault();
			if (timeMark != null) {
				Date gmtTimemark = dateFormat.parse(timeMark);
				msg.setTimeStamp(gmtTimemark);
				// long diff = local.getRawOffset();
				// if (local.inDaylightTime(gmtTimemark)) {
				// diff += local.getDSTSavings();
				// }
				// msg.setLocalTimeStamp(new Date(gmtTimemark.getTime() +
				// diff));
			}

			if (timeExpiry != null) {
				msg.setExpiryDate(dateFormat.parse(timeExpiry));
			}
			
			if (scheduleDeliveryTime != null) {
            	msg.setScheduleDeliveryTime(dateFormat.parse(scheduleDeliveryTime));
            }
			
			msg.setSenderAddress(sender);
			msg.setRecipientAddress(recipient);

			msg.setOriginalSenderAddr(original_sender);
			msg.setOriginalRecipientAddr(original_recipient);

			String charset = DataCoding.handleDatecoding(data_coding);
			msg.setContentType(charset);

			boolean hasUdh = udhIndicator == 1 ? true : false;

			TcpByteBuffer rawContentData = new TcpByteBuffer(content);
			int len = rawContentData.length();
			int udhLen = 0;
			if (hasUdh) {
				udhLen = rawContentData.read1ByteAsInt();
				byte[] udh = rawContentData.removeBytes(udhLen + 1).getBuffer();
				msg.setUdh(udh);
				len = len - udhLen - 1;

			}

			if (len == 0) {
				msg.setTextContent(null);
				msg.setMessageSize(0);
			} else {
				switch (data_coding) {
				case 2:
				case 4:
					msg.setMimeMultiPartData(rawContentData.removeBytes(len)
							.getBuffer());
					msg.setGmmsMsgType(GmmsMessage.AIC_MSG_TYPE_BINARY);
					msg.setContentType(GmmsMessage.AIC_MSG_TYPE_BINARY);
					msg.setMessageSize(len);
					break;
				case 0: {
					if (customerInfo.getSmsOptionIncomingGSM7bit() == 1
							|| customerInfo.getSmsOptionIncomingGSM7bit() == 2) {
						int fillBits = 0;
						if(hasUdh){							
							fillBits = (7 - (udhLen+1)*8%7)%7;
						}														 												
						this.handleGSM7bitMessageContent(msg, rawContentData.removeBytes(len).getBuffer(),
								customerInfo.getSmsOptionIncomingGSM7bit(),fillBits);
					} else {
						charset = GmmsMessage.AIC_CS_ASCII;
						String content = rawContentData.removeString(len, charset);
						msg.setTextContent(content);
						msg.setMessageSize(content.getBytes(charset).length);
					}					
					break;
				}
				default: {
					String content = rawContentData.removeString(len, charset);
					msg.setTextContent(content);
					msg.setMessageSize(content.getBytes(charset).length);
					break;
				}
				}
			}

			msg.setDeliveryReport(deliveryReport == 1 ? true : false);
			msg.setSarMsgRefNum(refNum);
			msg.setSarSegmentSeqNum(seqNum);
			msg.setSarTotalSegments(totalSegments);

			if (msg.getTimeStamp() == null) {
				Date now = new Date();
				// msg.setLocalTimeStamp(now);
				long diff = local.getRawOffset();
				if (local.inDaylightTime(now)) {
					diff += local.getDSTSavings();
				}
				msg.setTimeStamp(new Date(now.getTime() - diff));
			}
			if (msg.getCurrentA2P() <= 0) {
				msg.setCurrentA2P(ctm.getCurrentA2P());
			}
			
			if (serviceTypeID != -1 ) {
				msg.setServiceTypeID(serviceTypeID);
			}

		} catch (Exception e) {
			log.error(msg, e, e);
			return null;
		}
		return msg;
	}

	public TcpByteBuffer pduCommandToByteBuffer()
			throws NotEnoughDataInByteBufferException,
			UnsupportedEncodingException {

		TcpByteBuffer buffer = new TcpByteBuffer();

		appendParameterToBuffer(buffer, FIELD_MSGID, msgId);
		appendParameterToBuffer(buffer, FIELD_SESSIONID, sessionID);
		appendParameterToBuffer(buffer, FIELD_OOPERATOR, o_op);
		appendParameterToBuffer(buffer, FIELD_ROPERATOR, r_op);
		appendParameterToBuffer(buffer, FIELD_OHUB, o_hub);
		appendParameterToBuffer(buffer, FIELD_RHUB, r_hub);
		appendParameterToBuffer(buffer, FIELD_ORELAY, o_relay);
		appendParameterToBuffer(buffer, FIELD_RRELAY, r_relay);
		appendParameterToBuffer(buffer, FIELD_TIMEMARK, timeMark);
		appendParameterToBuffer(buffer, FIELD_TIMEEXPIRY, timeExpiry);
		appendParameterToBuffer(buffer, FIELD_SENDER, sender);
		appendParameterToBuffer(buffer, FIELD_RECIPIENT, recipient);
		appendParameterToBuffer(buffer, FIELD_UDH_INDICATOR, udhIndicator, 1);
		appendParameterToBuffer(buffer, FIELD_DATACODING, data_coding, 1);

		// if (udhIndicator > 0 || data_coding == 2 || data_coding == 4) {
		// appendParameterToBuffer(buffer, FIELD_BINARYCONTENT, binaryContent);
		// }
		//
		// if (data_coding != 2 && data_coding != 4) {
		// appendParameterToBuffer(buffer, FIELD_CONTENT, content);
		// }
		// appendParameterToBuffer(buffer, FIELD_BINARYCONTENT, binaryContent);
		appendParameterToBuffer(buffer, FIELD_CONTENT, content);

		appendParameterToBuffer(buffer, FIELD_NEED_DR, deliveryReport, 1);
		appendParameterToBuffer(buffer, FIELD_SAR_ReferenceNumber, refNum);
		appendParameterToBuffer(buffer, FIELD_SAR_TotalSegments, totalSegments,
				1);
		appendParameterToBuffer(buffer, FIELD_SAR_SegementsSeqnum, seqNum, 1);
		appendParameterToBuffer(buffer, FIELD_MILTERACTIONCODE,
				milterActionCode);
		appendParameterToBuffer(buffer, FIELD_ORIGINAL_SENDER, original_sender);
		appendParameterToBuffer(buffer, FIELD_ORIGINAL_RECIPIENT,
				original_recipient);
		appendParameterToBuffer(buffer, FIELD_SERVICETYPEID, serviceTypeID, 4);
        appendParameterToBuffer(buffer, FIELD_SCHEDULE_DELIVERY_TIME, scheduleDeliveryTime);

		return buffer;
	}

	public void convertFromMsg(GmmsMessage msg)
			throws NotEnoughDataInByteBufferException,
			UnsupportedEncodingException {
		if (msg == null) {
			return;
		}
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

		String msgId = msg.getMsgID();
		if (msgId == null) {
			msgId = MessageIdGenerator.generateCommonMsgID(msg.getCurrentA2P());
			this.msgId = msgId;
		} else {
			this.msgId = msg.getMsgID();
		}

		sessionID = msg.getOutMsgID();
		o_op = msg.getOoperator();
		r_op = msg.getRoperator();
		o_hub = msg.getOA2P();
		r_hub = msg.getRA2P();
		o_relay = msg.getOSsID();
		r_relay = msg.getRSsID();

		timeMark = dateFormat.format(msg.getTimeStamp());
		long expire = 0;
		long descend = Long.parseLong(gmmsUtility
				.getCommonProperty("DescendingTime")) * 60 * 1000; // The second
																	// A2P need
																	// before
																	// the first
																	// 10 mins.
		if (msg.getExpiryDate() != null) {
			expire = msg.getExpiryDate().getTime() - descend;
		} else {
			expire = (long) gmmsUtility.getExpireTimeInMinute() * 60 * 1000
					+ msg.getTimeStamp().getTime() - descend;
		}
		timeExpiry = dateFormat.format(new Date(expire));
		
		if (msg.getScheduleDeliveryTime() != null) {
        	scheduleDeliveryTime = dateFormat.format(msg.getScheduleDeliveryTime());
        }

		sender = msg.getSenderAddress();
		recipient = msg.getRecipientAddress();
		original_sender = msg.getOriginalSenderAddr();
		original_recipient = msg.getOriginalRecipientAddr();

		if (msg.getUdh() != null && msg.getUdh().length > 0) {
			udhIndicator = 1;
		}

		TcpByteBuffer tempBuffer = null;
		if (msg.getUdh() != null) {
			tempBuffer = new TcpByteBuffer(msg.getUdh());
		}

		if (GmmsMessage.AIC_MSG_TYPE_BINARY.equalsIgnoreCase(msg
				.getGmmsMsgType())) {
			if (tempBuffer == null) {
				tempBuffer = new TcpByteBuffer(msg.getMimeMultiPartData());
			} else {
				tempBuffer.appendBytes(new TcpByteBuffer(msg
						.getMimeMultiPartData()));
			}
			data_coding = 2;
			if (tempBuffer != null) {
				content = tempBuffer.getBuffer();
			}
		} else {
			//A2PCustomerInfo cst = ctm.getCustomerBySSID(msg.getRA2P());
			byte tempContent[] = null;
			String textContent = msg.getTextContent();
			if (msg.isGsm7bit()&&customerInfo!=null) {
				if (textContent != null ) {
					if(customerInfo.getSmsOptionOutgoingGSM7bit()==1){
						tempContent = Convert.convert2GSM(textContent);
						data_coding = 0;
					}else if(customerInfo.getSmsOptionOutgoingGSM7bit()==2){
						int fillBits = 0;
						if(udhIndicator==1){
							int udhLen = msg.getUdh().length;
							fillBits = (7 - (udhLen)*8%7)%7;
						}														 						
						tempContent = Convert.encode7bit(Convert.convert2GSM(textContent),fillBits);
						data_coding = 0;
					}else{
						tempContent = textContent.getBytes(msg.getContentType());
						data_coding = DataCoding.getDataCoding(msg.getContentType());
					}
					
				}
				
			} else {
				data_coding = DataCoding.getDataCoding(msg.getContentType());
				if (msg.getTextContent() != null) {
					tempContent = msg.getTextContent().getBytes(
							msg.getContentType());
				}
			}

			if (tempContent != null) {
				if (tempBuffer == null) {
					tempBuffer = new TcpByteBuffer(tempContent);
				} else {
					tempBuffer.appendBytes(new TcpByteBuffer(tempContent));
				}
			}
			if (tempBuffer != null) {
				content = tempBuffer.getBuffer();
			}
		}

		// if (tempBuffer != null) {
		// binaryContent = tempBuffer.getBuffer();
		// }

		milterActionCode = msg.getMilterActionCode();
		// force the next A2P generate DR
		// 20080806 Kevin.
		// if (!gmmsUtility.getCustomerManager().isRssidNotSupportDR(
		// msg.getRSsID())) {
		// deliveryReport = 1;
		// }
		int cA2P = msg.getCurrentA2P();
		int rA2P = msg.getRA2P();

		deliveryReport = 0;
		if (cA2P == rA2P
		// ||
		// gmmsUtility.getCustomerManager().vpOnSameGMD(cA2P, rA2P)
		) {
			// it is not peering. set the DR flag with Rssid configuration.
			if (!gmmsUtility.getCustomerManager().isRssidNotSupportDR(
					msg.getRSsID())) {
				deliveryReport = 1;
			}
		} else {
			// Peering. Not use now.
			if (!gmmsUtility.getCustomerManager().isRssidNotSupportDR(
					msg.getRSsID())
					|| msg.getDeliveryReport()) {
				deliveryReport = 1;
			}
		}

		if (msg.getSarTotalSeqments() > 1) {
			totalSegments = msg.getSarTotalSeqments();
			seqNum = msg.getSarSegmentSeqNum();
			refNum = msg.getSarMsgRefNum();
		}
		
		serviceTypeID = msg.getServiceTypeID();
		
		// log.info("PDU:" + this.toString());
	}

	public String toString() {
		return new StringBuffer("COMMAND_SUBMIT:").append("msgId:").append(
				msgId).append(",").append("sessionID:").append(sessionID)
				.append(",").append("o_op:").append(o_op).append(",").append(
						"r_op:").append(r_op).append(",").append("o_relay:")
				.append(o_relay).append(",").append("r_relay:").append(r_relay)
				.append(",").append("o_a2p:").append(o_hub).append(",").append(
						"r_a2p:").append(r_hub).append(",").append("sender:")
				.append(sender).append(",").append("recipient:").append(
						recipient).append(",").append("original_sender:")
				.append(original_sender).append(",").append(
						"original_recipient:").append(original_recipient)
				.append(",").append("timeMark:").append(timeMark).append(",")
				.append("datacoding:").append(data_coding).append(",").append(
						"refNum:").append(refNum)
				.append(",").append("udhIndicator:").append(udhIndicator)
				.append(",").append("ServiceTypeID:").append(serviceTypeID)
				.toString();
	}

	private void handleGSM7bitMessageContent(GmmsMessage msg,
			byte[] btContent, int smsOptionIncomingGSM7bit, int fillBits) throws Exception {

		boolean iso = false;
		boolean ucs2 = false;
		byte newContent[] = btContent;
		if (smsOptionIncomingGSM7bit == 2) {
			newContent = Convert.decodeGSM7Bit(newContent,fillBits);
		}
		int[] chars = new int[newContent.length];
		int i = 0;
		int j = 0;
		for (i = 0, j = 0; i < newContent.length; i++, j++) {
			// mapping ASCII characters
			int temp = newContent[i];
			int value = -1;
			if (temp < 128 && temp >= 0) {
				value = Convert.gsm2ASCII[temp];
				if (value == -1) { // mapping ISO-8859-1 characters
					value = Convert.gsm2ISO[temp];
					if (value == -1) { // mapping UCS2 characters
						if (temp < 28) {
							value = Convert.gsm2UCS2[temp];
							if (value == -1) {
								// mapping extended characters
								if (temp == 27) {
									int c = newContent[i + 1];
									switch (c) {
									case 10:
										value = 12;
										break;
									case 20:
										value = 94;
										break;
									case 40:
										value = 123;
										break;
									case 41:
										value = 125;
										break;
									case 47:
										value = 92;
										break;
									case 60:
										value = 91;
										break;
									case 61:
										value = 126;
										break;
									case 62:
										value = 93;
										break;
									case 64:
										value = 124;
										break;
									case 101:
										value = 8364;
										ucs2 = true;
										break;
									default:
										value = -1;
									}
									if (value == -1) {
										value = 63;
									} else {
										i++;
									}
								}else{
									value = 63;
								}
							} else if (!ucs2) {
								ucs2 = true;
							}// end ucs2
						} else {
							value = 63;
						}
					} else if (!iso) {
						iso = true;
					}// end iso
				}
			} else {
				value = 63;
			}
			chars[j] = value;
		}
		int tempContent[] = new int[j];
		System.arraycopy(chars, 0, tempContent, 0, j);
		String contentType = GmmsMessage.AIC_CS_ASCII;
		if (ucs2) {
			contentType = GmmsMessage.AIC_CS_UCS2;
		} else if (iso) {
			contentType = GmmsMessage.AIC_CS_ISO8859_1;
		}

		String textContent = new String(Convert.intToChar(tempContent));
		if (textContent.length() > Data.SM_MSG_LEN) { // as it is length of
			// database, one
			// char is length 1
			textContent = textContent.substring(0, Data.SM_MSG_LEN);
		}
		if (log.isTraceEnabled()) {
			log.trace("after convert 7-bit to {}, msg content is:{}",
					contentType, textContent);
		}
		msg.setContentType(contentType);
		msg.setTextContent(textContent);
		msg.setGsm7bit(true);
		msg.setMessageSize(textContent.getBytes(contentType).length);

	}

	protected Respond createResponse() {
		return null;
	}

	public int getServiceTypeID() {
		return serviceTypeID;
	}

	public void setServiceTypeID(int serviceTypeID) {
		this.serviceTypeID = serviceTypeID;
	}

	public void setScheduleDeliveryTime(String scheduleDeliveryTime) {
		this.scheduleDeliveryTime = scheduleDeliveryTime;
	}
}
