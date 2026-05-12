package com.king.gmms.milter;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.king.framework.SystemLogger;
import com.king.gmms.*;
import com.king.gmms.domain.*;
import com.king.message.gmms.*;

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
public class ContentScanMilter extends LocalMilter {
	private A2PCustomerManager gcm = super.gmmsUtility.getCustomerManager();
	private Map<Integer, ArrayList<ContentScan>> contentScanInfo;
	private static ContentScanMilter m_instance = null;

	private ContentScanMilter() {
		log = SystemLogger.getSystemLogger(LocalMilter.class.getName());
		contentScanInfo = gcm.getContentScanInfo();
	}

	public synchronized static ContentScanMilter getInstance() {
		if (m_instance == null) {
			try {
				m_instance = new ContentScanMilter();
			} catch (Exception e) {
				log.error("cann't create one ContentScanMilter instance!", e);
			}
		}
		return m_instance;
	}

	/**
	 * processGMMS
	 * 
	 * @param msg
	 *            GmmsMessage
	 * @return GmmsMessage
	 */
	public GmmsMessage processGMMS(GmmsMessage msg) {
		try {
			int ossid = msg.getOSsID();
			if (ossid <= 0 || contentScanInfo == null
					|| !contentScanInfo.containsKey(ossid))
				return msg;

			ArrayList<ContentScan> contentScan = contentScanInfo.get(ossid);

			Iterator iterator = contentScan.iterator();
			while (iterator.hasNext()) {
				ContentScan cs = (ContentScan) iterator.next();
				if (msg.getRecipientAddress().startsWith(cs.getRPrefix())
						&& msg.getContentType().equalsIgnoreCase(
								cs.getOCharset())) {
					scanMsg(msg, cs.getTransferedCharset());
					break;
				}
			}
		} catch (Exception ex) {
			log.error(ex, ex);
		} finally {
			return msg;
		}
	}

	/**
	 * 
	 * @param msg
	 *            GmmsMessage
	 * @param trasncodeCharset
	 *            String
	 * @return boolean
	 */
	private boolean scanMsg(GmmsMessage msg, String trasncodeCharset) {
		if (GmmsMessage.AIC_CS_UCS2.equalsIgnoreCase(msg.getContentType())) {// Unicode
			return scanMsgForUnicode(msg, trasncodeCharset);
		} else if (GmmsMessage.AIC_MSG_TYPE_BINARY.equalsIgnoreCase(msg
				.getContentType())) {// binary
			return scanMsgForBinary(msg, trasncodeCharset);
		} else {// other charset
			log
					.warn(
							"We can't support original charset: {} when content scaning.",
							msg.getContentType());
			return false;
		}
	}

	/**
	 * 
	 * @param msg
	 *            GmmsMessage
	 * @param trasncodeCharset
	 *            String
	 * @return boolean
	 */
	private boolean scanMsgForUnicode(GmmsMessage msg, String trasncodeCharset) {
		boolean hasNoOtherChar = true;
		if (!GmmsMessage.AIC_CS_ASCII.equalsIgnoreCase(trasncodeCharset)
				&& !GmmsMessage.AIC_CS_GBK.equalsIgnoreCase(trasncodeCharset)
				&& !GmmsMessage.AIC_CS_EUCJP.equalsIgnoreCase(trasncodeCharset)
				&& !GmmsMessage.AIC_CS_KSC5601
						.equalsIgnoreCase(trasncodeCharset)) {
			log
					.warn(
							"We can't support target charset: {} when content scaning from Unicode.",
							trasncodeCharset);
			return false;
		}

		byte[] bytes = null;
		String content = msg.getTextContent();
		try {
			bytes = content.getBytes(GmmsMessage.AIC_CS_UCS2);
		} catch (UnsupportedEncodingException ex) {
			// no exception as hard code to unicode
		}
		if (bytes.length % 2 != 0) {
			return false;
		}
		byte bigBit = (byte) 0x00;
		byte littleBit = (byte) 0x00;
		int i = 0;
		for (; i < bytes.length; i = i + 2) {
			bigBit = bytes[i];
			littleBit = bytes[i + 1];

			if (GmmsMessage.AIC_CS_ASCII.equalsIgnoreCase(trasncodeCharset)) {
				hasNoOtherChar = isEnglish(bigBit, littleBit);
			} else if (GmmsMessage.AIC_CS_KSC5601
					.equalsIgnoreCase(trasncodeCharset)) {
				hasNoOtherChar = isEnglish(bigBit, littleBit)
						|| isKorean(bigBit, littleBit);
			} else if (GmmsMessage.AIC_CS_EUCJP
					.equalsIgnoreCase(trasncodeCharset)) {
				hasNoOtherChar = isEnglish(bigBit, littleBit)
						|| isJapanese(bigBit, littleBit);
			} else if (GmmsMessage.AIC_CS_GBK
					.equalsIgnoreCase(trasncodeCharset)) {
				hasNoOtherChar = isEnglish(bigBit, littleBit)
						|| isChinese(bigBit, littleBit);
			}
			// add other charset here in future

			if (!hasNoOtherChar) {
				// has other character, no need to transcode, just return
				return true;
			}
		}

		try { // need to be transcoded to trasncodeCharset
			msg.setContentType(trasncodeCharset);
			msg.setMessageSize(content.getBytes(trasncodeCharset).length);
			return true;
		} catch (UnsupportedEncodingException ex) {
			return false;
		}
	}

	private boolean scanMsgForBinary(GmmsMessage msg, String trasncodeCharset) {
		byte[] bytes = msg.getMimeMultiPartData();
		if (bytes == null) {
			return false;
		}
		byte bigBit = (byte) 0x00;
		byte littleBit = (byte) 0x00;
		int bytesLen = bytes.length;
		int i = 0;

		if (GmmsMessage.AIC_CS_KSC5601.equalsIgnoreCase(trasncodeCharset)) {
			while (i < bytesLen) {
				bigBit = bytes[i];
				if (bigerThanOrEquals(bigBit, (byte) 0x00)
						&& bigerThanOrEquals((byte) 0x7F, bigBit)) { // ASCII
																		// one
																		// byte
					i++;
					continue;
				} else if (bigerThanOrEquals(bigBit, (byte) 0x81) && // KSC5601
																		// two
																		// bytes
						bigerThanOrEquals((byte) 0xFE, bigBit)) { // Leading
																	// byte
																	// range:0x81-0xFE
					i++;
					if (i < bytesLen) {
						littleBit = bytes[i];
						if ((bigerThanOrEquals(littleBit, (byte) 0x41) && bigerThanOrEquals(
								(byte) 0x5A, littleBit)) // Trailing byte
															// range:0x41-0x5A
								|| (bigerThanOrEquals(littleBit, (byte) 0x61) && bigerThanOrEquals(
										(byte) 0x7A, littleBit)) // Trailing
																	// byte
																	// range:0x61-0x7A
								|| (bigerThanOrEquals(littleBit, (byte) 0x81) && // Trailing
																					// byte
																					// range:0x81-0xFE
								bigerThanOrEquals((byte) 0xFE, littleBit))) {
							i++;
							continue;
						} else {
							log
									.warn(
											"{} ContentScan from binary to KSC5601, has other character, big:{}:little:{}",
											msg.getMsgID(), bigBit, littleBit);
							return true;
						}
					} else {
						log
								.warn(
										"{} ContentScan from binary to KSC5601, last byte is:{}",
										msg.getMsgID(), bigBit);
						return true;
					}
				} else {
					log
							.warn(
									"{} ContentScan from binary to KSC5601, has other character, current byte is:{}",
									msg.getMsgID(), bigBit);
					return true;
				}
			}
		} else {
			log
					.warn(
							"We can't support target charset: {} when content scaning from binary.",
							trasncodeCharset);
			return false;
		}

		try { // need to be transcoded to trasncodeCharset
			msg.setTextContent(new String(bytes, trasncodeCharset));
			msg.setContentType(trasncodeCharset);
			msg.setMessageSize(bytes.length);
			msg.setMimeMultiPartData(null);
			msg.setGmmsMsgType(GmmsMessage.AIC_MSG_TYPE_TEXT);
			if(log.isInfoEnabled()){
				log.info("{} Finish contentScan from binary to {}", msg.getMsgID(),
					trasncodeCharset);
			}
			return true;
		} catch (Exception ex) {
			log.warn(ex, ex);
			return false;
		}
	}

	private boolean isEnglish(byte bigBit, byte littleBit) {
		if (bigBit == (byte) 0x00) // ASCII
			return true;
		return false;
	}

	private boolean isKorean(byte bigBit, byte littleBit) {
		if (bigBit == (byte) 0x11
				|| // Korean-specific: Hangul Jamo (锟斤拷锟斤拷锟斤拷母)
				(bigerThanOrEquals(bigBit, (byte) 0xAC) && bigerThan(
						(byte) 0xD7, bigBit))
				|| ((bigBit == (byte) 0xD7) && (bigerThanOrEquals(littleBit,
						(byte) 0x00) && bigerThanOrEquals((byte) 0xAF,
						littleBit))) || // Korean-specific: Hangul Syllables
										// (锟斤拷锟斤拷拼锟斤拷)
				(bigBit == (byte) 0x31 && (bigerThanOrEquals(littleBit,
						(byte) 0x30) && bigerThanOrEquals((byte) 0x8F,
						littleBit)))) { // Hangul Compatibility Jamo
			return true;
		}
		return false;
	}

	private boolean isChinese(byte bigBit, byte littleBit) {
		if (bigBit == (byte) 0x31
				&& ((bigerThanOrEquals(littleBit, (byte) 0x00) && bigerThanOrEquals(
						(byte) 0x2F, littleBit)) || // Chinese-specific:
													// Bopomofo(注锟斤拷锟斤拷锟�
				(bigerThanOrEquals(littleBit, (byte) 0xA0) && bigerThanOrEquals(
						(byte) 0xBF, littleBit)))) // Chinese-specific: Bopomofo
													// Extended
													// (注锟斤拷锟斤拷锟�-锟斤拷锟斤拷锟斤、锟酵硷拷锟斤拷锟斤拷展锟斤拷
			return true;
		return false;
	}

	private boolean isJapanese(byte bigBit, byte littleBit) {
		if ((bigBit == (byte) 0x31 && (bigerThanOrEquals(littleBit, (byte) 0xF0) && bigerThanOrEquals(
				(byte) 0xFF, littleBit)))
				|| // Japanese-specific: Katakana Phonetic Ext.(锟斤拷锟斤拷片锟斤拷锟斤拷拼锟斤拷锟斤拷展)
				(bigBit == (byte) 0x30 && ((bigerThanOrEquals(littleBit,
						(byte) 0x40) && bigerThanOrEquals((byte) 0x9F,
						littleBit)) || // Japanese-specific: Hiragana (锟斤拷锟斤拷平锟斤拷锟斤拷)
				(bigerThanOrEquals(littleBit, (byte) 0xA0) && bigerThanOrEquals(
						(byte) 0xFF, littleBit))))) // Japanese-specific:
													// Katakana(锟斤拷锟斤拷片锟斤拷锟斤拷)锟斤拷30A0-30FF
			return true;
		return false;
	}

	private static boolean bigerThan(byte bt1, byte bt2) {
		int i1 = bt1;
		int i2 = bt2;
		if (i1 < 0)
			i1 += 256;
		if (i2 < 0)
			i2 += 256;
		return i1 > i2;
	}

	private static boolean bigerThanOrEquals(byte bt1, byte bt2) {
		return bigerThan(bt1, bt2) || (bt1 == bt2);
	}

	public Map getContentScanInfo() {
		return contentScanInfo;
	}

	public void setContentScanInfo(ConcurrentHashMap contentScanInfo) {
		this.contentScanInfo = contentScanInfo;
	}

}
