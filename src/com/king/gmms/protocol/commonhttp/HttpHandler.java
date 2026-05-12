package com.king.gmms.protocol.commonhttp;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.domain.http.HttpConstants;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpParam;
import com.king.gmms.throttle.ThrottlingControl;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.rest.util.StringUtility;

public abstract class HttpHandler {
	private static SystemLogger log = SystemLogger.getSystemLogger(HttpHandler.class);
	protected GmmsUtility gmmsUtility;
	protected A2PCustomerManager customerManager;
	protected HttpInterface hi;
	protected HttpStatus subSuccess = null;
	protected HttpStatus subFail = null;
	protected HttpStatus drSuccess = null;
	protected HttpStatus drFail = null;

	public HttpHandler(HttpInterface hie) {
		hi = hie;
		gmmsUtility = GmmsUtility.getInstance();
		subSuccess = hi.mapGmmsStatus2HttpSubStatus(GmmsStatus.SUCCESS);
		subFail = hi.mapGmmsStatus2HttpSubStatus(GmmsStatus.UNKNOWN_ERROR);
		drSuccess = hi.mapGmmsStatus2HttpDRRespStatus(GmmsStatus.DELIVERED);
		drFail = hi.mapGmmsStatus2HttpDRRespStatus(GmmsStatus.UNKNOWN);
		customerManager = gmmsUtility.getCustomerManager();		
	}

	public abstract String makeRequest(GmmsMessage message, String urlEncoding,
			A2PCustomerInfo cst) throws UnsupportedEncodingException;

	public abstract HttpStatus parseRequest(GmmsMessage message,
			HttpServletRequest request) throws ServletException, IOException;		

	public abstract String makeResponse(HttpStatus hs, GmmsMessage message,
			A2PCustomerInfo cst) throws IOException, ServletException;

	public abstract void parseResponse(GmmsMessage message, String resp);

	/**
	 * @param src
	 *            String
	 * @return String
	 * @throws UnsupportedEncodingException
	 */
	protected String urlEncode(String src, String charset)
			throws UnsupportedEncodingException {
		if (src == null) {

			log.debug("src == null!");

			return null;
		}
		return java.net.URLEncoder.encode(src, charset);
	}

	/**
	 * decode http message content
	 * 
	 * @param content
	 * @param mtype
	 * @param hi
	 * @return
	 */
	protected String decodeContent(String content, String mtype,
			HttpInterface hi) {

		if(log.isDebugEnabled()){
			log.debug("content value before process : {};mtype={}", content, mtype);
		}
		if (content == null) {
			return null;
		}
		String retValue = content;
		HttpCharset httpCharset = hi.getHttpCharsetByMessageType(mtype);
		String charset = httpCharset.getCharset();
		if (mtype != null) {
			try {
				retValue = URLDecoder.decode(content, charset);
			} catch (UnsupportedEncodingException e) {
				log.error("{} is not support.", charset);
			}
		}

		if(log.isDebugEnabled()){
			log.debug("content value after charset convert : {}", retValue);
		}
		if (retValue.length() > 255) {
			retValue = retValue.substring(0, 255);
		}
		return retValue;
	}

	/**
	 * process if is need DeliveryReport
	 * 
	 * @param param
	 * @param message
	 * @return
	 */
	protected String parseHttpDeliveryReport(HttpParam param,
			GmmsMessage message) {
		String value = param.getDefaultValue().trim();
		if ("1".equals(value)) {
			return "1";// req_status
		} else if ("0".equals(value)) {
			return "0";// req_status
		} else {
			boolean isNeedDR = message.getDeliveryReport();
			if (isNeedDR) {
				return "1";// req_status
			} else {
				return "0";// req_status
			}
		}
	}

	/**
	 * process ExpiryDate
	 * 
	 * @param csts
	 * @param expireDate
	 * @param value
	 * @return
	 */
	protected Date parseHttpExpiryDate(A2PCustomerInfo csts,
			HttpParam expireDate, String value) {
		
		// check whether need parse time
		if (!csts.isParseValidityPeriod()) {
			return null;
		}
		
		Date date = null;
		try {
			date = HttpUtils.parseHttpDate(csts, expireDate, value);
		} catch (Exception e) {
			if (log.isInfoEnabled()) {
				log.info("Invalid expireDate format: {}", value);
			}
			return null;
		}
		
		if (date != null ) {
			if (gmmsUtility.checkExpiryDateFromCust(date)) {
				return date;
			} else {
				if (log.isInfoEnabled()) {
					log.info("CheckExpiryDateFromCust failed: {}", value);
				}
				return null;
			}
			
		}
		return null;
	}
	
	protected Date parseHttpScheduleDeliveryTime(A2PCustomerInfo csts,
			HttpParam scheduleDeliveryTime, String value) throws Exception {
		
		// check whether need parse time
		if (!csts.isParseScheduleDeliveryTime()) {
			return null;
		}
		
		Date date = HttpUtils.parseHttpDate(csts, scheduleDeliveryTime, value);
		
		if (date != null ) {
			if (gmmsUtility.checkScheduleDeliveryTimeFromCust(date)) {
				return date;
			} else {
				throw new Exception("Invalid scheduleDeliveryTime.");
			}
			
		}
		
		return null;
	}
	
	/**
	 * parse Gmms Timestamp
	 */
	public static String parseTimestamp(HttpParam param,
			GmmsMessage message)
	{
		Date timestamp=message.getTimeStamp();
		if(timestamp==null)
		{
			timestamp=new Date();
		}
		SimpleDateFormat from = new SimpleDateFormat("yyyyMMddHHmmss");
		String datetime = from.format(timestamp);
		return datetime;
		
	}

	/**
	 * parse Gmms ExpiredDate
	 * 
	 * @param s
	 * @return
	 */
	public static String parseGmmsExpiredDate(HttpParam param,
			GmmsMessage message) {
		Date expiredDate = message.getExpiryDate();
		log.trace(message, "parseGmmsExpiredDate expiredDate={}" , expiredDate);
		return parseDate(param, expiredDate);
	}

	/**
	 * 
	 * @param param
	 * @param date
	 * @return
	 */
	protected static String parseDate(HttpParam param, Date date) {
		String ptype = param.getType();
		if (date == null) {
			date = new Date();
		}
		if (HttpConstants.EXPIRYDATE_TYPE_ABSOLUTE.equalsIgnoreCase(ptype)) {// ABSOLUTE
																				// type
			SimpleDateFormat from = new SimpleDateFormat(param.getFormat());
			String datetime = from.format(date);
			return datetime;
		} else if (HttpConstants.EXPIRYDATE_TYPE_RELATIVE
				.equalsIgnoreCase(ptype)) {// Relative type
			Date curdate = new Date();
			long datetime = (date.getTime() - curdate.getTime()) / (1000 * 60);
			return Long.toString(datetime);// validity
		}
		return new SimpleDateFormat().format(date);
	}

	/**
	 * process http DateIn
	 * 
	 * @param s
	 * @return
	 */
	protected String parseHttpDateIn(HttpParam param) {
		String format = param.getFormat();
		if (null == format || "".equals(format.trim())) {
			format = "yyyyMMddHHmmss";
		}
		SimpleDateFormat df = new SimpleDateFormat(format);
		TimeZone zone = TimeZone.getTimeZone("GMT+8");
		df.setTimeZone(zone);
		String time = df.format(new Date());
		return time;
	}

	/**
	 * encode message content
	 * 
	 * @param msg
	 * @return
	 */
	protected String encodeMessage(GmmsMessage msg, String urlEncoding,
			boolean isUrlEncodingTwice) {
		if (msg.getContentType() == null
				|| "".equals(msg.getContentType().trim())) {
			log.warn(msg, "The content type is null!");
			msg.setStatus(GmmsStatus.INVALID_MSG_FIELD);
			return null;
		} else if (msg.getTextContent() == null) {
			if(log.isDebugEnabled()){
				log.debug("Null message content={}", msg.getTextContent());
			}
			return null;
		}
		HttpCharset httpCharset = hi.mapGmmsCharset2HttpCharset(msg
				.getContentType());
		String charset = httpCharset.getCharset();
		try {
			String message = hi.convert2HttpContent(msg.getContentType(), msg
					.getTextContent());
			message = java.net.URLEncoder.encode(message, charset);
			if (isUrlEncodingTwice) {
				message = java.net.URLEncoder.encode(message, urlEncoding);
			}
			return message;
		} catch (UnsupportedEncodingException e) {
			log.error("Incorrect charset when httpcharset:" + httpCharset, e);
		}
		return null;
	}

	protected String[] parseGmmsContent(GmmsMessage message) {
		return parseGmmsContent(message, null, false);
	}

	/**
	 * process Gmms Content & UDH
	 * 
	 * @param message
	 * @return
	 */
	protected String[] parseGmmsContent(GmmsMessage message,
			String urlEncoding, boolean isUrlEncodingTwice) {
		StringBuffer contentStr = new StringBuffer();
		StringBuffer udhStr = new StringBuffer();
		String messageType = message.getGmmsMsgType();
		byte[] udh = message.getUdh();
		if (udh != null && udh.length > 0) { // has UDH
			for (byte one : udh) {
				udhStr
						.append(HttpUtils.format2Digits(Integer
								.toHexString(one)));
			}
		}
		if (messageType.equals(GmmsMessage.AIC_MSG_TYPE_BINARY)) { // text/binary
			byte[] content = message.getMimeMultiPartData();
			for (int i = 0; i < content.length; i++) {
				contentStr.append(HttpUtils.format2Digits(Integer
						.toHexString(content[i])));
			}
		} else { // text
			String msgcontent = this.encodeMessage(message, urlEncoding,
					isUrlEncodingTwice);
			contentStr.append(msgcontent);
		}
		String[] rt = { contentStr.toString(), udhStr.toString() };
		return rt;
	}

	/**
	 * parse http Content & UDH
	 * 
	 * @param message
	 * @return
	 */
	protected void parseHttpContent(GmmsMessage message, String mtype,
			String content, String udh) throws UnsupportedEncodingException {
		HttpCharset httpCharset = hi
				.mapGmmsCharset2HttpCharset(GmmsMessage.AIC_MSG_TYPE_BINARY);
		boolean isBinary = false;
		if (httpCharset.getMessageType().equalsIgnoreCase(mtype)) {
			isBinary = true;
		}
		byte[] udhBytes = HttpUtils.getBytesByHexString(udh);
		if (udhBytes != null) {
			message.setUdh(udhBytes);
		}
		if (isBinary) {
			byte[] mimeData = HttpUtils.getBytesByHexString(content);
			message.setMimeMultiPartData(mimeData);
			message.setMessageSize(mimeData.length);
		} else {
			String textcontent = decodeContent(content, mtype, hi);
			message.setTextContent(textcontent);
			message.setMessageSize(message.getTextContent().getBytes(
					message.getContentType()).length);
		}
	}
	
	protected boolean checkIncomingThrottlingControl(int ssid, String args) {
		boolean ret = false;
		
		if(!StringUtility.stringIsNotEmpty(GmmsUtility.getInstance().getRedisClient().getString("thcon"))){    		
        	return ret;
    	}
		
		if (ThrottlingControl.getInstance().isAllowedToReceive(ssid)) {
			ret = true;
		} else {
			if (log.isInfoEnabled()) {
				log.info(args + " refuced by incoming throttling control");
			}
		}
		return ret;
	}
	
	protected boolean checkIncomingThrottlingControl(int ssid, GmmsMessage msg) {
		boolean ret = false;
		
		if (ThrottlingControl.getInstance().isAllowedToReceive(ssid)) {
			ret = true;
		} else {
			if (log.isInfoEnabled()) {
				log.info(msg, "refuced by incoming throttling control");
			}
		}
		return ret;
	}
	
	public static void main(String[] args) {
		
	}
		
}