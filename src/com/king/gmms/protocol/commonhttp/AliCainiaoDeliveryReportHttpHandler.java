package com.king.gmms.protocol.commonhttp;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import com.alibaba.fastjson.JSONObject;
import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpParam;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageIdGenerator;
import com.king.rest.util.SMSSignatureUtil;
import com.king.rest.util.StringUtility;

public class AliCainiaoDeliveryReportHttpHandler extends HttpHandler {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(AliCainiaoDeliveryReportHttpHandler.class);

	public AliCainiaoDeliveryReportHttpHandler(HttpInterface hie) {
		super(hie);
	}

	/**
	 * generate DR request data
	 */
	public String makeRequest(GmmsMessage message, String urlEncoding,
			A2PCustomerInfo cst) throws UnsupportedEncodingException {
		Map<String, Object> paramMap = new HashMap<>();
		String autkey = cst.getChlPasswordr();
        paramMap.put("method", "MD5");
		
		
		String requestId= message.getInMsgID();
		String bizId= message.getMsgID().replace("_", "");
		String phoneNumber = message.getRecipientAddress();
		String sender = message.getSenderAddress();
		String supplier = ((A2PSingleConnectionInfo) cst).getSpID();
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
		long rDateTime = System.currentTimeMillis()-100L;		
		Date rDateGMT8 = new Date(rDateTime);			
		String reportTime = sdf.format(rDateGMT8);
		long timestamp = System.currentTimeMillis();
		String splitRedis = gmmsUtility.getRedisClient().getString("CainiaoMsg_"+requestId);
		if(splitRedis!=null && !splitRedis.isEmpty()) {
			String sp[] = splitRedis.split("_");
			long sendTimemart = Long.parseLong(sp[0]);
			Date sDate = new Date(sendTimemart);
			String sendTime = sdf.format(sDate);
			int split = Integer.parseInt(sp[1]);				
			paramMap.put("sendTime", sendTime);
			paramMap.put("smsSize", split);
			if(splitRedis.length()>sp[0].length()+sp[1].length()+2) {
				String extend = splitRedis.substring(sp[0].length()+sp[1].length()+2);
				extend = URLDecoder.decode(extend, "UTF-8");
				paramMap.put("extend", extend);
			}				
		}
		paramMap.put("bizId", bizId);			
		paramMap.put("phoneNumber", phoneNumber);
		paramMap.put("requestId", requestId);
		paramMap.put("senderId", sender);
		paramMap.put("supplier", supplier);
		paramMap.put("timestamp", timestamp);
		String errorcode = "UNKNOWN";
		String errorMsg = "UNKNOWN";
		boolean success = false;
		if(GmmsStatus.DELIVERED.getText().equals(message.getStatusText())) {
			errorcode = "DELIVERED";
			errorMsg = "DELIVERED";
			success = true;
		}else if(GmmsStatus.REJECTED.getText().equals(message.getStatusText())) {
			errorcode = "REJECTED";
			errorMsg = "REJECTED";
			success = false;
		}else if(GmmsStatus.UNDELIVERABLE.getText().equals(message.getStatusText())) {
			errorcode = "UNDELIVERED";
			errorMsg = "UNDELIVERED";
			success = false;
		}
		paramMap.put("errorCode", errorcode);
		paramMap.put("errorMsg", errorMsg);
		paramMap.put("success", success);
		paramMap.put("reportTime", reportTime);			
		String sign = SMSSignatureUtil.getAliCainiaoDRSignature(autkey, paramMap);		
		paramMap.put("signature", sign);
		return JSONObject.toJSONString(paramMap);
		
	}

	/**
	 * generate DR response
	 */
	public String makeResponse(HttpStatus hs, GmmsMessage message,
			A2PCustomerInfo cst) throws IOException, ServletException {
		log.debug("make response was called. message status is:{},{}", message.getStatusCode(),message.getStatusText());
		return "";
		/*StringBuffer content = new StringBuffer();
		if (hs == null) {
			hs = drFail;
		}
		HttpStatus respStatus = drSuccess;
		if (!drSuccess.equals(hs)) {
			respStatus = drFail;
		}

		List<HttpParam> respList = hi.getMtDRResponse().getParamList();
		String respValue = "";
		for (HttpParam hp : respList) {
			String param = hp.getParam();
			if ("StatusCode".equalsIgnoreCase(param)) {
				respValue = "" + respStatus.getCode();
			} else if ("StatusText".equalsIgnoreCase(param)) {
				respValue = respStatus.getText();
			} else {
				respValue = HttpUtils.getParameter(hp, message, cst);
			}
			content.append(hp.getOppsiteParam() + "=" + respValue + "&");
		}
		String respContent = content.toString();
		if (respContent.endsWith("&")) {
			respContent = respContent.substring(0, respContent.length() - 1);
		}
		return respContent;*/
	}

	/**
	 * parse DR request
	 */
	public HttpStatus parseRequest(GmmsMessage message,
			HttpServletRequest request) {
		try {
			String hiUsername = hi.getUsername();
			int rssid = -1;
			String username = null;

			A2PCustomerInfo csts = null;
			A2PSingleConnectionInfo sInfo = null;
			if (hiUsername == null) {
				String protocol = hi.getInterfaceName();
				if (protocol != null && protocol.trim().length() > 0) {
					ArrayList<Integer> alSsid = gmmsUtility
							.getCustomerManager().getSsidByProtocol(protocol);
					if (alSsid == null || alSsid.size() < 1) {
						log.warn(message, "getSsid by interfaceName {} failed" , protocol);
						return drFail;
					}
					rssid = alSsid.get(0);
					message.setRSsID(rssid);
					csts = gmmsUtility.getCustomerManager().getCustomerBySSID(
							rssid);
					sInfo = (A2PSingleConnectionInfo) csts;
				}
			} else {
				username = request.getParameter(hi.getUsername());
        		if(log.isDebugEnabled()){
        			log.debug("username={}", username);
        		}
				if (username == null || username.trim().length() < 1) {
					message.setStatus(GmmsStatus.UNKNOWN);
					return drFail;
				}
				String password = request.getParameter(hi.getPassword());

				log.debug("password={}", password);

				if (password == null || password.trim().length() < 1) {
					message.setStatus(GmmsStatus.UNKNOWN);
					return drFail;
				}

				csts = gmmsUtility.getCustomerManager().getCustomerBySpID(
						username);
				sInfo = (A2PSingleConnectionInfo) csts;
				if (sInfo == null || !(password.equals(sInfo.getAuthKey()))) {

					log.debug("customerInfo == {} by serverid = {}", sInfo,
							username);

					message.setStatus(GmmsStatus.UNKNOWN);
					return drFail;
				} else {
					message.setRSsID(sInfo.getSSID());
				}

			}

			// throttling control process
			if (!super.checkIncomingThrottlingControl(csts.getSSID(), message)) {
				return drFail;
			}

			String statusCode = null;
			String statusText = null;
			for (HttpParam hp : hi.getMtDRRequest().getParamList()) {
				String param = hp.getParam();
				String value = request.getParameter(hp.getOppsiteParam()) != null ? request
						.getParameter(hp.getOppsiteParam())
						: hp.getDefaultValue();

				log
						.debug(
								"param name={};OppsiteParam={}; requestvalue = {}; value={}",
								hp.getParam(), hp.getOppsiteParam(), request
										.getParameter(hp.getOppsiteParam()),
								value);

				if ("statusText".equalsIgnoreCase(param)) {
					statusText = value;
				} else if ("statusCode".equalsIgnoreCase(param)) {
					statusCode = value;
				} else {
					message.setProperty(hp.getParam(), value);
				}
			}
			GmmsStatus gs = processStatus(statusCode, statusText);
			message.setStatus(gs);
			log.debug("outmsgid:{}", message.getOutMsgID());
			if (message.getOutMsgID() == null) {
				message
						.setOutMsgID(MessageIdGenerator
								.generateCommonStringID());
			}
			// use msgId to swap between modules
			message.setMsgID(message.getOutMsgID());

			return drSuccess;
		} catch (Exception e) {
			log.error(e, e);
			return drFail;
		}
	}

	/**
	 * parse DR response
	 */
	public void parseResponse(GmmsMessage message, String resp) {
		log.info("received response is: {}", resp);
		gmmsUtility.getRedisClient().del("CainiaoMsg_"+message.getInMsgID());
		return;
	}

	/**
	 * deal with Common Http status
	 * 
	 * @param status
	 * @return
	 * @throws Exception
	 */
	private GmmsStatus processStatus(String statusCode, String statusText)
			throws Exception {
		if(log.isDebugEnabled()){
			log.debug("DR statuscode={},statusText={}", statusCode, statusText);
		}
		GmmsStatus gs = GmmsStatus.UNKNOWN;
		try {
			// HttpStatus hs = CommonHttpStatus.getDRStatusByCode(code);
			HttpStatus hs = new HttpStatus(statusCode, statusText);
			gs = hi.mapHttpDRStatus2GmmsStatus(hs);
		} catch (NumberFormatException e) {
			log.error("Parse status code error!", e);
			throw e;
		} catch (Exception e) {
			log.error("Process status error!", e);
			throw e;
		}
		return gs;
	}
	
	/**
	 * generate Query DR request data
	 */
	public String makeQueryDRRequest(GmmsMessage message, String urlEncoding,
			A2PCustomerInfo cst) throws UnsupportedEncodingException {
		StringBuffer postData = new StringBuffer();
		List<HttpParam> parameters = hi.getMtDRRequest().getParamList();
		for (HttpParam param : parameters) {
			String pval = param.getParam();
			if ("statusText".equalsIgnoreCase(pval)) {
				if (message.getStatusCode() == GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT
						.getCode()) {
					message.setStatusCode(HttpUtils.getCodeFromDRStatus(message
							.getStatusText()));
				}
				HttpStatus hs = hi.mapGmmsStatus2HttpDRStatus(message
						.getStatus());
				String del_status = hs.getText();
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append(urlEncode(del_status, urlEncoding));
			} else if ("statusCode".equalsIgnoreCase(pval)) {
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append(message.getStatusCode());
			} else if ("dateIn".equalsIgnoreCase(pval)) {
				String dateIn = parseHttpDateIn(param);
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append(urlEncode(dateIn, urlEncoding));
			} else {
				String mval = HttpUtils.getParameter(param, message, cst);
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append(urlEncode(mval, urlEncoding));
			}
		}
		if (postData.length() > 0) {
			postData.deleteCharAt(0);// delete &
		}
		return postData.toString();
	}
}
