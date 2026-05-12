package com.king.gmms.protocol.commonhttp;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpParam;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageIdGenerator;
import com.king.rest.util.StringUtility;

public class CommonDeliveryReportHttpHandler extends HttpHandler {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(CommonDeliveryReportHttpHandler.class);

	public CommonDeliveryReportHttpHandler(HttpInterface hie) {
		super(hie);
	}

	/**
	 * generate DR request data
	 */
	public String makeRequest(GmmsMessage message, String urlEncoding,
			A2PCustomerInfo cst) throws UnsupportedEncodingException {
		StringBuffer postData = new StringBuffer();
		List<HttpParam> parameters = hi.getMoDRRequest().getParamList();
		for (HttpParam param : parameters) {
			String pval = param.getParam();
			HttpStatus hs = hi.mapGmmsStatus2HttpDRStatus(message
					.getStatus());
			String del_status = hs.getText();
			String del_statucode = hs.getCode();
			
			if ("statusText".equalsIgnoreCase(pval)) {
				if (message.getStatusCode() == GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT
						.getCode()) {
					message.setStatusCode(HttpUtils.getCodeFromDRStatus(message
							.getStatusText()));
				}				
				postData.append("&").append(param.getOppsiteParam()).append("=").append(urlEncode(del_status, urlEncoding));
			} else if ("statusCode".equalsIgnoreCase(pval)) {
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append(del_statucode);
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
		log.debug("parseResponse was called. message status is:{},{}", message.getStatusCode(),message.getStatusText());
		log.info("received response is: {}", resp);
		List<HttpParam> parameters = hi.getMoDRResponse().getParamList();
		if (parameters == null || parameters.isEmpty()) {
			return;
		}
		if (!StringUtility.stringIsNotEmpty(resp)) {
			message.setStatus(GmmsStatus.DELIVERED);
			return;
		}
		String respStr = resp.trim();
		String jobid = "";
		int msg_status_code = -1;
		String reason_phrase = "";
		String[] arr_r = respStr.split("&");
		HashMap<String, String> respmap = new HashMap<String, String>();
		for (String str : arr_r) {
			String[] arr_v = str.split("=");
			if (arr_v.length != 2) {
				log.error("Invlid response format!");
				message.setStatus(GmmsStatus.UNKNOWN);
				return;
			}
			respmap.put(arr_v[0], arr_v[1]);
		}
		for (HttpParam param : parameters) {
			String pval = param.getParam();
			String oval = param.getOppsiteParam();
			if ("inMsgID".equalsIgnoreCase(pval)) {
				jobid = respmap.get(oval);
			} else if ("StatusCode".equalsIgnoreCase(pval)) {
				try {
					msg_status_code = Integer.parseInt(respmap.get(oval));
				} catch (Exception e) {
					log.error("Invlid response format:{}", oval);
					message.setStatus(GmmsStatus.UNKNOWN);
					return;
				}

			} else if ("StatusText".equalsIgnoreCase(pval)) {
				reason_phrase = respmap.get(oval);
			}
		}
		if(log.isInfoEnabled()){
			log.info(message, "Receive DR response,JobID: {} , statuscode : {} " ,
				jobid,msg_status_code);
		}
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
