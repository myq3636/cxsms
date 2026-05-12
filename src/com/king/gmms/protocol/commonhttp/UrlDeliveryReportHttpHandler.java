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

public class UrlDeliveryReportHttpHandler extends HttpHandler {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(UrlDeliveryReportHttpHandler.class);

	public UrlDeliveryReportHttpHandler(HttpInterface hie) {
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
			if ("statusText".equalsIgnoreCase(pval)) {
				if (message.getStatusCode() == GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT
						.getCode()) {
					message.setStatusCode(HttpUtils.getCodeFromDRStatus(message
							.getStatusText()));
				}
				HttpStatus hs = hi.mapGmmsStatus2HttpDRStatus(message
						.getStatus());
				String del_status = null;
				if (hs == null) {
					del_status = drFail.text;
					log.error(message, "Failed to mapGmmsStatus "
							+ message.getStatus()
							+ " to HttpStatus!So use default HttpStatus:"
							+ del_status);
				} else {
					del_status = hs.toString();
				}
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append(urlEncode(del_status, urlEncoding));
			} else if ("statusCode".equalsIgnoreCase(pval)) {
				if (message.getStatusCode() == GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT
						.getCode()) {
					message.setStatusCode(HttpUtils.getCodeFromDRStatus(message
							.getStatusText()));
				}
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
		if(log.isDebugEnabled()){
    		log.debug(message, "postData = {}" , postData.toString());
		}
		return postData.toString();
	}

	/**
	 * generate DR response
	 */
	public String makeResponse(HttpStatus hs, GmmsMessage message,
			A2PCustomerInfo cst) throws IOException, ServletException {
		StringBuffer content = new StringBuffer();
		if (hs == null) {
			hs = drFail;
		}
		HttpStatus respStatus = drSuccess;
		if (!drSuccess.equals(hs)) {
			respStatus = drFail;
		}
		List<HttpParam> respList = hi.getMtDRResponse().getParamList();

		for (HttpParam hp : respList) {
			String respValue = "";
			String param = hp.getParam();
			if ("StatusCode".equalsIgnoreCase(param)) {
				respValue = "" + respStatus.getCode();
			} else if ("StatusText".equalsIgnoreCase(param)) {
				respValue = respStatus.getText();
			} else {
				respValue = HttpUtils.getParameter(hp, message, cst);
			}

			if(log.isDebugEnabled()){
				log.debug("param name={};OppsiteParam={}; respValue = {}", hp
					.getParam(), hp.getOppsiteParam(), respValue);
			}
			content.append(hp.getOppsiteParam() + "=" + respValue + "&");
		}
		String respContent = content.toString();
		if (respContent.endsWith("&")) {
			respContent = respContent.substring(0, respContent.length() - 1);
		}
		if(log.isDebugEnabled()){
    		log.debug(message, "respContent = {}" , respContent);
		}
		return respContent;
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

			String statusText = null;
			for (HttpParam hp : hi.getMtDRRequest().getParamList()) {
				String value = request.getParameter(hp.getOppsiteParam()) != null ? request
						.getParameter(hp.getOppsiteParam())
						: hp.getDefaultValue();

				log
						.debug(
								"param name={};OppsiteParam={}; requestvalue = {}; value={}",
								hp.getParam(), hp.getOppsiteParam(), request
										.getParameter(hp.getOppsiteParam()),
								value);

				if (hp.getParam().equalsIgnoreCase("statustext")) {
					statusText = value;
				} else {
					message.setProperty(hp.getParam(), value);
				}
			}

			GmmsStatus gs = processStatus(statusText);
			message.setStatus(gs);

			// throttling control
			if (!super.checkIncomingThrottlingControl(csts.getSSID(), message)) {
				return drFail;
			}

			// use msgId to swap between modules
			message.setMsgID(message.getOutMsgID());

			return drSuccess;
		} catch (Exception e) {
			log.error("process dr error!");
			return drFail;
		}
	}

	/**
	 * parse DR response
	 */
	public void parseResponse(GmmsMessage message, String resp) {
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
		List<HttpParam> parameters = hi.getMoDRResponse().getParamList();
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
			log.info(message, "Receive DR response and the JobID:{},statuscode:{} " ,jobid, msg_status_code);
		}

	}

	/**
	 * deal with Netstars status
	 * 
	 * @param status
	 * @return
	 * @throws Exception
	 */
	private GmmsStatus processStatus(String status) throws Exception {

		if(log.isDebugEnabled()){
			log.debug("DR statusText={}", status);
		}
		GmmsStatus gs = GmmsStatus.UNKNOWN;
		if (status == null) {
			return gs;
		}
		try {
			String[] statusArr = status.split("-");
			if (statusArr != null && statusArr.length >= 2) {
				String statusCode = statusArr[0].trim();
				String statusText = statusArr[1].trim();

				if(log.isDebugEnabled()){
					log.debug("DR statuscode={},statusText={}", statusCode,
						statusText);
				}
				// HttpStatus hs =
				// NetStarsHttpStatus.getDRStatusByCode(statusCode);
				HttpStatus hs = new HttpStatus(statusCode, statusText);
				gs = hi.mapHttpDRStatus2GmmsStatus(hs);
			}
		} catch (Exception e) {
			log.error("Process status error!", e);
			throw e;
		}

		return gs;
	}
}
