package com.king.gmms.protocol.commonhttp;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
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

public class CICNDeliveryReportHttpHandler extends HttpHandler {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(CICNDeliveryReportHttpHandler.class);

	public CICNDeliveryReportHttpHandler(HttpInterface hie) {
		super(hie);
	}

	/**
	 * generate DR request data
	 */
	public String makeRequest(GmmsMessage message, String urlEncoding,
			A2PCustomerInfo cst) throws UnsupportedEncodingException {
		return null;
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
		if (hs != drSuccess) {
			respStatus = drFail;
		}

		List<HttpParam> respList = hi.getMtDRResponse().getParamList();
		String respValue = "";
		for (HttpParam hp : respList) {
			String param = hp.getParam();
			// Object value = message.getProperty(hp.getParam());
			// response won't have "deliveryReport"
			// if ("deliveryReport".equalsIgnoreCase(param)) {
			// if ((Boolean) value) {
			// respValue = "1";
			// } else {
			// respValue = "0";
			// }
			if ("StatusCode".equalsIgnoreCase(param)) {
				respValue = "" + respStatus.getCode();
			} else {
				respValue = HttpUtils.getParameter(hp, message, cst);
			}
			content.append(hp.getOppsiteParam() + "=" + respValue + "&");
		}
		String respContent = content.toString();
		if (respContent.endsWith("&")) {
			respContent = respContent.substring(0, respContent.length() - 1);
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
						log.warn(message, "getSsid by interfaceName {} failed", protocol);
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

				if ("statusCode".equalsIgnoreCase(param)) {
					statusCode = value;
				} else {
					message.setProperty(hp.getParam(), value);
				}
			}
			HttpStatus status = new HttpStatus(statusCode, null);
			GmmsStatus gs = hi.mapHttpDRStatus2GmmsStatus(status);
			message.setStatus(gs);
			if (message.getOutMsgID() == null) {
				return drFail;
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

	}

}
