package com.king.gmms.protocol.commonhttp;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpParam;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageIdGenerator;

public class ThaibulkDeliveryReportHttpHandler extends HttpHandler {

	private static SystemLogger log = SystemLogger
	.getSystemLogger(ThaibulkDeliveryReportHttpHandler.class);
	
	public ThaibulkDeliveryReportHttpHandler(HttpInterface hie){
		super(hie);
	}
	
	@Override
	public String makeRequest(GmmsMessage message, String urlEncoding,
			A2PCustomerInfo cst) throws UnsupportedEncodingException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String makeResponse(HttpStatus hs, GmmsMessage message,
			A2PCustomerInfo cst) throws IOException, ServletException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public HttpStatus parseRequest(GmmsMessage message,
			HttpServletRequest request) throws ServletException, IOException {
		// TODO Auto-generated method stub
		
		A2PCustomerInfo csts = null;
		int rssid = -1;
		try {
			
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
	
				log.debug("param name={};OppsiteParam={}; requestvalue = {}; value={}",
								hp.getParam(), hp.getOppsiteParam(), request.getParameter(hp.getOppsiteParam()),
								value);
	
				if ("statusCode".equalsIgnoreCase(param)) {
					statusCode = value;
				} else {
					message.setProperty(hp.getParam(), value);
				}
			}
			
			GmmsStatus gs = processStatus(statusCode, null);
			if(gs.equals(GmmsStatus.ENROUTE)){
				return drFail;
			}else{
				message.setStatus(gs);
			}
			
			log.debug("outmsgid:{}", message.getOutMsgID());
			if (message.getOutMsgID() == null) {
				message.setOutMsgID(MessageIdGenerator
								.generateCommonStringID());
			}
			// use msgId to swap between modules
			message.setMsgID(message.getOutMsgID());
				
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return drFail;
		} 
		return drSuccess;
	}

	@Override
	public void parseResponse(GmmsMessage message, String resp) {
		// TODO Auto-generated method stub

	}
	
	private GmmsStatus processStatus(String statusCode, String statusText)
	throws Exception {
		if(log.isDebugEnabled()){
			log.debug("DR statuscode={},statusText={}", statusCode, statusText);
		}
		GmmsStatus gs = GmmsStatus.UNKNOWN;
		try {
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

}
