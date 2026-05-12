package com.king.gmms.protocol.commonhttp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpParam;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

public class VocomDeliveryReportHttpHandler extends HttpHandler {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(VocomDeliveryReportHttpHandler.class);

	public VocomDeliveryReportHttpHandler(HttpInterface hie) {		   
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
		return "OK";
	}

	/**
	 * parse DR request
	 */
	public HttpStatus parseListRequest(List<GmmsMessage> msgs,
			HttpServletRequest request){
		String protocol = hi.getInterfaceName();		
		try {
			int rssid = -1;

			A2PCustomerInfo csts = null;
			A2PSingleConnectionInfo sInfo = null;
			//String protocol = hi.getInterfaceName();
			if (protocol != null && protocol.trim().length() > 0) {
				ArrayList<Integer> alSsid = gmmsUtility.getCustomerManager()
						.getSsidByProtocol(protocol);
				if (alSsid == null || alSsid.size() < 1) {
					log
							.warn(
									"getSsid by interfaceName {} failed in parseListRequest() function.",
									protocol);
					return drFail;
				}
				rssid = alSsid.get(0);
				csts = gmmsUtility.getCustomerManager()
						.getCustomerBySSID(rssid);
				sInfo = (A2PSingleConnectionInfo) csts;
			}

			if (sInfo == null) {
				return drFail;
			}

			BufferedReader streamReader = new BufferedReader( new InputStreamReader(request.getInputStream(), "UTF-8"));
            StringBuilder responseStrBuilder = new StringBuilder();
            String inputStr;
            while ((inputStr = streamReader.readLine()) != null){
                responseStrBuilder.append(inputStr);						
			}	
		    String wholeStr = responseStrBuilder.toString();
			// throttling control process
			/*if (!super.checkIncomingThrottlingControl(csts.getSSID(), args)) {
				return drFail;
			}*/

			if (wholeStr == null || ("").equals(wholeStr.trim())) {
				log.warn("received dr msg is null from {}", protocol);
				return drFail;
			}
			log.warn("received dr msg is {} from {}", wholeStr, protocol);
			JSONArray DRs = JSONObject.parseArray(wholeStr);
			
			for (int i= 0 ; i<DRs.size(); i++) {
				JSONObject dr = DRs.getJSONObject(i);
				if (dr!=null && "1".equals(String.valueOf(dr.get("flag")))) {
					String outmsgid = String.valueOf(dr.get("mid"));
					GmmsMessage drMsg = new GmmsMessage();
					drMsg.setMsgID(outmsgid);
					drMsg.setOutMsgID(outmsgid);
					drMsg.setRecipientAddress(String.valueOf(dr.get("mobile")));					
					
					GmmsStatus gs = null;
					try {
						gs = processStatus(String.valueOf(dr.get("stat")), String.valueOf(dr.get("stat")));
					} catch (Exception e) {
						log
								.error(drMsg,
										"process message status error in parseListRequest() function." + e.toString());
					}
					drMsg.setStatus(gs);
					if(log.isDebugEnabled()){
		        		log.debug(drMsg, "outmsgid:{}" , outmsgid);
					}
					drMsg.setRSsID(rssid);
					msgs.add(drMsg);
				}				
			}

			return drSuccess;
		} catch (Exception e) {
			log.error(e, e);
			return drFail;
		}
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
		} catch (Exception e) {
			log.error("Process status error!", e);			
		}
		return gs;
	}

	/**
	 * parse DR response
	 */
	public void parseResponse(GmmsMessage message, String resp) {

	}

	@Override
	public HttpStatus parseRequest(GmmsMessage message,
			HttpServletRequest request) throws ServletException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
