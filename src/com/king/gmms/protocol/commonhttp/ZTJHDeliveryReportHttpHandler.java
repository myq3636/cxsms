package com.king.gmms.protocol.commonhttp;

import java.io.BufferedReader;
import java.io.IOException;
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
import com.alibaba.fastjson.JSONObject;
import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpParam;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

public class ZTJHDeliveryReportHttpHandler extends HttpHandler {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(ZTJHDeliveryReportHttpHandler.class);

	public ZTJHDeliveryReportHttpHandler(HttpInterface hie) {		   
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
		if (hs == null) {
			hs = drFail;
		}
		Map<String, String> map = new HashMap<String, String>();
		if (hs != drSuccess) {
			map.put("status", "0") ;
			map.put("msg", "失败") ;
		}else {
			map.put("status", "1") ;
			map.put("msg", "成功") ;
		}		
		String respContent = JSONObject.toJSONString(map);		
		return respContent;
	}

	/**
	 * parse DR request
	 */
	public HttpStatus parseListRequest(List<GmmsMessage> msgs,
			HttpServletRequest request){
		try {
			int rssid = -1;

			A2PCustomerInfo csts = null;
			A2PSingleConnectionInfo sInfo = null;
			String protocol = hi.getInterfaceName();
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

			BufferedReader br = request.getReader();
			String str, wholeStr = "";
			while((str = br.readLine()) != null){
			wholeStr += str;
			}			
			// throttling control process
			/*if (!super.checkIncomingThrottlingControl(csts.getSSID(), args)) {
				return drFail;
			}*/

			if (wholeStr == null || ("").equals(wholeStr.trim())) {
				return drFail;
			}
			List<Map<String, String>> DRs = (List<Map<String, String>>)JSONObject.parse(wholeStr);
			for (Map<String, String> dr : DRs) {			
				if (dr!=null && dr.get("gatewayId") != null) {
					String outmsgid = dr.get("gatewayId");
					GmmsMessage drMsg = new GmmsMessage();
					drMsg.setMsgID(outmsgid);
					drMsg.setOutMsgID(outmsgid);
					drMsg.setRecipientAddress(dr.get("phone"));
					try {
						SimpleDateFormat sdFormat = new SimpleDateFormat(
			        			"yyyy-MM-dd HH:mm:ss");
						Date drDate = sdFormat.parse(dr.get("reportTime"));
						drMsg.setDateIn(drDate);
					} catch (Exception e) {
						log.warn("date format is invalid! timemark="+dr.get("reportTime"));
					}
					
					GmmsStatus gs = null;
					try {
						gs = processStatus(dr.get("status"), dr.get("status"));
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
