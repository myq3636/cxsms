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

public class YunXunDeliveryReportHttpHandler extends HttpHandler {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(YunXunDeliveryReportHttpHandler.class);

	public YunXunDeliveryReportHttpHandler(HttpInterface hie) {		   
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
		return "OK";
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

			if (wholeStr == null || ("").equals(wholeStr.trim())) {
				return drFail;
			}
			if(log.isDebugEnabled()){
				log.debug("receive dr from {}, message is: {}", rssid, wholeStr);
			}
			String[] response = URLDecoder.decode(wholeStr,"UTF-8").split("=");
			if (response == null || response.length<2) {
				return drFail;
			}
			String drResp= response[1];
			String[] drStrs = drResp.split("\\|");
			for (int i = 0; i < drStrs.length; i++) {
				String[] drmsg = drStrs[i].split(",");
				if (drmsg!=null && drmsg.length>=3) {
					GmmsMessage drMsg = new GmmsMessage();
					drMsg.setOutMsgID(drmsg[0]);
					drMsg.setMsgID(drmsg[0]);
					drMsg.setRecipientAddress(drmsg[1]);
					GmmsStatus gs = null;
					try {
						gs = processStatus(drmsg[2], drmsg[2]);
					} catch (Exception e) {
						log
								.error(drMsg,
										"process message status error in parseListRequest() function." + e.toString());
					}
					drMsg.setStatus(gs);
					if(log.isDebugEnabled()){
		        		log.debug(drMsg, "outmsgid:{}" , drmsg[0]);
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
	
	public static void main(String[] args) {
		String m="data=634470007%2C13523456789%2CDELIVRD";
		try {
			System.out.println(URLDecoder.decode(m, "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String s = "628400007,18500375454,DELIVRD|1112090856000111,13900000002,DELIVRD|1112090856000112,13900000003,DELIVRD";
		String[] ts = s.split("\\|");
		System.out.println(ts[1]);
		try {
			System.out.println("北京".getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
