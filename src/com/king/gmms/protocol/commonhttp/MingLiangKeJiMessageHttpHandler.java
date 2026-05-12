package com.king.gmms.protocol.commonhttp;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpParam;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageIdGenerator;

public class MingLiangKeJiMessageHttpHandler extends HttpHandler {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(MingLiangKeJiMessageHttpHandler.class);	

	public MingLiangKeJiMessageHttpHandler(HttpInterface hie) {
		super(hie);
	}

	/**
	 * generate submit request data
	 */
	public String makeRequest(GmmsMessage message, String urlEncoding,
			A2PCustomerInfo cst) throws UnsupportedEncodingException {		
		List<HttpParam> parameters = hi.getMtSubmitRequest().getParamList();		
		Map<String, Object> map = new HashMap<String, Object>();	
        long currentTime = System.currentTimeMillis()/1000L;
        map.put("timestamp", currentTime);
		for (HttpParam param : parameters) {
			String pval = param.getParam();
			if ("senderAddress".equalsIgnoreCase(pval)) {
				map.put(param.getOppsiteParam(), message.getSenderAddress());
			} else if ("textContent".equalsIgnoreCase(pval)) {
				String content = bytesToHexString(message.getTextContent().getBytes("utf-8"));
				map.put(param.getOppsiteParam(), content);
			} else if ("recipientAddress".equalsIgnoreCase(pval)) {				
				map.put(param.getOppsiteParam(), message.getRecipientAddress());
			}else {
				String mval = HttpUtils.getParameter(param, message, cst);
				if (mval != null) {
					map.put(param.getOppsiteParam(), mval);
				}
			}
				
		}
		String pwd = HttpUtils.md5Encrypt(cst.getChlAcctNamer() + "00000000"
				+ cst.getChlPasswordr() + currentTime);
	    map.put("pwd", pwd);
		map.put("spid", cst.getChlAcctNamer());		
		return JSONObject.toJSONString(map);
	}

	/**
	 * do response for submit request
	 */
	public String makeResponse(HttpStatus hs, GmmsMessage msg,
			A2PCustomerInfo cst) throws IOException, ServletException {
		return null;
	}

	/**
	 * parse submit request and give response
	 */
	public HttpStatus parseRequest(GmmsMessage msg, HttpServletRequest request)
			throws ServletException, IOException {
		return null;

	}

	/**
	 * parse submit response
	 */
	public void parseResponse(GmmsMessage msg, String resp) {
		String respStr = resp.trim();
		if (respStr == null) {						
			msg.setStatus(GmmsStatus.UNKNOWN_ERROR);
			return;
		}
        log.info("receive minglingkeji submit response {}", resp);
		Map<String, Object> map = (Map<String, Object>)JSONObject.parse(respStr);
		String statusCode = "";
		if (map!=null && map.get("status")!=null) {
			statusCode = String.valueOf(map.get("status"));
			
		}	
		List<Map<String, Object>> list = (List<Map<String, Object>>)map.get("results");
		if(list!=null){
			String outmsgid = String.valueOf(list.get(0).get("msgid"));
			if(log.isDebugEnabled()){
				log.debug(msg, "outMsgId={}" , outmsgid);
			}
			msg.setOutMsgID(outmsgid);
		}else{
			log.warn("receive minglingkeji submit response results is null");
		}
		// log.debug("jobid="+jobid+",msg_status_code="+msg_status_code+",reason_phrase="+reason_phrase);
		
		HttpStatus status = new HttpStatus(statusCode, statusCode);
		GmmsStatus gs = hi.mapHttpSubStatus2GmmsStatus(status);
		msg.setStatus(gs);
	}
	
	public static String stringToHexString(String s) {  
        String str = "";  
        for (int i = 0; i < s.length(); i++) {  
            int ch = s.charAt(i);  
            String s4 = Integer.toHexString(ch);  
            str = str + s4;  
        }  
        return str;  
  } 
	
	public static String bytesToHexString(byte[] src){   
	    StringBuilder stringBuilder = new StringBuilder("");   
	    if (src == null || src.length <= 0) {   
	        return null;   
	    }   
	    for (int i = 0; i < src.length; i++) {   
	        int v = src[i] & 0xFF;   
	        String hv = Integer.toHexString(v);   
	        if (hv.length() < 2) {   
	            stringBuilder.append(0);   
	        }   
	        stringBuilder.append(hv);   
	    }   
	    return stringBuilder.toString();   
	}

	
	public static void main(String[] args) {
		Map<String, Object> map = new HashMap<String, Object>();
		List<Map<String, String>> msgList = new ArrayList<Map<String,String>>();
		Map<String, String> msg = new HashMap<String, String>();
		map.put("useId", "uid");
		map.put("sign", "sing");
		map.put("msgList", msgList);
		msgList.add(msg);
		msg.put("text", "abc");
		msg.put("rec", "sre");
		String respStr = JSONObject.toJSONString(map);
		Map<String, Object> result = (Map<String, Object>)JSONObject.parse(respStr);
		System.out.println(result.get("msgList"));
		System.out.println(stringToHexString("Hello world"));
	}
}
