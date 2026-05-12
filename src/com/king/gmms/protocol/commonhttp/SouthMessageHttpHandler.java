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

import com.alibaba.fastjson.JSONObject;
import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpParam;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageIdGenerator;

public class SouthMessageHttpHandler extends HttpHandler {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(SouthMessageHttpHandler.class);	

	public SouthMessageHttpHandler(HttpInterface hie) {
		super(hie);
	}

	/**
	 * generate submit request data
	 */
	public String makeRequest(GmmsMessage message, String urlEncoding,
			A2PCustomerInfo cst) throws UnsupportedEncodingException {		
		List<HttpParam> parameters = hi.getMtSubmitRequest().getParamList();		
		Map<String, Object> map = new HashMap<String, Object>();		
		if (message.getOutMsgID() == null) {
	   		 String commonMsgID = MessageIdGenerator.generateCommonStringID();
				message.setOutMsgID(commonMsgID);
		  }
		for (HttpParam param : parameters) {
			String pval = param.getParam();
			if ("senderAddress".equalsIgnoreCase(pval)) {
				map.put(param.getOppsiteParam(), message.getSenderAddress());
			} else if ("textContent".equalsIgnoreCase(pval)) {
				map.put(param.getOppsiteParam(), message.getTextContent());
			} else if ("recipientAddress".equalsIgnoreCase(pval)) {
				List<String> reciptList = new ArrayList<String>();
				reciptList.add(message.getRecipientAddress());
				map.put(param.getOppsiteParam(), reciptList);
			}else {
				String mval = HttpUtils.getParameter(param, message, cst);
				if (mval != null) {
					map.put(param.getOppsiteParam(), mval);
				}
			}
			
		}	
		map.put("dr", Boolean.TRUE);
		map.put("prefix", "86");
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
		Map<String, Object> map = (Map<String, Object>)JSONObject.parse(respStr);
		String statusCode = "";
		if (map!=null && map.get("result")!=null) {
			statusCode = String.valueOf(map.get("result"));
			
		}						
		// log.debug("jobid="+jobid+",msg_status_code="+msg_status_code+",reason_phrase="+reason_phrase);
		if(log.isDebugEnabled()){
			log.debug(msg, "outMsgId={}" , msg.getOutMsgID());
		}
		HttpStatus status = new HttpStatus(statusCode, statusCode);
		GmmsStatus gs = hi.mapHttpSubStatus2GmmsStatus(status);
		msg.setStatus(gs);
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
	}
}
