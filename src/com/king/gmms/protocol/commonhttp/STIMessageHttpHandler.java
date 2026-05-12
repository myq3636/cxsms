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

public class STIMessageHttpHandler extends HttpHandler {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(STIMessageHttpHandler.class);	

	public STIMessageHttpHandler(HttpInterface hie) {
		super(hie);
	}

	/**
	 * generate submit request data
	 */
	public String makeRequest(GmmsMessage message, String urlEncoding,
			A2PCustomerInfo cst) throws UnsupportedEncodingException {
		StringBuffer postData = new StringBuffer();
		String recipient = message.getRecipientAddress();
		List<HttpParam> parameters = hi.getMtSubmitRequest().getParamList();
		boolean isUrlEncodingTwice = cst.isUrlEncodingTwice();
		String[] content = this.parseGmmsContent(message, urlEncoding,
				isUrlEncodingTwice);
		String userName = null;
		String Password = null;
		Map<String, Object> map = new HashMap<String, Object>();				
		for (HttpParam param : parameters) {
			String pval = param.getParam();
			if ("senderAddress".equalsIgnoreCase(pval)) {
				map.put(param.getOppsiteParam(), message.getSenderAddress());
			} else if ("textContent".equalsIgnoreCase(pval)) {
				map.put(param.getOppsiteParam(), message.getTextContent());
			} else if ("recipientAddress".equalsIgnoreCase(pval)) {
				map.put(param.getOppsiteParam(), recipient);
			} else if ("chlAcctNamer".equalsIgnoreCase(pval)) {
				userName = HttpUtils.getParameter(param, message, cst);				
				map.put(param.getOppsiteParam(), userName);
			} else if ("chlPasswordr".equalsIgnoreCase(pval)) {
				String chpwd = HttpUtils.getParameter(param, message, cst);				
				map.put(param.getOppsiteParam(), chpwd);
			} else {
				String mval = HttpUtils.getParameter(param, message, cst);
				if (mval != null) {
					map.put(param.getOppsiteParam(), mval);
				}
			}

		}		
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
		if(log.isDebugEnabled()){
			log.debug("received submit response is {}", respStr);
		}
		Map<String, Object> map = (Map<String, Object>)JSONObject.parse(respStr);
		Map<String, String> result = null;
		
		if (map!=null && map.get("result")!=null) {
			result = (Map<String, String>)map.get("result");			
		}
		
		if (result == null) {			
			//GmmsStatus gs = hi.mapHttpSubStatus2GmmsStatus(null);
			msg.setStatus(GmmsStatus.UNKNOWN_ERROR);
			return;
		}
		List<HttpParam> parameters = hi.getMtSubmitResponse().getParamList();
		String statusCode = null;
		for (HttpParam param : parameters) {
			String pval = param.getParam();
			String oval = param.getOppsiteParam();
    		if(log.isDebugEnabled()){
    			log.debug("pval={},oval={}", pval, oval);
    		}
			if ("StatusCode".equalsIgnoreCase(pval)) {
				statusCode = result.get(oval);
			} else {
				msg.setProperty(pval, result.get(oval));
			}
		}
		// log.debug("jobid="+jobid+",msg_status_code="+msg_status_code+",reason_phrase="+reason_phrase);
		if(log.isDebugEnabled()){
			log.debug(msg, "outMsgId={}" , msg.getOutMsgID());
		}
		HttpStatus status = new HttpStatus(statusCode, null);
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
