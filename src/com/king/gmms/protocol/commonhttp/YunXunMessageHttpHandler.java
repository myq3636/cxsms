package com.king.gmms.protocol.commonhttp;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import com.alibaba.fastjson.JSONObject;
import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpParam;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageIdGenerator;

public class YunXunMessageHttpHandler extends HttpHandler {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(YunXunMessageHttpHandler.class);	

	public YunXunMessageHttpHandler(HttpInterface hie) {
		super(hie);
	}

	/**
	 * generate submit request data
	 */
	public String makeRequest(GmmsMessage message, String urlEncoding,
			A2PCustomerInfo cst) throws UnsupportedEncodingException {
		StringBuffer postData = new StringBuffer();
		String sender = message.getSenderAddress();
		String recipient = message.getRecipientAddress();
		String[] address = new String[]{sender,recipient};
    	List<HttpParam> parameters = hi.getMtSubmitRequest().getParamList();
		for(HttpParam param:parameters){
			String pval = param.getParam();
			if("recipientAddress".equalsIgnoreCase(pval)){
				postData.append("&").append(param.getOppsiteParam()).append("=").append(urlEncode(address[1],urlEncoding));//recipient
			}else if("textContent".equalsIgnoreCase(pval)){			
				postData.append("&").append(param.getOppsiteParam()).append("=").append(urlEncode(message.getTextContent(), urlEncoding));//message
			}else{
				String mval = HttpUtils.getParameter(param, message, cst);
				postData.append("&").append(param.getOppsiteParam()).append("=").append(urlEncode(mval,urlEncoding));
			}
		}
		if(postData.length()>0){
			postData.deleteCharAt(0);//delete &
		}
		String chlAccount = cst.getChlAcctNamer();
		String chlPwd = cst.getChlPasswordr();
		String send = ((A2PSingleConnectionInfo)cst).getSMSOptionHttpCustomParameter();
		long timestamp = System.currentTimeMillis();
        SimpleDateFormat sdFormat = new SimpleDateFormat(
    			"yyyyMMddHHmmss");
        sdFormat.setTimeZone(TimeZone.getTimeZone("GMT+8"));
        String dataTime = sdFormat.format(new Date(timestamp));
		postData.append("&uid=").append(chlAccount)
		        .append("&pw=").append(chlPwd)
		        .append("&tm=").append(dataTime);
		if (send !=null && !"".equals(send)) {
			postData.append("&ex=").append(send);
		}
		return postData.toString();
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
		
		if (respStr == null) {			
			//GmmsStatus gs = hi.mapHttpSubStatus2GmmsStatus(null);
			msg.setStatus(GmmsStatus.UNKNOWN_ERROR);
			return;
		}
		if (respStr.startsWith("-")) {
			HttpStatus status = new HttpStatus(respStr, null);
			GmmsStatus gs = hi.mapHttpSubStatus2GmmsStatus(status);
			msg.setStatus(gs);
		}else{			
			msg.setStatus(GmmsStatus.SUCCESS);
			msg.setOutMsgID(respStr);
			if(log.isDebugEnabled()){
				log.debug(msg, "outMsgId={}" , msg.getOutMsgID());
			}
		}				
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
