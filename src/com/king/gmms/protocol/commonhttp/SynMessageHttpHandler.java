package com.king.gmms.protocol.commonhttp;

import java.io.IOException;
import java.io.StringReader;
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

import org.apache.commons.net.util.Base64;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import org.xml.sax.InputSource;

import com.alibaba.fastjson.JSONObject;
import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpParam;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageIdGenerator;

public class SynMessageHttpHandler extends HttpHandler {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(SynMessageHttpHandler.class);	

	public SynMessageHttpHandler(HttpInterface hie) {
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
		String contentType = message.getContentType();
		String[] address = new String[]{sender,recipient};
    	List<HttpParam> parameters = hi.getMtSubmitRequest().getParamList();
		for(HttpParam param:parameters){
			String pval = param.getParam();
			if("recipientAddress".equalsIgnoreCase(pval)){
				postData.append("&").append(param.getOppsiteParam()).append("=").append(urlEncode(address[1],urlEncoding));//recipient
			}else if("charset".equalsIgnoreCase(pval)){	
				if(contentType.equalsIgnoreCase(GmmsMessage.AIC_CS_UCS2)){
					postData.append("&").append(param.getOppsiteParam()).append("=").append("UCS2");//message
				}else if(contentType.equalsIgnoreCase(GmmsMessage.AIC_CS_ISO8859_1)){
					postData.append("&").append(param.getOppsiteParam()).append("=").append("iso-8859-1");//message
				}				
			}else if("textContent".equalsIgnoreCase(pval)){	
				String content = message.getTextContent();
				/*if(contentType.equalsIgnoreCase(GmmsMessage.AIC_CS_UCS2) 
						&& content.getBytes(message.getContentType()).length>140){
                	postData.append("&").append("split").append("=3");
				}else{
					postData.append("&").append("split").append("=3");
				}*/
				postData.append("&").append("split").append("=4");
				if(contentType.equalsIgnoreCase(GmmsMessage.AIC_CS_UCS2)){                    
					content = Base64.encodeBase64String(content.getBytes(GmmsMessage.AIC_CS_UCS2));	
					
				}
				postData.append("&").append(param.getOppsiteParam()).append("=").append(urlEncode(content, urlEncoding));//message
			}else{
				String mval = HttpUtils.getParameter(param, message, cst);
				postData.append("&").append(param.getOppsiteParam()).append("=").append(urlEncode(mval,urlEncoding));
			}
		}
		if(postData.length()>0){
			postData.deleteCharAt(0);//delete &
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
		
		Map<String, String> map = domXml(respStr);
		if(map != null){
			String text = map.get("statusText");
			String outmsgId = map.get("trackingid");
			if(outmsgId!=null && outmsgId.contains(",")){
				outmsgId = outmsgId.substring(0, outmsgId.indexOf(","));
			}
			msg.setOutMsgID(outmsgId);
			if(log.isDebugEnabled()){
				log.debug(msg, "outMsgId={}" , msg.getOutMsgID());
			}
			if (text!=null) {
				HttpStatus status = new HttpStatus(text, text);
				GmmsStatus gs = hi.mapHttpSubStatus2GmmsStatus(status);
				msg.setStatus(gs);				
			}else{			
				msg.setStatus(GmmsStatus.UNKNOWN_ERROR);								
			}
		}
						
	}
	
	private Map<String, String> domXml(String xml) {
		StringReader reader = null;
		Map<String, String> result = new HashMap<String, String>();
		try {
			reader = new StringReader(xml);
			InputSource source = new InputSource(reader);
			SAXBuilder sb = new SAXBuilder();
			Document doc = sb.build(source);
			Element root = doc.getRootElement();
			// List node = root.getChildren();
			String statusText = root.getChildText("status");
			String trackingid = root.getChildText("trackingid");
			result.put("statusText", statusText);
			result.put("trackingid", trackingid);
		} catch (JDOMException e) {
			log.warn(" dom xml format error, and Exception is {}", e
					.getMessage());
			return null;
		} catch (IOException e) {
			log.warn("io exception and Exception is {}", e.getMessage());
			return null;
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (Exception e) {
					return null;
				}
			}
		}
		return result;
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
