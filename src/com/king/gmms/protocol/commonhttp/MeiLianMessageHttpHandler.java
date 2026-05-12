package com.king.gmms.protocol.commonhttp;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;






import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpParam;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

public class MeiLianMessageHttpHandler extends HttpHandler {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(MeiLianMessageHttpHandler.class);

	public MeiLianMessageHttpHandler(HttpInterface hie) {
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
		String[] address = new String[] { sender, recipient };
		List<HttpParam> parameters = hi.getMtSubmitRequest().getParamList();
		for (HttpParam param : parameters) {
			String pval = param.getParam();
			if ("recipientAddress".equalsIgnoreCase(pval)) {
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append(urlEncode(address[1], urlEncoding));// recipient
			} else if ("textContent".equalsIgnoreCase(pval)) {
				postData
						.append("&")
						.append(param.getOppsiteParam())
						.append("=")
						.append(
								urlEncode(message.getTextContent(), urlEncoding));// message
			}else{
				String parameter = HttpUtils.getParameter(param, message, cst);
				postData.append("&").append(param.getOppsiteParam()).append("=").append(parameter);
			}
		}
		if (postData.length() > 0) {
			postData.deleteCharAt(0);// delete &
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
	 * 
	 * @throws UnsupportedEncodingException
	 */
	public void parseResponse(GmmsMessage msg, String respStr) {		
		String jobid = "";
		String reason_phrase = "";		
		if (respStr == null) {			
			if(log.isInfoEnabled()){
				log.info(msg,"Invlid response format!");
			}
			
			msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
			return;
		}		
		String[] arr_r = respStr.trim().split(":");
		if (arr_r.length < 2) {
			if(log.isInfoEnabled()){
				log.info(msg, "Invlid response format!");
			}			
			msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
			return;
		}
		List<HttpParam> parameters = hi.getMtSubmitResponse().getParamList();
		for (HttpParam param : parameters) {
			String pval = param.getParam();
			String oval = param.getOppsiteParam();

			if(log.isDebugEnabled()){
				log.debug("pval={},oval={}", pval, oval);
			}
			if ("success".equalsIgnoreCase(arr_r[0])) {
				if ("outMsgID".equalsIgnoreCase(pval)) {
					jobid = arr_r[1];
				} else if ("StatusText".equalsIgnoreCase(pval)) {
					reason_phrase = arr_r[0];
				}
				msg.setOutMsgID(jobid);				
			} else {
				if (arr_r[1]!=null&&arr_r[1].contains("black keywords")&&arr_r.length>2) {
					if(log.isInfoEnabled()){
						StringBuffer keyword = new StringBuffer();
					    for(int i = 2; i<arr_r.length; i++){
					    	keyword.append(arr_r[i]).append(":");
					    }
					    keyword.delete(keyword.lastIndexOf(":"), keyword.length());
						log.info(msg, "message is blocked by key {}",keyword);
					}					
					String []outmsgids = arr_r[1].split("<br");
					if(outmsgids!=null){
						msg.setOutMsgID(outmsgids[0]);
					}
					msg.setStatus(GmmsStatus.SPAMED);
					return;
				}else{
					if(arr_r[0].contains("error")){
						if(log.isInfoEnabled()){
							log.info(msg, "message send error! detail error message is {}",arr_r[1]);
						}						
						boolean isDigit = false;
						if (Character.isDigit(arr_r[1].charAt(0))) {
							isDigit = true;
						}
						if (!isDigit) {
							reason_phrase = arr_r[1];
						}else{
							jobid = arr_r[1];
							reason_phrase = arr_r[0];
							msg.setOutMsgID(jobid);
						}												
					}else {						
						msg.setStatus(GmmsStatus.UNKNOWN_ERROR);
						return;
					}
				}
			}
		}		
		if(log.isDebugEnabled()){
    		log.debug(msg, "outMsgId={}" , msg.getOutMsgID());
		}
		HttpStatus status = new HttpStatus(null, reason_phrase);
		GmmsStatus gs = hi.mapHttpSubStatus2GmmsStatus(status);
		msg.setStatus(gs);
	}	
	
}
