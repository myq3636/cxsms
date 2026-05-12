package com.king.gmms.protocol.commonhttp;

import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import org.xml.sax.InputSource;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpParam;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

public class WeMediaMessageHttpHandler extends HttpHandler {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(WeMediaMessageHttpHandler.class);

	public WeMediaMessageHttpHandler(HttpInterface hie) {
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
		String paramKey_content = null;
		String paramKey_signature = null;
		String paramData = null;
		Map<String, String> paramValues = new HashMap<String, String>();
		
		List<HttpParam> parameters = hi.getMtSubmitRequest().getParamList();
		for (HttpParam param : parameters) {
			String pval = param.getParam();
			if ("recipientAddress".equalsIgnoreCase(pval)) {
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append(urlEncode(address[1], urlEncoding));// recipient
			} else if ("textContent".equalsIgnoreCase(pval)) {
				paramData = param.getOppsiteParam();				
			} else if ("innerTransaction".equalsIgnoreCase(pval)) {				
				paramKey_content = param.getOppsiteParam();// paramkey
			} else if ("transaction".equalsIgnoreCase(pval)) {	
				paramKey_signature = param.getOppsiteParam();// paramkey
	        }else if ("specialDataCodingScheme".equalsIgnoreCase(pval)) {				
				postData.append("&").append(param.getOppsiteParam())
				.append("=").append("XML");// paramType
	        } else if ("contentType".equalsIgnoreCase(pval)) {
				String mtype = message.getContentType();
				if (mtype.equalsIgnoreCase("ASCII")) {
					mtype = "0";
				} else {
					mtype = "8";
				}
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append(urlEncode(mtype, urlEncoding));
			} else if ("ChlPassword".equalsIgnoreCase(pval)) {
				String tokenID = HttpUtils.getParameter(param, message, cst);
				String[] numberList = cst.getSpecialServiceNumList();
				if (numberList != null && numberList.length > 0) {
					for (String number : numberList) {
						if (address[0].startsWith(number)) {
							tokenID += address[0].substring(number.length());
							break;
						}
					}
				}
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append(tokenID);
			} else {
				String parameter = HttpUtils.getParameter(param, message, cst);
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append(parameter);
			}
		}
		A2PCustomerInfo customer = GmmsUtility.getInstance().getCustomerManager().getCustomerBySSID(message.getOSsID());
		if (customer!=null&& customer.getSmsOptionTemplateSignature()!=null) {			
			paramValues.put(paramKey_signature, urlEncode(customer.getSmsOptionTemplateSignature(), urlEncoding));
		} else {
			String value = ((A2PSingleConnectionInfo)cst).getChlAcctName();
			if (value == null) {
				value = "King";
			}
			paramValues.put(paramKey_signature, urlEncode(value, urlEncoding));
		}
		
		// Wemedia need content be compatible with XML specification
		// XML has a special set of characters that cannot be used in normal XML strings 
		String xmlTextContent = message.getTextContent();
		xmlTextContent = xmlTextContent.replaceAll("&", "&amp;");
		xmlTextContent = xmlTextContent.replaceAll(">", "&gt;");
		xmlTextContent = xmlTextContent.replaceAll("<", "&lt;");
		xmlTextContent = xmlTextContent.replaceAll("'", "&apos;");
		xmlTextContent = xmlTextContent.replaceAll("\"", "&quot;");
		
		paramValues.put(paramKey_content,xmlTextContent);
		String paramValue = "<data><param key="+"\""+paramKey_signature+"\""+" value="+"\""
				+ paramValues.get(paramKey_signature) +"\""+ " /> <param key="+"\""+paramKey_content+"\""+" value=" +"\""
				+ paramValues.get(paramKey_content) +"\""+ " /> </data>";
		postData.append("&")
		.append(paramData)
		.append("=")
		.append(urlEncode(paramValue, urlEncoding));// message
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
	public void parseResponse(GmmsMessage msg, String resp) {
		String respStr = null;
		if (resp != null) {
			respStr = this.domXml(resp);
		} else {
			respStr = resp;
		}

		String jobid = "";
		String reason_phrase = "";
		String resp_code = "";
		if (respStr == null) {
			log.error("Invlid response format!");
			msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
			return;
		}
		String[] arr_r = respStr.split(":");
		if (arr_r.length < 2 && arr_r.length > 3) {
			log.error("Invlid response format!");
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
			if ("Ok".equalsIgnoreCase(arr_r[0])) {
				if ("outMsgID".equalsIgnoreCase(pval)) {
					jobid = arr_r[1];
				} else if ("StatusText".equalsIgnoreCase(pval)) {
					reason_phrase = arr_r[0];
				}
				String outmsgid = jobid.substring(1, jobid.length() - 1);
				msg.setOutMsgID(outmsgid);
				resp_code = "0";
			} else {
				if (arr_r.length != 3) {
					log.error("Invlid response format!");
					msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
					return;
				}
				reason_phrase = arr_r[2];
				resp_code = arr_r[1];
			}
		}
		// log.debug("jobid="+jobid+",msg_status_code="+msg_status_code+",reason_phrase="+reason_phrase);
		if(log.isDebugEnabled()){
    		log.debug(msg, "outMsgId={}" , msg.getOutMsgID());
		}
		HttpStatus status = new HttpStatus(resp_code, null);
		GmmsStatus gs = hi.mapHttpSubStatus2GmmsStatus(status);
		msg.setStatus(gs);
	}

	private String domXml(String xml) {
		StringReader reader = null;
		String s = null;
		try {
			reader = new StringReader(xml);
			InputSource source = new InputSource(reader);
			SAXBuilder sb = new SAXBuilder();
			Document doc = sb.build(source);
			Element root = doc.getRootElement();
			// List node = root.getChildren();
			s = root.getText();
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
		return s;
	}

}
