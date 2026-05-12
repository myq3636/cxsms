package com.king.gmms.protocol.commonhttp;

import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;
import org.xml.sax.InputSource;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpParam;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

public class ThaibulkMessageHttpHandler extends HttpHandler {
	public ThaibulkMessageHttpHandler(HttpInterface hie) {
		super(hie);
		// TODO Auto-generated constructor stub
	}

	private static SystemLogger log = SystemLogger.getSystemLogger(ThaibulkMessageHttpHandler.class);

	@Override
	public String makeRequest(GmmsMessage message, String urlEncoding,
			A2PCustomerInfo cst) throws UnsupportedEncodingException {
		// TODO Auto-generated method stub
		
		List<HttpParam> params = hi.getMtSubmitRequest().getParamList();
		StringBuffer postData = new StringBuffer();
		String content = message.getTextContent();
		HttpCharset httpCharset = hi.mapGmmsCharset2HttpCharset(message.getContentType());
		String charset = httpCharset.getCharset();
		content =  java.net.URLEncoder.encode(content, charset);
		
		for (HttpParam hp : params) {
			String pval = hp.getParam();
			String mval = null;
			if("textContent".equalsIgnoreCase(pval)){
				postData.append("&").append(hp.getOppsiteParam())
				.append("=").append(content);// message
			}else {
				mval = HttpUtils.getParameter(hp, message, cst);
				postData.append("&").append(hp.getOppsiteParam())
						.append("=").append(urlEncode(mval, urlEncoding));
			}
		}
		
		if (postData.length() > 0) {
			postData.deleteCharAt(0);// delete &
		}
		
		return postData.toString();
	}

	@Override
	public String makeResponse(HttpStatus hs, GmmsMessage message,
			A2PCustomerInfo cst) throws IOException, ServletException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public HttpStatus parseRequest(GmmsMessage message,
			HttpServletRequest request) throws ServletException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void parseResponse(GmmsMessage message, String resp) {
		// TODO Auto-generated method stub	
		List<HttpParam> parameters = hi.getMtSubmitResponse().getParamList();
		StringReader reader = null;
		reader = new StringReader(resp);
		InputSource source = new InputSource(reader);
		SAXBuilder sb = new SAXBuilder();
		Document doc = null;
		Element root = null;
		try {
				doc = sb.build(source);
				root = doc.getRootElement();
				List<Element> node = root.getChildren();
				if(node==null){
					log.error(message,"Invlid message Submit Response format from Thaibulk!{}");
					message.setStatus(GmmsStatus.UNKNOWN_ERROR);
					return;
				}
				String statusCode = "";
				String statusText = "";
				for(int i=0;i<node.size();i++){
					Element e =(Element)node.get(i);
					
					if("QUEUE".equalsIgnoreCase(e.getName())){
						List<Element> queueParam = e.getChildren();
						for(int j=0;j<queueParam.size();j++){
							Element e2 = (Element)queueParam.get(j);
							for (HttpParam param : parameters) {
								if(e2.getName().equalsIgnoreCase(param.getOppsiteParam())){
									if("Status".equalsIgnoreCase(e2.getName())){
										statusCode = e2.getText();
									}if("Detail".equalsIgnoreCase(e2.getName())){
										statusText = e2.getText();
									}else{
										message.setProperty(param.getParam(), e2.getText());										
									}
								}
							}
						}
						HttpStatus status = new HttpStatus(statusCode, statusText);
						GmmsStatus gs = hi.mapHttpSubStatus2GmmsStatus(status);
						message.setStatus(gs);
						statusCode = "";
						statusText = "";
					}// end of if (QUEUE)
					else{
						for (HttpParam param : parameters) {
							if(e.getName().equalsIgnoreCase(param.getOppsiteParam())){
								if("Status".equalsIgnoreCase(e.getName())){
									statusCode = e.getText();
								}if("Detail".equalsIgnoreCase(e.getName())){
									statusText = e.getText();
								}else{
									message.setProperty(param.getParam(), e.getText());										
								}
							}
						}
						
					}
				}//end of for
				
				if(!"".equals(statusCode) && !"".equals(statusText)){
					HttpStatus status = new HttpStatus(statusCode, statusText);
					GmmsStatus gs = hi.mapHttpSubStatus2GmmsStatus(status);
					message.setStatus(gs);
					statusCode = "";
					statusText = "";
				}
				
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("parseResponse exception in ThaibulkMessageHttpHandler class!{}", e);	
			message.setStatus(GmmsStatus.UNKNOWN_ERROR);
		}
	}
}
