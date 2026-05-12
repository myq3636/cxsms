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
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;
import org.xml.sax.InputSource;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpParam;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

public class JatisDeliveryReportHttpHandler extends HttpHandler {
	private static SystemLogger log = SystemLogger.getSystemLogger(JatisDeliveryReportHttpHandler.class);
	public JatisDeliveryReportHttpHandler(HttpInterface hie) {
		super(hie);
		// TODO Auto-generated constructor stub
	}

	/**
	 * generate DR request XML info
	 */
	public String makeRequest(GmmsMessage message, String urlEncoding,
			A2PCustomerInfo cst) throws UnsupportedEncodingException {
		
		Element root,messageID,drField;
		root = new Element("DRRequest");
		List<HttpParam> parameters = hi.getMtDRRequest().getParamList();
		for(HttpParam param:parameters){
			String pval = param.getParam();
			if("outMsgID".equalsIgnoreCase(pval)){
				String outMsgid = message.getOutMsgID();
				if(outMsgid==null){
					log.error(message,"Jatis did not give outMsgId to A2P!");
					return "";					
				}
				messageID = new Element(param.getOppsiteParam()); 
				messageID.setText(message.getOutMsgID());
				root.addContent(messageID);				
			}else{
				String mval =  HttpUtils.getParameter(param, message, cst);				
				if(!"".equals(mval)){					
					drField = new Element(param.getOppsiteParam()); 
					drField.setText(mval);
					root.addContent(drField);
				}else{
					log.error(message,"Jatis CCB info does not configure ",param.getOppsiteParam()," value");
					return "";
				}
			}			
		}
		Format format = Format.getCompactFormat();
	    format.setEncoding("utf-8");
	    format.setIndent("    ");
	    XMLOutputter XMLOut = new XMLOutputter(format);
	    String postData = XMLOut.outputString(root);
		if (log.isDebugEnabled()) {
			log.debug("Jatis dr request info :",XMLOut.outputString(root));
		}
		
		return postData;
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
		List<HttpParam> parameters = hi.getMtDRResponse().getParamList();
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
					log.error(message,"Invlid message DR Response format from Jatis!{}");
					message.setStatus(GmmsStatus.UNKNOWN);
					return;
				}
				String statusCode = "";
				for(int i=0;i<node.size();i++){
					Element e =(Element)node.get(i);
					
					if("RequestStatus".equalsIgnoreCase(e.getName())){
						statusCode = e.getText();												
						GmmsStatus gs = processStatus(statusCode,null);
						if(!GmmsStatus.DELIVERED.equals(gs)){
							message.setStatus(gs);
							return;
						}
					}else if("Reports".equalsIgnoreCase(e.getName())){
						List<Element> reports = e.getChildren();
						for(int j=0;j<reports.size();j++){
							Element e2 = (Element)reports.get(j);
							for (HttpParam param : parameters) {
								if(e2.getName().equalsIgnoreCase(param.getOppsiteParam())){
									if("DeliveryStatus".equalsIgnoreCase(e2.getName())){										
										GmmsStatus gs = processStatus(e2.getText(),null);
										message.setStatus(gs);
									}else{
										message.setProperty(param.getParam(), e2.getText());										
									}
								}
							}
						}
					}
				}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("parseResponse exception in JatisDeliveryReportHttpHandler class!{}", e);	
			message.setStatus(GmmsStatus.UNKNOWN);
		}
	}

	
	private GmmsStatus processStatus(String statusCode, String statusText) throws Exception {
		
		if (log.isDebugEnabled()) {
			log.debug("DR statuscode=" , statusCode,",statusText=",statusText);
		}
		
		GmmsStatus gs = GmmsStatus.UNKNOWN;		
		try{			
			HttpStatus hs = new HttpStatus(statusCode, statusText);
			gs = hi.mapHttpDRRespStatus2GmmsStatus(hs);
		}catch(NumberFormatException e){
			log.error("Parse status code error!{}",e);
			throw e;
		}catch(Exception e){
			log.error("Process status error!{}",e);
			throw e;
		}
		return gs;
	}
	
}
