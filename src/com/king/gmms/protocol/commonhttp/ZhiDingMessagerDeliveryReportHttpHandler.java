package com.king.gmms.protocol.commonhttp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import org.xml.sax.InputSource;

import com.king.db.DBLockConnection;
import com.king.db.DataControl;
import com.king.db.DataControlException;
import com.king.framework.SystemLogger;
import com.king.gmms.MailSender;
import com.king.gmms.customerconnectionfactory.CommonHttpClientFactory;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpParam;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageStoreManager;
import com.king.redis.RedisClient;
import com.mysql.jdbc.log.Log;

public class ZhiDingMessagerDeliveryReportHttpHandler extends HttpHandler {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(ZhiDingMessagerDeliveryReportHttpHandler.class);		
	
	public ZhiDingMessagerDeliveryReportHttpHandler(HttpInterface hie) {
		super(hie);
		
	}

	/**
	 * generate DR request data
	 */
	public String makeRequest(GmmsMessage message, String urlEncoding,
			A2PCustomerInfo cst) throws UnsupportedEncodingException {
		StringBuffer postData = new StringBuffer();
		List<HttpParam> parameters = hi.getMtDRRequest().getParamList();
		for (HttpParam param : parameters) {
			String pval = param.getParam();
			if ("MsgID".equalsIgnoreCase(pval)) {
				String minId = message.getMsgID();
				if(minId==null ||"".equals(minId.trim())){//todo
					minId = "0";
				}				
				postData.append("&").append(param.getOppsiteParam())
				.append("=").append(minId);
			}else {	
				String parameter = HttpUtils.getParameter(param, message, cst);
				postData.append("&").append(param.getOppsiteParam()).append("=").append(parameter);
			}
		}
		if (postData.length() > 0) {
			postData.deleteCharAt(0);// delete &
		}
		
		String chlAccount = cst.getChlAcctNamer();
		String chlPwd = cst.getChlPasswordr();
		String send = ((A2PSingleConnectionInfo)cst).getChlPassword();
		String userId = ((A2PSingleConnectionInfo)cst).getChlAcctName();		
		long timestamp = System.currentTimeMillis();
        SimpleDateFormat sdFormat = new SimpleDateFormat(
    			"yyyyMMddHHmmss");
        sdFormat.setTimeZone(TimeZone.getTimeZone("GMT+8"));
        String dataTime = sdFormat.format(new Date(timestamp));
		String sign = HttpUtils.encrypt(chlAccount+chlPwd+dataTime, "MD5");
		
		postData.append("&action=").append(send)
		        .append("&account=").append(chlAccount)
		        .append("&password=").append(chlPwd);
		        	
		
		return postData.toString();
	}

	/**
	 * generate DR response
	 */
	public String makeResponse(HttpStatus hs, GmmsMessage message,
			A2PCustomerInfo cst) throws IOException, ServletException {

		return null;
	}

	/**
	 * parse DR request
	 */
	public HttpStatus parseRequest(GmmsMessage message,
			HttpServletRequest request) {
		return null;
	}

	/**
	 * parse DR response
	 */
	public List<GmmsMessage> parseListResponse(GmmsMessage message, A2PCustomerInfo cm, String resp) {
		if (resp != null) {
			return this.domXml(cm, resp);
		} else {
			return null;
		}

	}
	

	private List<GmmsMessage> domXml(A2PCustomerInfo cm, String xml) {
		StringReader reader = null;
		List<GmmsMessage> messages = new ArrayList<GmmsMessage>();
		List<HttpParam> parameters = hi.getMtDRResponse().getParamList();		 
		try {
			reader = new StringReader(xml);
			InputSource source = new InputSource(reader);
			SAXBuilder sb = new SAXBuilder();
			Document doc = sb.build(source);
			Element root = doc.getRootElement();
			List node = root.getChildren();
			CommonHttpClientFactory.getInstance().setQueryMinID(cm.getShortName()+"_MTDR", "1");
			if (node == null) {
				return null;
			}
			
			Element et = null;			
			for (int i = 0; i < node.size(); i++) {
				et = (Element) node.get(i);
				List childern = et.getChildren();
				GmmsMessage msg = new GmmsMessage();				
				Element child = null;
				for (int j = 0; j < childern.size(); j++) {
					child = (Element) childern.get(j);
                    if ("error".equalsIgnoreCase(child.getName())) {
						log.error("the respone of query DR failed, the result code is {}, and the result reason is {}", child.getText(), et.getChildText("remark"));
					}
					String statusText = null;
					if(log.isInfoEnabled()){
						log.info("{}:{} ", child.getName(), child.getText());
					}
					for (HttpParam param : parameters) {
						msg
								.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
						String pval = param.getParam();
						// String pval1 = new
						// StringBuffer("").append(Character.toTitleCase(pval.charAt(0))).append(pval.substring(1)).toString();
						String value = null;
						if (child.getName().equalsIgnoreCase(
								param.getOppsiteParam())) {
							value = child.getText() != null ? child.getText()
									: param.getDefaultValue();
							if ("StatusText".equalsIgnoreCase(pval)) {
								statusText = value;
								HttpStatus status = new HttpStatus(statusText,
										statusText);
								GmmsStatus gs = hi
										.mapHttpDRStatus2GmmsStatus(status);
								msg.setStatus(gs);
							} else {
								msg.setProperty(pval, value);
							}
						}
					}
				}
				if(log.isInfoEnabled()){
					log.info(msg, "Receive DR response and the outMsgId is {}, and statuscode is {}, recipientAddress is {}"
						          , msg.getOutMsgID(), msg.getStatusCode() ,msg.getRecipientAddress());
				}
				messages.add(msg);
			}
		} catch (JDOMException e) {
			log.warn(" dom xml format error and exception is {}", e
					.getMessage());
			return null;
		} catch (IOException e) {
			log.warn("io exception{}", e.getMessage());
			return null;
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (Exception e) {
				}
			}
		}
		return messages;
	}
	
	
	@Override
	public void parseResponse(GmmsMessage message, String resp) {
		// TODO Auto-generated method stub

	}	
}
