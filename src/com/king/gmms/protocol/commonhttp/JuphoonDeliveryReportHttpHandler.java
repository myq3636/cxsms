package com.king.gmms.protocol.commonhttp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Date;
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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.customerconnectionfactory.CommonHttpClientFactory;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpParam;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageIdGenerator;

public class JuphoonDeliveryReportHttpHandler extends HttpHandler {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(JuphoonDeliveryReportHttpHandler.class);	

	public JuphoonDeliveryReportHttpHandler(HttpInterface hie) {
		super(hie);
	}
	

	/**
	 * generate DR request data
	 */
	public String makeRequest(GmmsMessage message, String urlEncoding,
			A2PCustomerInfo cst) throws UnsupportedEncodingException {
		return null;
	}

	/**
	 * generate DR response
	 */
	public String makeResponse(HttpStatus hs, GmmsMessage message,
			A2PCustomerInfo cst) throws IOException, ServletException {

		if (GmmsStatus.SUCCESS.equals(message.getStatus())) {
			return "0";
		}
		return "1";
	}

	/**
	 * parse DR request
	 */
	public HttpStatus parseRequest(GmmsMessage message,
			HttpServletRequest request) {
		//http://客户url?phone=138001380000&msgid=123&nstat=0&errcode= DELIVRD&revTime=2017-01-01 10:10:30
			try {
				String hiUsername = hi.getUsername();
				int rssid = -1;
				String username = null;

				A2PCustomerInfo csts = null;
				A2PSingleConnectionInfo sInfo = null;
				if (hiUsername == null) {
					String protocol = hi.getInterfaceName();
					if (protocol != null && protocol.trim().length() > 0) {
						ArrayList<Integer> alSsid = gmmsUtility
								.getCustomerManager().getSsidByProtocol(protocol);
						if (alSsid == null || alSsid.size() < 1) {
							log.warn(message, "getSsid by interfaceName {} failed" , protocol);
							return drFail;
						}
						rssid = alSsid.get(0);
						message.setRSsID(rssid);
						csts = gmmsUtility.getCustomerManager().getCustomerBySSID(
								rssid);
						sInfo = (A2PSingleConnectionInfo) csts;
					}
				} else {
					username = request.getParameter(hi.getUsername());
	        		if(log.isDebugEnabled()){
	        			log.debug("username={}", username);
	        		}
					if (username == null || username.trim().length() < 1) {
						message.setStatus(GmmsStatus.UNKNOWN);
						return drFail;
					}
					String password = request.getParameter(hi.getPassword());

					log.debug("password={}", password);

					if (password == null || password.trim().length() < 1) {
						message.setStatus(GmmsStatus.UNKNOWN);
						return drFail;
					}

					csts = gmmsUtility.getCustomerManager().getCustomerBySpID(
							username);
					sInfo = (A2PSingleConnectionInfo) csts;
					if (sInfo == null || !(password.equals(sInfo.getAuthKey()))) {

						log.debug("customerInfo == {} by serverid = {}", sInfo,
								username);

						message.setStatus(GmmsStatus.UNKNOWN);
						return drFail;
					} else {
						message.setRSsID(sInfo.getSSID());
					}

				}

				// throttling control process
				if (!super.checkIncomingThrottlingControl(csts.getSSID(), message)) {
					return drFail;
				}

				String drJson = recieveData(request);
				log.debug("receive dr request {}", drJson);
				JSONObject object = JSONObject.parseObject(drJson);				
				String statusCode = null;
				if (object.get("statusCode")!=null){
					statusCode = String.valueOf(object.get("statusCode"));
				}
				GmmsStatus gs = processStatus(statusCode, statusCode);
				message.setStatus(gs);
				String outmsgid = String.valueOf(object.get("requestId"));
				message.setOutMsgID(outmsgid);
				log.debug("outmsgid:{}", message.getOutMsgID());
				if (message.getOutMsgID() == null) {
					message
							.setOutMsgID(MessageIdGenerator
									.generateCommonStringID());
				}
				// use msgId to swap between modules
				message.setMsgID(message.getOutMsgID());

				return drSuccess;
			} catch (Exception e) {
				log.error(e, e);
				return drFail;
			}	
		
	}
	
	private GmmsStatus processStatus(String statusCode, String statusText)
			throws Exception {
		if(log.isDebugEnabled()){
			log.debug("DR statuscode={},statusText={}", statusCode, statusText);
		}
		GmmsStatus gs = GmmsStatus.UNKNOWN;
		try {
			// HttpStatus hs = CommonHttpStatus.getDRStatusByCode(code);
			HttpStatus hs = new HttpStatus(statusCode, statusText);
			gs = hi.mapHttpDRStatus2GmmsStatus(hs);
		} catch (NumberFormatException e) {
			log.error("Parse status code error!", e);
			throw e;
		} catch (Exception e) {
			log.error("Process status error!", e);
			throw e;
		}
		return gs;
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
	
	public static String recieveData(HttpServletRequest request){
        String inputLine = null;
        // 接收到的数据
        StringBuffer recieveData = new StringBuffer();
        BufferedReader in = null;
        try
        {
            in = new BufferedReader(new InputStreamReader(
                    request.getInputStream(), "UTF-8"));
            while ((inputLine = in.readLine()) != null)
            {
                recieveData.append(inputLine);
            }
        }
        catch (IOException e)
        {
        	System.out.println(e.getMessage());
        }
        finally
        {            
            try
            {
                if (null != in)
                {
                    in.close();
                }
            }
            catch (IOException e)
            {
            }            
        }
        
        return recieveData.toString();
    }
	
	
	@Override
	public void parseResponse(GmmsMessage message, String resp) {
		// TODO Auto-generated method stub

	}	
}
