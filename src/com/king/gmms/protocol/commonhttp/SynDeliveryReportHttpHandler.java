package com.king.gmms.protocol.commonhttp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
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

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.xml.sax.InputSource;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
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
import com.king.message.gmms.MessageIdGenerator;
import com.king.message.gmms.MessageStoreManager;
import com.king.redis.RedisClient;
import com.mysql.jdbc.log.Log;

public class SynDeliveryReportHttpHandler extends HttpHandler {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(SynDeliveryReportHttpHandler.class);		
	
	public SynDeliveryReportHttpHandler(HttpInterface hie) {
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
		http://客户url?phone=138001380000&msgid=123&nstat=0&errcode= DELIVRD&revTime=2017-01-01 10:10:30
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

				String drXml = recieveData(request);
				//JSONObject jsonObj = JSON.parseObject(drJson);
				//SmsSid=SM3d8ce25cc7124481ba51aa58c88d8315&SmsStatus=
				//delivered&MessageStatus=delivered&To=%2B8618500375454&MessagingServiceSid=MGe31a09545f784d81806ae35fafb5efa8&MessageSid=SM3d8ce25cc7124481ba51aa58c88d8315&Acco
				//untSid=ACbf917939c3fc962de8450de3cdbb2be2&From=%2B17605469515&ApiVersion=2010-04-01
				log.debug("receive dr request {}", drXml);
				Map<String, String> map = domXml(drXml);
				String statusCode = null;
				String statusText = null;
				for (HttpParam hp : hi.getMtDRRequest().getParamList()) {
					String param = hp.getParam();
					String value = map.get(hp.getOppsiteParam());

					log.debug("param name={};OppsiteParam={}; value={}",
									hp.getParam(), hp.getOppsiteParam(), value);

					if ("statusText".equalsIgnoreCase(param)) {
						statusText = value;
					} else if ("statusCode".equalsIgnoreCase(param)) {
						statusCode = value;
					} else {
						message.setProperty(hp.getParam(), value);
					}
				}
				GmmsStatus gs = processStatus(statusCode, statusText);
				message.setStatus(gs);
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
		return null;

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
	
	private Map<String, String> domXml(String xml) {
		StringReader reader = null;
		Map<String, String> result = new HashMap<String, String>();
		try {
			reader = new StringReader(xml);
			InputSource source = new InputSource(reader);
			SAXReader sb = new SAXReader();
			Document doc = sb.read(source);
			Element root = doc.getRootElement();
			Element statusNode = root.element("statuscode");
			Element trackingNode = root.element("trackingid");
			Element destinatNode = root.element("destination");
			String statuscode = statusNode.getText();
			String trackingid = trackingNode.getText();
			String destination = destinatNode.getText();
			result.put("statuscode", statuscode);
			result.put("trackingid", trackingid);
			result.put("destination", destination);
		} catch (Exception e) {
			log.warn(" dom xml format error, and Exception is {}", e
					.getMessage());
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
}
