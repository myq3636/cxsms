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
import com.king.message.gmms.MessageStoreManager;
import com.king.redis.RedisClient;

public class SouthDeliveryReportHttpHandler extends HttpHandler {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(SouthDeliveryReportHttpHandler.class);		
	
	public SouthDeliveryReportHttpHandler(HttpInterface hie) {
		super(hie);
		
	}

	/**
	 * generate DR request data
	 */
	public String makeRequest(GmmsMessage message, String urlEncoding,
			A2PCustomerInfo cst) throws UnsupportedEncodingException {						
		return "";
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
			return this.doJson(cm, resp);
		} else {
			return null;
		}

	}
	

	private List<GmmsMessage> doJson(A2PCustomerInfo cm, String json) {
		StringReader reader = null;
		List<GmmsMessage> messages = new ArrayList<GmmsMessage>();
		List<HttpParam> parameters = hi.getMtDRResponse().getParamList();		 
		try {
			/*{ "result":0, "message":"", "data":[
			{ "messageId":"0000000001", "src":"123456", 
				"dst":"86xxxxx", "submitTime":"2020-04-09 13:54:54", 
				"deliverTime":"2020-04-09 13:56:30", "status":1, "content":"test", "task":""
			}, ]
			}*/
			
			JSONObject object = JSONObject.parseObject(json);
			if (object.get("data")!=null&&"0".equalsIgnoreCase(String.valueOf(object.get("result")))) {
				List<Map<String, Object>> datas = (List<Map<String, Object>>)object.get("data");
				for (Map<String, Object> map : datas) {
					try {
						GmmsMessage msg = new GmmsMessage();				
						String outmsgid = String.valueOf(map.get("task"));
						msg.setOutMsgID(outmsgid);
						try {
							SimpleDateFormat sdFormat = new SimpleDateFormat(
				        			"yyyy-MM-dd HH:mm:ss");
							Date drDate = sdFormat.parse(String.valueOf(map.get("deliverTime")));
							msg.setDateIn(drDate);
						} catch (Exception e) {						
							log.error("convert deliveryTime is null.");
						}
						
						GmmsStatus gs = null;
						try {
							gs = processStatus(String.valueOf(map.get("status")), String.valueOf(map.get("status")));
						} catch (Exception e) {
							log.error(msg,
											"process message status error in parseListRequest() function." + e.toString());
						}
						msg.setStatus(gs);
						if(log.isInfoEnabled()){
							log.info(msg, "Receive DR response and the outMsgId is {}, and statuscode is {}, recipientAddress is {}"
								          , msg.getOutMsgID(), msg.getStatusCode() ,msg.getRecipientAddress());
						}
						messages.add(msg);
					} catch (Exception e) {
						// TODO: handle exception
					}					
				}
			}
		} catch (Exception e) {
			log.warn("exception{}", e.getMessage());
			return messages;
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
	
	private GmmsStatus processStatus(String statusCode, String statusText)
			throws Exception {
		if(log.isDebugEnabled()){
			log.debug("DR statuscode={},statusText={}", statusCode, statusText);
		}
		GmmsStatus gs = GmmsStatus.UNKNOWN;
		try {
			HttpStatus hs = new HttpStatus(statusCode, statusText);
			gs = hi.mapHttpDRStatus2GmmsStatus(hs);
		} catch (NumberFormatException e) {
			log.error("Parse status code error!", e);			
		} catch (Exception e) {
			log.error("Process status error!", e);			
		}
		return gs;
	}
	
	
	@Override
	public void parseResponse(GmmsMessage message, String resp) {
		// TODO Auto-generated method stub

	}	
}
