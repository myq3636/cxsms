package com.king.gmms.protocol.commonhttp;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import java.awt.*;
import java.applet.*;
import java.io.*;
import java.util.*;
import java.net.*;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.text.SimpleDateFormat;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.parsers.*;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.xerces.jaxp.DocumentBuilderFactoryImpl;
import org.hibernate.event.SessionEventListenerConfig;
import org.w3c.dom.*;

import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource; 
import javax.xml.transform.stream.StreamResult;






import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.ssl.SslConfiguration;
import com.king.gmms.connectionpool.ssl.SslContextFactory;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpParam;
import com.king.gmms.domain.http.HttpPdu;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageBase;
import com.king.message.gmms.MessageIdGenerator;
import com.king.rest.util.StringUtility;
import com.sun.org.apache.bcel.internal.generic.NEW;

public class YueFanMessageHttpHandler extends HttpHandler {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(YueFanMessageHttpHandler.class);

	public YueFanMessageHttpHandler(HttpInterface hie) {
		super(hie);
	}

	/**
	 * generate submit request data
	 */
	public String makeRequest(GmmsMessage message, String urlEncoding,
			A2PCustomerInfo cst) throws UnsupportedEncodingException {	
		StringBuilder builder =new StringBuilder();
		String postData =createXml(message);
		String chlAccount = cst.getChlAcctNamer();
		String chlPwd = cst.getChlPasswordr();
		String md5Pass = HttpUtils.md5Encrypt(chlPwd);
		String smsType = cst.getSMSOptionHttpCustomParameter();
		builder.append("message=").append(postData)
		       .append("&account=").append(chlAccount)
		       .append("&password=").append(md5Pass)
		       .append("&smsType=").append(smsType);
		return builder.toString();
	}
	
	public  HttpStatus parseRequest(List<GmmsMessage> message,
			HttpServletRequest request) throws ServletException, IOException{
		return null;
	}

	/**
	 * do response for submit request
	 */
	public String makeResponse(HttpStatus hs, GmmsMessage msg,
			A2PCustomerInfo cst) throws IOException, ServletException {
		HttpPdu submitResp = hi.getMoSubmitResponse();
		if (hs == null) {
			hs = subFail;
		}
		return this.generateResponse(hs, msg, submitResp, cst);

	}

	/**
	 * generate response
	 * 
	 * @param hs
	 * @param msg
	 * @param submitResp
	 * @param response
	 * @return
	 */
	private String generateResponse(HttpStatus hs, GmmsMessage msg,
			HttpPdu submitResp, A2PCustomerInfo cst) {

		StringBuffer content = new StringBuffer(60);
		List<HttpParam> respList = submitResp.getParamList();
		String respValue = "";
		for (HttpParam hp : respList) {
			String param = hp.getParam();
			Object value = msg.getProperty(hp.getParam());
			if ("StatusCode".equalsIgnoreCase(param)) {
				respValue = "" + hs.getCode();
			} else if ("StatusText".equalsIgnoreCase(param)) {
				respValue = hs.getText();
			} else {
				respValue = HttpUtils.getParameter(hp, msg, cst);
			}
			respValue = respValue != null ? respValue : hp.getDefaultValue();
    		if(log.isDebugEnabled()){
    			log.debug("hp.getParam()={};value={};respValue={}", hp.getParam(),
					value, respValue);
    		}
			content.append(hp.getOppsiteParam() + "=" + respValue + "&");
			respValue = "";
		}
		String respContent = content.toString();
		if (respContent.endsWith("&")) {
			respContent = respContent.substring(0, respContent.length() - 1);
		}
		return respContent;
	}

	/**
	 * parse submit request and give response
	 */
	public HttpStatus parseRequest(GmmsMessage msg, HttpServletRequest request)
			throws ServletException, IOException {
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
						log.warn(msg, "getSsid by interfaceName {} failed" , protocol);
						return hi
								.mapGmmsStatus2HttpSubStatus(GmmsStatus.AUTHENTICATION_ERROR);
					}
					rssid = alSsid.get(0);

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
					msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
					return hi
							.mapGmmsStatus2HttpSubStatus(GmmsStatus.INVALID_MSG_FORMAT);
				}
				String password = request.getParameter(hi.getPassword());

				log.debug("password={}", password);

				if (password == null || password.trim().length() < 1) {
					msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
					return hi
							.mapGmmsStatus2HttpSubStatus(GmmsStatus.INVALID_MSG_FORMAT);
				}

				csts = gmmsUtility.getCustomerManager().getCustomerBySpID(
						username);
				sInfo = (A2PSingleConnectionInfo) csts;
				if (sInfo == null || !(password.equals(sInfo.getAuthKey()))) {

					log.debug("customerInfo == {} by serverid = {}", sInfo,
							username);

					msg.setStatus(GmmsStatus.AUTHENTICATION_ERROR);
					return hi
							.mapGmmsStatus2HttpSubStatus(GmmsStatus.AUTHENTICATION_ERROR);
				}
			}

			// throttling control process
			try {
				if (!super.checkIncomingThrottlingControl(csts.getSSID(), msg)) {
					return hi.mapGmmsStatus2HttpSubStatus(GmmsStatus.SERVER_ERROR);
				}
			} catch (Exception e) {
				log.warn("Error occur when processing throttling control in CommonMessageHttpHandler.parseRequest",
								e);
			}

			String mtype = null;
			String messageContent = null;
			String udh = null;
			for (HttpParam hp : hi.getMoSubmitRequest().getParamList()) {
				String requestValue = request
						.getParameter(hp.getOppsiteParam());
				String value = requestValue != null ? requestValue : hp
						.getDefaultValue();
				String param = hp.getParam();

				log
						.debug(
								"param name={};OppsiteParam={}; requestvalue = {}; value={}",
								hp.getParam(), hp.getOppsiteParam(),
								requestValue, value);

				if ("deliveryReport".equalsIgnoreCase(param)) {
					if ("1".equalsIgnoreCase(value)) {
						msg.setProperty(param, true);
					} else {
						msg.setProperty(param, false);
					}
				} else if ("expiryDate".equalsIgnoreCase(param)) {
					Date expireDate = parseHttpExpiryDate(csts, hp, value);
					if (expireDate != null) {
						expireDate = gmmsUtility.getGMTTime(expireDate);
						msg.setProperty(param, expireDate);
					}
				} else if ("textContent".equalsIgnoreCase(param)) {// textContent
					messageContent = value;
				} else if ("udh".equalsIgnoreCase(param)) {// udh
					udh = value;
				} else if ("contentType".equalsIgnoreCase(param)) {// contentType
					mtype = value;
				} else if ("scheduleDeliveryTime".equalsIgnoreCase(param)) {
					try {
						Date scheduleDate = parseHttpScheduleDeliveryTime(csts, hp, value);
						if (scheduleDate != null) {
							scheduleDate = gmmsUtility.getGMTTime(scheduleDate);
							msg.setProperty(param, scheduleDate);
						}
					} catch (Exception e) {
						log.warn(msg, "Invalid scheduleDeliveryTime {}", value);
						msg.setStatus(GmmsStatus.INVALID_SCHEDULED_TIME);
						return hi.mapGmmsStatus2HttpSubStatus(GmmsStatus.INVALID_SCHEDULED_TIME);
					}
					
				} else {
					HttpUtils.setParameter(hp, msg, csts, requestValue);
				}
			}

			String gmmsCharset = hi.mapHttpCharset2GmmsCharset(mtype);
			msg.setContentType(gmmsCharset);

			if (MessageBase.AIC_MSG_TYPE_BINARY.equals(gmmsCharset)) {
				msg.setGmmsMsgType(MessageBase.AIC_MSG_TYPE_BINARY);
			}

			if (messageContent == null || "".equalsIgnoreCase(messageContent)) {
				msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
				return hi
						.mapGmmsStatus2HttpSubStatus(GmmsStatus.INVALID_MSG_FORMAT);
			} else {
				this.parseHttpContent(msg, mtype, messageContent, udh);
			}
			if (msg.getRecipientAddress() == null) {
				msg.setStatus(GmmsStatus.RECIPIENT_ADDR_ERROR);
				return hi
						.mapGmmsStatus2HttpSubStatus(GmmsStatus.RECIPIENT_ADDR_ERROR);
			}

			msg.setOSsID(sInfo.getSSID());
			String commonMsgID = MessageIdGenerator.generateCommonMsgID(sInfo
					.getSSID());
			msg.setMsgID(commonMsgID);

			return subSuccess;
		} catch (UnsupportedEncodingException e) {
			log.error(msg, " MessageType()={}" , msg.getMessageType());
			return subFail;
		} catch (Exception e) {
			log.error("common exception!", e);
			return subFail;
		}

	}

	/**
	 * parse submit response
	 */
	public void parseResponse(GmmsMessage msg, String resp) {
		log.info("receive YueFan response is {}, outmsgid is {}", resp, msg.getOutMsgID());
		if (StringUtility.stringIsNotEmpty(resp) 
				&& resp.contains("<stat>r:000<")) {
			msg.setStatus(GmmsStatus.SUCCESS);
			return;
		}else {			
			msg.setStatus(GmmsStatus.UNKNOWN_ERROR);
			return;
		}		
	}
	
	private static String createXml(GmmsMessage message) {
        try {
        	StringWriter sw = new StringWriter();
        	 YueFanMessage yfMessage = new YueFanMessage();
        	 yfMessage.setContent(message.getTextContent());
        	 yfMessage.setPhoneNumber(message.getRecipientAddress());
        	 if (message.getOutMsgID() == null) {
        		 String commonMsgID = MessageIdGenerator.generateCommonStringID();
				message.setOutMsgID(commonMsgID);
			}
        	 yfMessage.setSmsId(message.getOutMsgID());
             JAXBContext jAXBContext = JAXBContext.newInstance(yfMessage.getClass());
             Marshaller marshaller = jAXBContext.createMarshaller();
             marshaller.marshal(yfMessage, sw); 
            return sw.toString();

        } catch (Exception ex) {
        	log.error("create xml exception!", ex);
            return null;
        } 
    }
	

	private static String doHttpPost(String urlString, String requestString, boolean supportHttps ) {
        try {         	
            URL url = new URL(urlString);
            HttpURLConnection conn = (HttpURLConnection)url.openConnection();
            conn.setDoInput(true);
            conn.setDoOutput(true);
            conn.setUseCaches(false);
            conn.setRequestMethod("POST");
            OutputStreamWriter wr = new OutputStreamWriter(conn.getOutputStream());
            wr.write(requestString);
            wr.flush();
            // Get the response
            BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String line;
            String response = "";
            while ((line = rd.readLine()) != null) {
                response += line;
            }
            wr.close();
            rd.close();

            return response;
        } catch (IOException ex) {
            System.err.println(ex); 
            return ex.toString();
        }
    }
	
	public static String post(String url, Map<String, String> paramsMap) {
        HttpClient client = new HttpClient();
        try {
            PostMethod method = new PostMethod(url);
            if (paramsMap != null) {
                NameValuePair[] namePairs = new NameValuePair[paramsMap.size()];
                int i = 0;
                for (Map.Entry<String, String> param : paramsMap.entrySet()) {
                    NameValuePair pair = new NameValuePair(param.getKey(), param.getValue());
                    namePairs[i++] = pair;
                }
                method.setRequestBody(namePairs);
                HttpMethodParams param = method.getParams();
                param.setContentCharset("UTF-8");
            }
            client.executeMethod(method);
            return method.getResponseBodyAsString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }
	
	public static String sendSms(String apikey, String text, String mobile, String URL) throws IOException {
        Map<String, String> params = new HashMap<String, String>();
        params.put("apikey", apikey);
        params.put("text", text);
        params.put("mobile", mobile);
        return post(URL, params);
    }

	public static void main(String[] args) {
		try {
			//String URL = "http://39.106.41.204:11640/Gmms/Common/MTRequest";
	        String URL = "http://39.106.43.83:7032/Gmms/CommonJson/MTRequest";
			//String URL = "http://gwhk.jxintelink.com:11630/Gmms/CommonJson/MTRequest";
			//String URL = "http://sms.jxintelink.com:11830/Gmms/Common/MTRequest";
			//String URL = "http://39.106.43.83:7032/Gmms/DataN/DRReply";
			//String URL = "http://10.202.9.103/Gmms/Common/MTRequest";
			//String URL = "http://127.0.0.1:8080/hello";
			//String URL = "http://47.104.8.201:8088/v2sms.aspx";
			//String URL = "http://47.104.8.201:8088/v2statusApi.aspx";
			//String URL="bd5d94d927387b51c797d056517f0a86";
            String key = "72_201712131308_test";
            String token = "bd5d94d927387b51c797d056517f0a86";
           /* Thread threads[]=new Thread[100];
            for(int i=0;i<100;i++){
            threads[i]=new Thread(){
            	@Override
            	public void run() {
            		String key = "lacc20181213";
                    String token = "bd5d94d927387b51c797d056517f0a86";
                    String URL = "http://39.106.43.83:7032/Gmms/Common/MTRequest";
            		GmmsMessage message = new GmmsMessage();
                	message.setTextContent("现在预订房间有优惠吗？");
                    message.setSenderAddress("00773421114448");
                    message.setRecipientAddress("911142500200");
                    message.setOutMsgID("1000000022");
                    message.setContentType("UnicodeBigUnmarked");
                    String xml = createXml(key, message);                
                    System.out.println(xml);
                    String response = doHttpPost(URL, xml, false);
            	}
            };
            threads[i].start();
            }*/
            //for (int i = 0; i < 1000; i++) {
            	GmmsMessage message = new GmmsMessage();
            	message.setTextContent("现在预订房间有优惠吗？");
                message.setSenderAddress("773421114448");
                message.setRecipientAddress("911142500200");
                message.setOutMsgID("1000000022");
                message.setContentType("UnicodeBigUnmarked");
                String xml = createXml(message);  
                String json = "{\"AUTHENTICATION\": "
                		+ "{ \"PRODUCTTOKEN\": \"72_201712131308_test\"}, "
                		+ "\"MSG\": [ { \"FROM\": \"SenderName\", "
                		+ "\"TO\": \"00447911123456\", "
                		+ "\"REFERENCE\": \"1123456\", "
                		+ "\"DCS\": 0, "
                		+ "\"BODY\": \"Test message1\"}, "
                		+ "{ \"FROM\": \"SenderName\", "
                		+ "\"TO\": \"8618500375454\", "
                		+ "\"DCS\": 8, "
                		+ "\"BODY\": \"中文测试\"}]}";
                String DataN = "628400007,18500375454,DELIVRD|1112090856000111,13900000002,DELIVRD|1112090856000112,13900000003,DELIVRD";
                System.out.println(json);
                String response = doHttpPost(URL, json, false);
                //Thread.sleep(180005);
			//}
            //String string = "A2P%E6%B5%8B%E8%AF%95%E6%AD%A3%E5%B8%B8%E6%B6%88%E6%81%AF%EF%BC%8C%E8%AF%B7%E5%BF%BD%E7%95%A5%EF%BC%8C%E8%B0%A2%E8%B0%A2%EF%BC%81%5B%E5%B0%B9%E7%A7%AF%E4%B8%87%5D";
            //String response = URLDecoder.decode(string, "utf8");
            System.out.println(response);
            
            /*String appkey="huaqing";
            String secretkey="HongZong11";
            String tString = "1505297261994";
            long timestamp = System.currentTimeMillis();
            SimpleDateFormat sdFormat = new SimpleDateFormat(
        			"yyyyMMddHHmmss");
            String dataTime = sdFormat.format(new Date());
            System.out.println(dataTime);
            String signKey = appkey+secretkey+dataTime;
            //System.out.println(signKey);
            String sign =HttpUtils.encrypt(signKey, "MD5");
            //System.out.println(sign);
*/            
			//String URL="http://sms.10690221.com:9011/hy/";
			/*String URL="https://sms.yunpian.com/v2/sms/single_send.json";
            String phone="";
            String content="马风先生/女士因您所在参保单位有变，如您有门诊就医需求，请您于19日前前往居住地就近社康进行重新确认绑定。给你造成不便敬请谅解！详情请咨询壹公里社区服务：4001800502。【壹公里社区服务】";
            String message = URLEncoder.encode(content, "utf8");
            String signaid="";
            String params="106908937534";
            String cid = "50504d09ae80a660fd38148acc523919";
            String pwd = "ejp5mE";
            String auth= HttpUtils.encrypt(cid+pwd, "MD5");
            System.out.println(auth);
            String xml = "apikey="+cid+"&mobile=18500375454"
                         +"&text="+URLEncoder.encode(content, "utf8");
            //String xml = "userid=17&timestamp="+dataTime+"&sign="+sign+"&action=query";
            //String xml = "extno=106908937534&account="+cid+"&msg=abc"+"&mobile=18201109958";
            System.out.println(xml);
            //String response = doHttpPost(URL, xml, false);
            String response = sendSms(cid, content, "18500375454", URL);
            System.out.println("Response: " + response);
			
			/*String a = "{\"result\":\"鎻愪氦鎴愬姛\",\"code\":\"0\",\"info\":\"璇锋眰鎴愬姛\"}";
			String b =a.split("\"code\":\"")[1].split("\"")[0];
			System.out.println(b);*/
            //System.in.read();
            
        } catch (Exception e) {
            System.err.println(e); // Display the string.
        }
	}

}
