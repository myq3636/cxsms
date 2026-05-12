package com.king.gmms.connectionpool.session;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.ConnectionStatus;
import com.king.gmms.connectionpool.connection.ConnectionManager;
import com.king.gmms.connectionpool.ssl.SslConfiguration;
import com.king.gmms.connectionpool.ssl.SslContextFactory;
import com.king.gmms.customerconnectionfactory.InternalAgentConnectionFactory;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.domain.ConnectionInfo;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.messagequeue.OperatorMessageQueue;
import com.king.gmms.protocol.commonhttp.UnicodeReader;
import com.king.message.gmms.GmmsMessage;


/**
 * <p>
 * Title:
 * </p>
 * 
 * <p>
 * Description:
 * </p>
 * 
 * <p>
 * Copyright: Copyright (c) 2006
 * </p>
 * 
 * <p>
 * Company:
 * </p>
 * 
 * @author not attributable
 * @version 1.0
 */
public abstract class HttpSession extends Session {

	private static SystemLogger log = SystemLogger.getSystemLogger(HttpSession.class);
	protected GmmsUtility gmmsUtility;

	protected HttpURLConnection connection;
	protected String m_strStatus;
	protected String m_strCookie;
	protected int m_nRetCode;
	protected String m_strRetDescription;
	protected String sysId;
	protected long lastActivity = 0;
	public boolean connected = false;

	private int httpConnTimeOut;
	private int httpReadTimeOut;

	protected int port = 0;
	protected String systemId = "";
	protected String password = "";
	protected String charset = "utf8";
	protected String isSupportDeliveryReport = "yes";
	protected int ssId = -1;
	protected ArrayList serverId = new ArrayList();
	protected String foreword = null;
	protected String serverAddress;
	protected String serverDRAddress;
	protected String senderId = "";
	protected String httpMethod = "post";

	protected String module ="";
	
	private SslContextFactory sslContextFactory = null;
	
	public HttpSession() {
		
		gmmsUtility = GmmsUtility.getInstance();
		gmmsUtility.initDBManager(null);
		module = System.getProperty("module");
		charset = "utf8";
	}

	public HttpSession(A2PCustomerInfo info) {
		
		 
		gmmsUtility = GmmsUtility.getInstance();
		A2PSingleConnectionInfo sInfo = (A2PSingleConnectionInfo) info;
		serverAddress = sInfo.getChlURL()[0];
		if (sInfo.getChlDRURL() == null) {
			serverDRAddress = sInfo.getChlURL()[0];
		} else {
			serverDRAddress = sInfo.getChlDRURL();
		}

		systemId = sInfo.getChlAcctName();
		password = sInfo.getChlPassword();
		charset = sInfo.getChlCharset();
		if (charset == null || "".equals(charset)) {
			charset = "utf8";
		}
		senderId = sInfo.getSenderId();
		serverId = sInfo.getSysId();
		ssId = sInfo.getSSID();
		isSupportDeliveryReport = (sInfo.isSupportDeliverReport() ? "yes"
				: "no");
		if (sInfo.getForeword() != null) {
			foreword = sInfo.getForeword();
		}
		httpConnTimeOut = Integer.parseInt(gmmsUtility.getModuleProperty(
				"ConnectTimeout", "30")) * 1000;
		httpReadTimeOut = Integer.parseInt(gmmsUtility.getModuleProperty(
				"ReadTimeout", "60")) * 1000;

		httpMethod = sInfo.getHttpMethod();
		module = System.getProperty("module");
		if(info.isSMSOptionIsSupportHttps()){
			SslConfiguration config = sInfo.getSslConfiguration()== null? gmmsUtility.getSslConfiguration():sInfo.getSslConfiguration();
			try {
				sslContextFactory = new SslContextFactory(config);
			} catch (Exception e) {
				log.error("Initialize SslContextFactory instance failed, and excpeiton is {}",e.getMessage());				
			}
		}

	}

	protected String doGet(URL url, boolean supportHttps) throws IOException {

		try {
			if(supportHttps){
				connection = (HttpsURLConnection) url.openConnection();
				SSLSocketFactory ssf = sslContextFactory.getSslContext().getSocketFactory();
				((HttpsURLConnection)connection).setSSLSocketFactory(ssf);
			}else{
				connection = (HttpURLConnection) url.openConnection();
			}

			connection.setDoInput(true);
			connection.setDoOutput(true);
			connection.setUseCaches(false);
			connection.setRequestMethod("GET");

			connection.setConnectTimeout(this.httpConnTimeOut);
			connection.setReadTimeout(this.httpReadTimeOut);

			InputStream in = connection.getInputStream();
			BufferedReader data = new BufferedReader(new InputStreamReader(in));
			StringBuffer respones = new StringBuffer();
			String line = data.readLine();

			while (line != null) {
				respones.append(line + "\r\n");
				line = data.readLine();
			}
			data.close();
			return respones.toString();
		} catch (IOException ex) {
			log.error(ex, ex);
			throw ex;
		} finally {
			if (connection != null) {
				connection.disconnect();
			}
		}
	}
	
	protected String doQueryMsgWithHeader(URL url, Map<String, String>header, boolean supportHttps) throws Exception {

		try {
			if(supportHttps){							
				HostnameVerifier hv = new HostnameVerifier() {
					public boolean verify(String urlHostName, SSLSession session) {
						// TODO Auto-generated method stub
						log.info("Warning: URL Host: " + urlHostName + " vs "+ session.getPeerHost());
						//return true;
						return true;
					}
			     };
			     trustAllHttpsCertificates();
			     HttpsURLConnection.setDefaultHostnameVerifier(hv);
			     connection = (HttpsURLConnection) url.openConnection();	
			}else{
				connection = (HttpURLConnection) url.openConnection();
			}

			connection.setDoInput(true);
			connection.setDoOutput(true);
			connection.setUseCaches(false);
			connection.setRequestMethod("GET");

			connection.setConnectTimeout(this.httpConnTimeOut);
			connection.setReadTimeout(this.httpReadTimeOut);
			if (header != null) {
				for(String key : header.keySet()){
					connection.setRequestProperty(key, header.get(key));
				}

			}
			InputStream in = connection.getInputStream();
			BufferedReader data = new BufferedReader(new InputStreamReader(in));
			StringBuffer respones = new StringBuffer();
			String line = data.readLine();

			while (line != null) {
				respones.append(line + "\r\n");
				line = data.readLine();
			}
			data.close();
			return respones.toString();
		} catch (Exception ex) {
			log.error(ex, ex);
			throw ex;
		} finally {
			if (connection != null) {
				connection.disconnect();
			}
		}
	}

	protected String doPost(URL url, String post, String cookie, boolean supportHttps)
			throws IOException {
		try {
			if(supportHttps){
				connection = (HttpsURLConnection) url.openConnection();
				SSLSocketFactory ssf = sslContextFactory.getSslContext().getSocketFactory();
				
				HostnameVerifier hv = new HostnameVerifier() {
					public boolean verify(String urlHostName, SSLSession session) {
						// TODO Auto-generated method stub
						log.info("Warning: URL Host: " + urlHostName + " vs "+ session.getPeerHost());
						//return true;
						return urlHostName.equals(session.getPeerHost());
					}
			     };
			    
			    ((HttpsURLConnection)connection).setHostnameVerifier(hv);				
				((HttpsURLConnection)connection).setSSLSocketFactory(ssf);				
			}else{
				connection = (HttpURLConnection) url.openConnection();
			}

			connection.setDoInput(true);
			connection.setDoOutput(true);
			connection.setUseCaches(false);
			connection.setRequestMethod("POST");
			connection.setConnectTimeout(this.httpConnTimeOut);
			connection.setReadTimeout(this.httpReadTimeOut);

			if (cookie != null) {
				connection.setRequestProperty("Cookie", cookie);
			}

			DataOutputStream dos = new DataOutputStream(connection
					.getOutputStream());
			if (post != null) {
				dos.writeBytes(post);
			}
			dos.flush();
			// dos.close();

			BufferedReader br = null;
			if("UTF8".equalsIgnoreCase(charset) || "UTF-8".equalsIgnoreCase(charset)){
				UnicodeReader ur = new UnicodeReader(connection.getInputStream(), "UTF-8");
				br = new BufferedReader(ur);
			}else{
				 InputStreamReader in = new InputStreamReader(connection.getInputStream());
		         br = new BufferedReader(in);
			}			
			
			StringBuffer strResponse = new StringBuffer();
			String readLine;
			while ((readLine = br.readLine()) != null) {
				strResponse.append(readLine.trim() + "\r\n");
			}
			return strResponse.toString();
		} catch (IOException ex) {
			log.error(ex, ex);
			throw ex;
		} finally {
			if (connection != null) {
				connection.disconnect();
			}
		}
	}

	protected String doPost(URL url, String post, Map<String, String> header,String contentType, boolean supportHttps)
			throws Exception {
		try {
			if(supportHttps){							
				HostnameVerifier hv = new HostnameVerifier() {
					public boolean verify(String urlHostName, SSLSession session) {
						// TODO Auto-generated method stub
						log.info("Warning: URL Host: " + urlHostName + " vs "+ session.getPeerHost());
						return true;
						//return urlHostName.equals(session.getPeerHost());
					}
			     };
			    
			     trustAllHttpsCertificates();
			     HttpsURLConnection.setDefaultHostnameVerifier(hv);
			     connection = (HttpsURLConnection) url.openConnection();
			}else{
				connection = (HttpURLConnection) url.openConnection();
			}

			connection.setDoInput(true);
			connection.setDoOutput(true);
			connection.setUseCaches(false);
			connection.setRequestMethod("POST");
			connection.setConnectTimeout(this.httpConnTimeOut);
			connection.setReadTimeout(this.httpReadTimeOut);
			
		
			if (header != null) {
				for(String key : header.keySet()){
					connection.setRequestProperty(key, header.get(key));
				}

			}
			
			if (contentType != null && contentType.length() > 0) {
				connection.setRequestProperty("Content-Type", contentType);
			}
		
			
			//DataOutputStream dos = new DataOutputStream(connection.getOutputStream());
			OutputStreamWriter writer = new OutputStreamWriter(connection.getOutputStream(), "UTF-8");
			if (post != null) {
				writer.write(post); 				
			}
			writer.flush();
			InputStream stream = connection.getErrorStream();
			if(connection.getResponseCode()>399 && stream !=null){
				BufferedReader bf = new BufferedReader(new InputStreamReader(stream));
				StringBuffer strResponse = new StringBuffer();
				String readLine;
				while ((readLine = bf.readLine()) != null) {
					strResponse.append(readLine.trim() + "\r\n");
				}
				log.error("http response error message {}", strResponse.toString());
			}
			BufferedReader br = null;
			if("UTF8".equalsIgnoreCase(charset) || "UTF-8".equalsIgnoreCase(charset)){
				UnicodeReader ur = new UnicodeReader(connection.getInputStream(), "UTF-8");
				br = new BufferedReader(ur);
			}else{
				 InputStreamReader in = new InputStreamReader(connection.getInputStream());
		         br = new BufferedReader(in);
			}
					
			StringBuffer strResponse = new StringBuffer();
			String readLine;
			while ((readLine = br.readLine()) != null) {
				strResponse.append(readLine.trim() + "\r\n");
			}
			return strResponse.toString();
		} catch (Exception ex) {
			log.error(ex, ex);
			throw ex;
		} finally {
			if (connection != null) {
				connection.disconnect();
			}
		}
	}
	
	protected String doMNPquery(URL url, String post, Map<String, String> header,String contentType, boolean supportHttps)
			throws Exception {
		try {
			if(supportHttps){							
				HostnameVerifier hv = new HostnameVerifier() {
					public boolean verify(String urlHostName, SSLSession session) {
						// TODO Auto-generated method stub
						log.info("Warning: URL Host: " + urlHostName + " vs "+ session.getPeerHost());
						return true;
						//return urlHostName.equals(session.getPeerHost());
					}
			     };
			    
			     trustAllHttpsCertificates();
			     HttpsURLConnection.setDefaultHostnameVerifier(hv);
			     connection = (HttpsURLConnection) url.openConnection();
			}else{
				connection = (HttpURLConnection) url.openConnection();
			}

			connection.setDoInput(true);
			connection.setDoOutput(true);
			connection.setUseCaches(false);
			connection.setRequestMethod("POST");
			connection.setConnectTimeout(30000);
			connection.setReadTimeout(30000);
			
		
			if (header != null) {
				for(String key : header.keySet()){
					connection.setRequestProperty(key, header.get(key));
				}

			}
			
			if (contentType != null && contentType.length() > 0) {
				connection.setRequestProperty("Content-Type", contentType);
			}
		
			
			//DataOutputStream dos = new DataOutputStream(connection.getOutputStream());
			OutputStreamWriter writer = new OutputStreamWriter(connection.getOutputStream(), "UTF-8");
			if (post != null) {
				writer.write(post); 				
			}
			writer.flush();
			InputStream stream = connection.getErrorStream();
			if(connection.getResponseCode()>399 && stream !=null){
				BufferedReader bf = new BufferedReader(new InputStreamReader(stream));
				StringBuffer strResponse = new StringBuffer();
				String readLine;
				while ((readLine = bf.readLine()) != null) {
					strResponse.append(readLine.trim() + "\r\n");
				}
				log.error("http response error message {}", strResponse.toString());
			}
			BufferedReader br = null;
			if("UTF8".equalsIgnoreCase(charset) || "UTF-8".equalsIgnoreCase(charset)){
				UnicodeReader ur = new UnicodeReader(connection.getInputStream(), "UTF-8");
				br = new BufferedReader(ur);
			}else{
				 InputStreamReader in = new InputStreamReader(connection.getInputStream());
		         br = new BufferedReader(in);
			}
					
			StringBuffer strResponse = new StringBuffer();
			String readLine;
			while ((readLine = br.readLine()) != null) {
				strResponse.append(readLine.trim() + "\r\n");
			}
			return strResponse.toString();
		} catch (Exception ex) {
			log.error(ex, ex);
			throw ex;
		} finally {
			if (connection != null) {
				connection.disconnect();
			}
		}
	}

	public static String post(String url, String params) {
        HttpClient client = new HttpClient();
        try {
            PostMethod method = new PostMethod(url);
           
            if (params != null) {
            	Map<String, String> paramsMap = new HashMap<>();
            	String[]paramString = params.split("&");
            	for (int i = 0; i < paramString.length; i++) {
            		int index = paramString[i].indexOf("=");
            		int length = paramString[i].length();
					if (index>0 && index<length-1) {
						paramsMap.put(paramString[i].substring(0, index), paramString[i].substring(index+1, length));						
					}
				}
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
	
	protected String verifyUrl(String url) {
		if (!url.toLowerCase().startsWith("http://")) {
			url = "http://" + url;
		}
		if (!url.endsWith("/")) {
			url = url + "/";
		}
		return url;
	}

	/**
	 * @param src
	 *            String
	 * @return String
	 * @throws UnsupportedEncodingException
	 */
	protected String urlEncode(String src) throws UnsupportedEncodingException {
		if (src == null) {
			log.debug("src == null!");

			return null;
		}
		return java.net.URLEncoder.encode(src, charset);
	}

	/**
	 * Encode the data to be submitted to CHT server
	 * 
	 * @param src
	 *            byte[]
	 * @return String
	 */
	protected String urlEncode(byte[] src) {
		String retstr = new String();
		for (byte aSrc : src) {
			if (aSrc < 0) { // src[i] >= 0x80
				String hh = Integer.toHexString((int) (aSrc & 0xff));
				retstr += "%" + hh;
			} else {
				char cc = (char) aSrc;

				if (Character.isLetterOrDigit(cc)) {
					retstr += cc;
				} else if (cc == ' ') {
					retstr += '+';
				} else if (cc == '.' || cc == '-' || cc == '*' || cc == '_') {
					retstr += cc;
				} else {
					String hh = Integer.toHexString((int) (aSrc & 0xff));
					if (aSrc < 0x10) {
						retstr += "%0" + hh;
					} else {
						retstr += "%" + hh;
					}
				}
			}
		}
		return retstr;
	}

	/**
	 * put message to Router queue
	 * 
	 * @param msg
	 */
	protected boolean putGmmsMessage2RouterQueue(GmmsMessage msg) {
		if (msg == null) {
			return false;
		}
		ModuleManager moduleManager = ModuleManager.getInstance();
		InternalAgentConnectionFactory factory = InternalAgentConnectionFactory
				.getInstance();
		String routerQueue = null;	
		String deliveryChannelQueue = null;
		OperatorMessageQueue msgQueue = null;
		if (GmmsMessage.MSG_TYPE_INNER_ACK.equalsIgnoreCase(msg
				.getMessageType())
				|| GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(msg
						.getMessageType())
				|| GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP
						.equalsIgnoreCase(msg.getMessageType())
				|| GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY_RESP
						.equalsIgnoreCase(msg.getMessageType())) {
			if (innerTransaction == null) {
				if(log.isInfoEnabled()){
					log.info(msg, "Cannot get the inner transaction");
				}
				return false;
			}
			
			// V4.1 Redirect all responses to Redis Results Stream
			if (log.isInfoEnabled()) {
				log.info(msg, "Sending {} to Redis results stream", msg.getMessageType());
			}
			return com.king.gmms.messagequeue.StreamQueueManager.getInstance().produceResult(msg);
		} else {
			routerQueue = moduleManager.selectRouter(msg);
			msgQueue = factory.getMessageQueue(msg, routerQueue);
			deliveryChannelQueue = routerQueue;
			if (msgQueue == null) {
				String aliveRouterQueue = moduleManager.selectAliveRouter(routerQueue, msg);
				msgQueue = factory.getMessageQueue(msg, aliveRouterQueue);
				if(msgQueue == null){
            		ArrayList<String> failedRouters = new ArrayList<String>();
            		failedRouters.add(routerQueue);
            		failedRouters.add(aliveRouterQueue);
            		aliveRouterQueue = moduleManager.selectAliveRouter(failedRouters, msg);
            		while(aliveRouterQueue != null){
            			msgQueue = factory.getMessageQueue(msg, aliveRouterQueue);
            			if(msgQueue == null){
            				failedRouters.add(aliveRouterQueue);
            				aliveRouterQueue = moduleManager.selectAliveRouter(failedRouters, msg);
            			}else{
            				break;
            			}
            		}
            	}
				deliveryChannelQueue = aliveRouterQueue;
			}
			if(log.isInfoEnabled()){
				log.info(msg, "Send {} to {}",msg.toString(), deliveryChannelQueue);
			}
		}
		
		if (msgQueue == null) {
			log.warn(msg, "Can not find the alive delivery router");
			msg.setDeliveryChannel(module);
			return false;
		} else {
			msg.setDeliveryChannel(module+":"+deliveryChannelQueue);
			return msgQueue.putMsg(msg);
		}
	}

	/**
	 * submit
	 * 
	 * @param msg
	 *            GmmsMessage
	 * @return boolean
	 * @throws IOException
	 * @todo Implement.gmms.connectionpool.session.Session
	 *       method
	 */
	public abstract boolean submit(GmmsMessage msg) throws IOException;

	/**
	 * submitAndRec
	 * 
	 * @param msg
	 *            GmmsMessage
	 * @return ByteBuffer
	 * @throws IOException
	 * @todo Implement.gmms.connectionpool.session.Session
	 *       method
	 */
	public abstract ByteBuffer submitAndRec(GmmsMessage msg) throws IOException;

	protected abstract StringBuffer appendData(GmmsMessage message);

	// @Override
	public boolean connect() {
		// TODO Auto-generated method stub
		return false;
	}

	// @Override
	public boolean createConnection() throws IOException {
		// TODO Auto-generated method stub
		return true;
	}

	// @Override
	public int enquireLink() throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	// @Override
	public ConnectionInfo getConnectionInfo() {
		// TODO Auto-generated method stub
		return null;
	}

	// @Override
	public long getLastActivity() {
		// TODO Auto-generated method stub
		return 0;
	}

	// @Override
	public OperatorMessageQueue getOperatorMessageQueue() {
		// TODO Auto-generated method stub
		return null;
	}

	// @Override
	public String getSessionName() {
		// TODO Auto-generated method stub
		return null;
	}

	// @Override
	public int getSessionNum() {
		// TODO Auto-generated method stub
		return 0;
	}

	// @Override
	public ConnectionStatus getStatus() {
		// TODO Auto-generated method stub
		return null;
	}

	// @Override
	public TransactionURI getTransactionURI() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void initReceivers() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateReceivers() {
		// TODO Auto-generated method stub
		
	}
	
	// @Override
	public void initSenders() {
		// TODO Auto-generated method stub

	}

	// @Override
	public boolean isKeepRunning() {
		// TODO Auto-generated method stub
		return false;
	}

	// @Override
	public boolean isServer() {
		// TODO Auto-generated method stub
		return false;
	}

	// @Override
	public void setConnectionManager(ConnectionManager connManager) {
		// TODO Auto-generated method stub

	}

	// @Override
	public void setKeepRunning(boolean keepRunning) {
		// TODO Auto-generated method stub

	}

	// @Override
	public void setSessionName(String connectionName) {
		// TODO Auto-generated method stub

	}

	// @Override
	public void setStatus(ConnectionStatus status) {
		// TODO Auto-generated method stub

	}

	// @Override
	public void stop() {
		// TODO Auto-generated method stub

	}

	// @Override
	public void connectionUnavailable() {
		// TODO Auto-generated method stub

	}

	// @Override
	public void parse(ByteBuffer buffer) {
		// TODO Auto-generated method stub

	}

	// @Override
	public boolean receive(Object obj) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean isFakeSession() {
		return false;
	}
	
	public static void trustAllHttpsCertificates() throws Exception {
	    TrustManager[] trustAllCerts = new TrustManager[1];
	    TrustManager tm = new miTM();
	    trustAllCerts[0] = tm;
	    SSLContext sc = SSLContext.getInstance("SSL");
	    sc.init(null, trustAllCerts, null);
	    HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
	}
	 
	static class miTM implements TrustManager,X509TrustManager {
	    public X509Certificate[] getAcceptedIssuers() {
	        return null;
	    }
	 
	    public boolean isServerTrusted(X509Certificate[] certs) {
	        return true;
	    }
	 
	    public boolean isClientTrusted(X509Certificate[] certs) {
	        return true;
	    }
	 
	    public void checkServerTrusted(X509Certificate[] certs, String authType)
	            throws CertificateException {
	        return;
	    }
	 
	    public void checkClientTrusted(X509Certificate[] certs, String authType)
	            throws CertificateException {
	        return;
	    }
	}
	
}
