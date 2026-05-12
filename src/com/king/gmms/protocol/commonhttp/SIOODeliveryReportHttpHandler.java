package com.king.gmms.protocol.commonhttp;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;





import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;






import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpParam;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

public class SIOODeliveryReportHttpHandler extends HttpHandler {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(SIOODeliveryReportHttpHandler.class);

	private static final String SUCCESS = "SUCCESS";  // if handle class has not parameter mapping, return SUCCESS
	public SIOODeliveryReportHttpHandler(HttpInterface hie) {
		super(hie);
	}

	/**
	 * generate DR request data
	 */
	public String makeRequest(GmmsMessage message, String urlEncoding,
			A2PCustomerInfo cst) throws UnsupportedEncodingException {
		
		return SUCCESS; 
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
	
	public HttpStatus parseListRequest(List<GmmsMessage> msgs,
			HttpServletRequest request) {
		try {
			int rssid = -1;

			A2PCustomerInfo csts = null;
			A2PSingleConnectionInfo sInfo = null;
			String protocol = hi.getInterfaceName();
			if (protocol != null && protocol.trim().length() > 0) {
				ArrayList<Integer> alSsid = gmmsUtility.getCustomerManager()
						.getSsidByProtocol(protocol);
				if (alSsid == null || alSsid.size() < 1) {
					log
							.warn(
									"getSsid by interfaceName {} failed in parseListRequest() function.",
									protocol);
					return drFail;
				}
				rssid = alSsid.get(0);
				csts = gmmsUtility.getCustomerManager()
						.getCustomerBySSID(rssid);
				sInfo = (A2PSingleConnectionInfo) csts;
			}

			if (sInfo == null) {
				return drFail;
			}

			String[] messages = null;
			String args = recieveData(request);
			log.info("get the SIOO Delivery report message is {}", args);
			try {
				args = URLDecoder.decode(args, "UTF-8");
			} catch (Exception e) {
				try {
					args = URLDecoder.decode(args, "ASCII");
				} catch (Exception e2) {
					// TODO: handle exception
				}	
			}
			// throttling control process
			if (!super.checkIncomingThrottlingControl(csts.getSSID(), args)) {
				return drFail;
			}

			if (args != null && !("").equals(args.trim())) {
				messages = args.split(";");
			} else {
				return drFail;
			}

			for (String msg : messages) {
				String[] parmeters = msg.split(",");
				log.debug(msg);
				if (parmeters.length != 4) {
					return drFail;
				} else {
					String timemark = parmeters[0];
					String outmsgid = parmeters[1];
					String mobile = parmeters[2];
					String statustext = parmeters[3];

					GmmsMessage drMsg = new GmmsMessage();
					drMsg.setMsgID(outmsgid);
					drMsg.setOutMsgID(outmsgid);
					drMsg.setRecipientAddress(mobile);
					try {
						SimpleDateFormat sdFormat = new SimpleDateFormat(
			        			"yyyyMMddHHmmss");
						Date drDate = sdFormat.parse(timemark);
						drMsg.setDateIn(drDate);
					} catch (Exception e) {
						log.warn("date format is invalid! timemark="+timemark);
					}
					
					GmmsStatus gs = null;
					try {
						gs = processStatus("0", statustext);
					} catch (Exception e) {
						log
								.error(drMsg,
										"process message status error in parseListRequest() function." + e.toString());
					}
					drMsg.setStatus(gs);
					if(log.isDebugEnabled()){
		        		log.debug(drMsg, "outmsgid:{}" , outmsgid);
					}
					drMsg.setRSsID(rssid);
					msgs.add(drMsg);
				}
			}

			return drSuccess;
		} catch (Exception e) {
			log.error(e, e);
			return drFail;
		}
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

	/**
	 * parse DR response
	 */
	public List<GmmsMessage> parseListResponse(GmmsMessage message, A2PCustomerInfo cm, String resp) {
		String []messages = null;        
		List<GmmsMessage> msgs = new ArrayList<GmmsMessage>();
		if (resp != null && !("").equals(resp.trim())) {			
			if(resp.contains("error")){
				if(log.isInfoEnabled()){
					log.info(message, "MeiLian query DR error!");
				}
				
				return null;
			}else if(resp.contains("no record")){
				if(log.isInfoEnabled()){
					log.info(message, "no record for MeiLian interface query!");
				}				
				return null;
			}
			messages = resp.trim().split(";");
		} else {
			return null;
		}

		for (String msg : messages) {
			String[] parmeters = msg.split(",");
			if (parmeters.length != 5) {
				continue;
			} else {
				GmmsMessage drMsg = new GmmsMessage();
				List<HttpParam> respList = hi.getMtDRResponse().getParamList();
				for(int i = 0;i<respList.size(); i++){
					HttpParam param = respList.get(i);
					if(!"StatusText".equals(param.getParam())){
						drMsg.setProperty(param.getParam(), parmeters[i]);
					}else{						
						HttpStatus status = new HttpStatus(null,parmeters[i]);
						GmmsStatus gs = hi.mapHttpDRStatus2GmmsStatus(status);
						drMsg.setStatus(gs);
					}
					
				}				
				if(log.isDebugEnabled()){
	        		log.debug(drMsg, "outmsgid:{}" , drMsg.getOutMsgID());
				}				
				msgs.add(drMsg);
			}
		}

		return msgs;

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

	}
    public static void main(String[] args) {
    	//String time = "2012-11-15 18:19:52";
    	//SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
		try {
			//System.out.println(dateFormat.parse(time));
			String string="20171215184417%2C705118673%2C18201109958%2CDELIVRD%3B";
			String response = URLDecoder.decode(string, "utf-8");
			System.out.println(response);
		} catch (Exception e) {
			log.error("{} convert time error! ");
		}
		
	}
}
