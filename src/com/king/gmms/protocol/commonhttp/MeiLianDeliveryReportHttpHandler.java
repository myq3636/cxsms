package com.king.gmms.protocol.commonhttp;


import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;





import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;






import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpParam;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

public class MeiLianDeliveryReportHttpHandler extends HttpHandler {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(MeiLianDeliveryReportHttpHandler.class);

	private static final String SUCCESS = "SUCCESS";  // if handle class has not parameter mapping, return SUCCESS
	public MeiLianDeliveryReportHttpHandler(HttpInterface hie) {
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
	
    @Override
	public void parseResponse(GmmsMessage message, String resp) {		

	}
    public static void main(String[] args) {
    	String time = "2012-11-15 18:19:52";
    	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
		try {
			System.out.println(dateFormat.parse(time));
		} catch (ParseException e) {
			log.error("{} convert time error! ");
		}
		
	}
}
