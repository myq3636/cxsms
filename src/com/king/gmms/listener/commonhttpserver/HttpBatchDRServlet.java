package com.king.gmms.listener.commonhttpserver;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.king.framework.SystemLogger;
import com.king.gmms.customerconnectionfactory.CommonHttpServerFactory;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.domain.HttpInterfaceManager;
import com.king.gmms.domain.http.HttpConstants;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpPdu;
import com.king.gmms.messagequeue.OperatorMessageQueue;
import com.king.gmms.protocol.commonhttp.HttpStatus;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

@SuppressWarnings("serial")
public class HttpBatchDRServlet extends CommonHttpDRServlet {

	private static SystemLogger log = SystemLogger.getSystemLogger(HttpBatchDRServlet.class);
	private static A2PCustomerManager ctm = null;
	private static HttpInterfaceManager him = null;
	private static CommonHttpServerFactory factory = null;
	
	
	/**
	 * init factory
	 */
	public void init(){
		super.init();
		ctm = gmmsUtility.getCustomerManager();
		him = gmmsUtility.getHttpInterfaceManager();
		factory = CommonHttpServerFactory.getInstance();
	}

	public void processRequest(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
		if(log.isTraceEnabled()){
			log.trace("Received a message from url: {}", request.getRequestURL());
		}
		
		String requestURL = request.getRequestURL().toString();
		String[] args = requestURL.split("/");
		int interfaceIndex = args.length -2;
		String interfaceName = args[interfaceIndex];
		
		log.debug("interfaceName = {}", interfaceName);
		
		him.initHttpInterfaceMap(interfaceName);
		
		List<GmmsMessage> msgs =  new ArrayList<GmmsMessage>();
		HttpStatus hs = this.parseListDrRequest(interfaceName, msgs, request, response);
		
		ServletResponseParameter responseParameter = new ServletResponseParameter(interfaceName,
																		hs,request,response,this);
		HttpInterface hi = him.getHttpInterfaceMap().get(interfaceName);
		if(hi!=null){			
			if(!msgs.isEmpty()){
				this.response(interfaceName, hs, msgs, response, responseParameter);
			}else{//isn't DELEVRED DR status
				this.response(interfaceName, hs,response);
			}
		}else{//hi is null
			log.warn(" get null HttpInterface when name is:{}",interfaceName);
			this.response(interfaceName, hs, response);
		}
	}
		
	public void response(String interfaceName, HttpStatus hs, GmmsMessage msg, 
			HttpServletResponse response,ServletResponseParameter responseParameter) throws ServletException, IOException {
		
		msg.setMessageType(GmmsMessage.MSG_TYPE_INNER_ACK);
		putGmmsMessage2RouterQueue(msg);

		synchronized(responseParameter){
			responseParameter.notifyAll();
		}
	}
	
	//process with right status response
	public void response(String interfaceName, HttpStatus hs, List<GmmsMessage> msgs, 
			HttpServletResponse response,ServletResponseParameter responseParameter) throws ServletException, IOException {
		
		String respContent = hs.getCode();
		if(log.isTraceEnabled()){
			log.trace("give DR right status response of {}:{}" ,interfaceName,respContent);
		}
		OutputStream o = null;
		try {
				o = response.getOutputStream();
				if("vocom".equalsIgnoreCase(interfaceName)){
					o.write("OK".getBytes());
				}else if (respContent == null) {
					o.write("".getBytes());
				}else {
					o.write(respContent.getBytes());
				}
				
				o.flush();
				
				for(int i=0;i<msgs.size();i++){
					GmmsMessage message = msgs.get(i);
					message.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
					//factory.putServletParam(message.getMsgID(), responseParameter);
					if(!putGmmsMessage2RouterQueue(message)){	
						message.setStatus(GmmsStatus.ENROUTE);						
						gmmsUtility.getCdrManager().logOutDeliveryReportRes(message);
						//factory.removeServlet(message.getMsgID());
					}/*else{
						try{
							synchronized(responseParameter){
								responseParameter.wait(timeout);
							}
						}catch(Exception e){
							log.warn(message,"Fail to waiting for the response");
						}
					}*/
				}
				
		} catch (Exception e) {
			log.error(e, e);
		} finally {
			if (o != null) {
				o.close();
			}
		}			
	}
	
	//process with wrong status response
	public void response(String interfaceName,HttpStatus hs, HttpServletResponse response) throws ServletException, IOException {
		
		if(hs == null){
			hs = new HttpStatus("10900",null);
		}
		String respContent = hs.getCode();
		if(log.isTraceEnabled()){
			log.trace(" give DR wrong status response of {}:{}",interfaceName,respContent);
		}
		OutputStream o = null;
		try {
			o = response.getOutputStream();
			if("vocom".equalsIgnoreCase(interfaceName)){
				o.write("OK".getBytes());
			}else{
				o.write(respContent.getBytes());
			}
			
			o.flush();
		} catch (Exception e) {
			log.error(e, e);
		} finally {
			if (o != null) {
				o.close();
			}
		}
	}

	private HttpStatus parseListDrRequest(String interfaceName,List<GmmsMessage>  msgs, HttpServletRequest request, HttpServletResponse response)throws IOException, ServletException  {
		HttpStatus hs = null;
		HttpInterface hi = him.getHttpInterfaceMap().get(interfaceName);
		if(hi == null){
			return hs;
		}
		HttpPdu drRequest = hi.getMtDRRequest();
		if(drRequest.hasHandlerClass()){
			String className = drRequest.getHandlerClass();
			Object[] args= {msgs,request};
			hs = (HttpStatus)hi.invokeHandler(className, HttpConstants.HANDLER_METHOD_PARSELISTREQUEST, args);
    		if(log.isDebugEnabled()){
				log.debug("Used handlerclass:{},invoke method:{} in parseListDrRequest() function.",className,HttpConstants.HANDLER_METHOD_PARSELISTREQUEST);
    		}
        	
		}else{
			log.warn(" drRequest does not configure class name!");
		}
		return hs;
	}
	
}
