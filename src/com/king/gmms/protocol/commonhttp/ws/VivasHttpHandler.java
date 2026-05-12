package com.king.gmms.protocol.commonhttp.ws;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;





import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpParam;
import com.king.gmms.protocol.commonhttp.HttpCharset;
import com.king.gmms.protocol.commonhttp.HttpStatus;
import com.king.gmms.protocol.commonhttp.HttpUtils;
import com.king.gmms.protocol.commonhttp.ws.vivas.MsgObjectRespone;
import com.king.gmms.protocol.commonhttp.ws.vivas.ResponeObject;
import com.king.gmms.protocol.commonhttp.ws.vivas.SendSMSAPI;
import com.king.gmms.protocol.commonhttp.ws.vivas.VerifySMSAPI;
import com.king.gmms.protocol.commonhttp.ws.vivas.VivasClient;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageIdGenerator;

public class VivasHttpHandler extends WebServiceHandler {
	private static SystemLogger log = SystemLogger.getSystemLogger(VivasHttpHandler.class);
	private VivasClient vivasClient = null;
	public VivasHttpHandler(HttpInterface hie){
		super(hie);
	}
	/**
	 * 
	 * @param msg
	 * @param cst
	 * @return
	 */
	public GmmsMessage newRequest(GmmsMessage msg,A2PCustomerInfo cst){
			A2PSingleConnectionInfo singleInfo = (A2PSingleConnectionInfo)cst;
			String wsurl = singleInfo.getChlURL()[0];
			vivasClient = new VivasClient(wsurl);
			msg.setOutClientPull(true);//for query dr
	    	try{
	    		List<HttpParam> params = hi.getMtSubmitRequest().getParamList();
	    		SendSMSAPI instance = new SendSMSAPI();
				MessageIdGenerator idGenerator = MessageIdGenerator.getInstance();
	        	for(HttpParam pm:params){
	        		String par = pm.getParam();
	        		String pval = null;
	        		if("outTransID".equalsIgnoreCase(par)){
	        			pval = idGenerator.generateLongID();
	    			}else if("outMsgID".equalsIgnoreCase(par)){
	    				if(msg.getOutMsgID()!=null&&!"".equals(msg.getOutMsgID())){//if retry
	    					continue;
	    				}
	    				pval = idGenerator.generateLongID();
	    			}else if("msgID".equalsIgnoreCase(par)){
	    				pval = idGenerator.generateLongID();
	    			}else if("deliveryReport".equalsIgnoreCase(par)){
	    				pval = parseHttpDeliveryReport(pm,msg);
	    			}else if("expiryDate".equalsIgnoreCase(par)){
	    				pval = parseGmmsExpiredDate(pm,msg);
	    			}else if("dateIn".equalsIgnoreCase(par)){
	    				pval = getSendTime();
	    			}else if("contentType".equalsIgnoreCase(par)){
	    				HttpCharset httpCharset = hi.mapGmmsCharset2HttpCharset(msg.getContentType());
	    				pval  = httpCharset.getMessageType();
	    			}else  if ("chlPassword".equalsIgnoreCase(par)) {
	    				A2PSingleConnectionInfo sinfo = (A2PSingleConnectionInfo)cst;
	    				String method2encrypt = sinfo.getPasswdEncryptMethod();
	    				if("sha1".equalsIgnoreCase(method2encrypt)){
	    					pval = HttpUtils.shaEncrypt(sinfo.getChlPassword());
	    				}else{
	    					pval = sinfo.getChlPassword();
	    				}
	    			}else  if ("chlPasswordr".equalsIgnoreCase(par)) {
	    				String method2encrypt = cst.getPasswdEncryptMethod();
	    				if("sha1".equalsIgnoreCase(method2encrypt)){
	    					pval = HttpUtils.shaEncrypt(cst.getChlPasswordr());
	    				}else{
	    					pval = cst.getChlPasswordr();
	    				}
	    			}else{
	    				pval = HttpUtils.getParameter(pm, msg, cst);
	    			}
	        		String oproperty = pm.getOppsiteParam();
	        		instance.setProperty(instance,oproperty,pval);
	        	}
				makeChecksum(instance,cst);
				ResponeObject respObj = vivasClient.sendSMSAPI(instance.getArg0(), instance.getArg1(), instance.getArg2(), instance.getArg3()
						, instance.getArg4(), instance.getArg5(), instance.getArg6(), instance.getArg7());
				if(respObj==null){
					msg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
				}else{
					String reqStatus = respObj.getStatus();
					if(log.isInfoEnabled()){
    					log.info(msg,"Received resp with reqStatus:{}",reqStatus);
					}
					HttpStatus httpStatus = new HttpStatus(reqStatus,null);
					GmmsStatus gmmsStatus = hi.mapHttpStatus2GmmsStatus(httpStatus, msg.getMessageType());
					if(gmmsStatus==null){
						msg.setStatus(GmmsStatus.UNKNOWN_ERROR);
					}else{
						msg.setStatus(gmmsStatus);
					}
					msg.setOutMsgID(String.valueOf(respObj.getRequestid()));
				}
				msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
				return msg;
	    	}catch(Exception e){
	    		log.warn("newRequest failed:",e);
	    		if(e.toString().contains("SocketTimeoutException")||e.toString().contains("ConnectException")){
	    			msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
					msg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
					return msg;
	    		}
	    	}
	    	return null;
	    }
	 /**
		 * 
		 * @param msg
		 * @param cst
		 * @return
		 */
	 public GmmsMessage drRequest(GmmsMessage msg,A2PCustomerInfo cst){
		 	A2PSingleConnectionInfo singleInfo = (A2PSingleConnectionInfo)cst;
			String wsurl = singleInfo.getChlURL()[0];
			String drurl = singleInfo.getChlDRURL();
			if(drurl!=null){
				vivasClient = new VivasClient(drurl);
			}else{
				vivasClient = new VivasClient(wsurl);
			}
	    	try{
		    		List<HttpParam> params = hi.getMtSubmitRequest().getParamList();
		    		VerifySMSAPI instance = new VerifySMSAPI();
		        	for(HttpParam pm:params){
		        		String par = pm.getParam();
		        		String pval = null;
		        		if("deliveryReport".equalsIgnoreCase(par)){
		    				pval = parseHttpDeliveryReport(pm,msg);
		    			}else if("expiryDate".equalsIgnoreCase(par)){
		    				pval = parseGmmsExpiredDate(pm,msg);
		    			}else if("dateIn".equalsIgnoreCase(par)){
		    				pval = getSendTime();
		    			}else if("contentType".equalsIgnoreCase(par)){
		    				HttpCharset httpCharset = hi.mapGmmsCharset2HttpCharset(msg.getContentType());
		    				pval  = httpCharset.getMessageType();
		    			}else  if ("chlPassword".equalsIgnoreCase(par)) {
		    				A2PSingleConnectionInfo sinfo = (A2PSingleConnectionInfo)cst;
		    				String method2encrypt = sinfo.getPasswdEncryptMethod();
		    				if("sha1".equalsIgnoreCase(method2encrypt)){
		    					pval = HttpUtils.shaEncrypt(sinfo.getChlPassword());
		    				}else{
		    					pval = sinfo.getChlPassword();
		    				}
		    			}else  if ("chlPasswordr".equalsIgnoreCase(par)) {
		    				String method2encrypt = cst.getPasswdEncryptMethod();
		    				if("sha1".equalsIgnoreCase(method2encrypt)){
		    					pval = HttpUtils.shaEncrypt(cst.getChlPasswordr());
		    				}else{
		    					pval = cst.getChlPasswordr();
		    				}
		    			}else{
		    				pval = HttpUtils.getParameter(pm, msg, cst);
		    			}
		        		String oproperty = pm.getOppsiteParam();
		        		instance.setProperty(instance,oproperty,pval);
		        	}
					ResponeObject respObj = vivasClient.verifySMSAPI(instance.getArg0(), instance.getArg1(), instance.getArg2());
					if(respObj==null){
						msg.setStatus(GmmsStatus.FAIL_QUERY_DELIVERREPORT);
					}else{
						String reqStatus = respObj.getStatus();
						if(log.isInfoEnabled()){
	    					log.info(msg,"Received DR resp with reqStatus:{}",reqStatus);
						}
						if("0".equals(reqStatus)){
							if(respObj.getMsg()==null||respObj.getMsg().size()==0){
								msg.setStatus(GmmsStatus.FAIL_QUERY_DELIVERREPORT);
							}else{
								MsgObjectRespone drMsg = respObj.getMsg().get(0);
								String smsStatus = drMsg.getResult();
								HttpStatus httpStatus = new HttpStatus(smsStatus,null);
								GmmsStatus gmmsStatus = hi.mapHttpStatus2GmmsStatus(httpStatus, msg.getMessageType());
								if(gmmsStatus==null){
									msg.setStatus(GmmsStatus.UNKNOWN);
								}else{
									msg.setStatus(gmmsStatus);
								}
							}
						}else{
							msg.setStatus(GmmsStatus.FAIL_QUERY_DELIVERREPORT);
						}
					}
					msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY_RESP);
					return msg;
		    	}catch(Exception e){
		    		log.warn("drRequest failed:",e);
		    		if(e.toString().contains("SocketTimeoutException")||e.toString().contains("ConnectException")){
		    			msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY_RESP);
						msg.setStatus(GmmsStatus.FAIL_QUERY_DELIVERREPORT);
						return msg;
		    		}
		    	}
		    	return null;
		    }
	/**
	 * 
	 * @param instance
	 */
	private String getSendTime(){
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");  
		TimeZone zone = TimeZone.getTimeZone("GMT+7");
		df.setTimeZone(zone);
		String sendtime = df.format(new Date()); 
		return sendtime;
	}
	/**
	 * make checksum
	 */
	private String makeChecksum(SendSMSAPI instance,A2PCustomerInfo cst) {
		String toMD5 = "username=" + instance.getArg1();
		toMD5 += "&password=" + instance.getArg2();
		toMD5 += "&brandname=" + instance.getArg3();
		toMD5 += "&sendtime=" + instance.getArg5();
		toMD5 += "&msgid=" + instance.getMsgid(); 
		toMD5 += "&msg=" + instance.getArg4();
		toMD5 += "&msisdn=" + instance.getMsisdn();
		toMD5 += "&sharekey=" + cst.getChlAcctNamer();
		String checksum =  HttpUtils.encrypt(toMD5, "MD5");
		instance.setChecksum(checksum);
		return checksum;
	}
	@Override
	public List<GmmsMessage> parseMORequestList(GmmsMessage message,A2PCustomerInfo cst) {
		// TODO Auto-generated method stub
		return null;
	}
	
	
}
