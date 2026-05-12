package com.king.gmms.protocol.commonhttp.ws;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.rpc.ServiceException;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpParam;
import com.king.gmms.protocol.commonhttp.HttpStatus;
import com.king.gmms.protocol.commonhttp.HttpUtils;
import com.king.gmms.protocol.commonhttp.ws.emay.MT;
import com.king.gmms.protocol.commonhttp.ws.emay.Mo;
import com.king.gmms.protocol.commonhttp.ws.emay.SDKServiceBindingStub;
import com.king.gmms.protocol.commonhttp.ws.emay.SDKService_ServiceLocator;
import com.king.gmms.protocol.commonhttp.ws.emay.StatusReport;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageIdGenerator;

public class EmayHttpHandler extends WebServiceHandler {

	private static SystemLogger log = SystemLogger.getSystemLogger(EmayHttpHandler.class);
	public SDKService_ServiceLocator serviceLocator = null;
	public SDKServiceBindingStub moRequSDKClient = null; 
	public SDKServiceBindingStub mtRequSDKClient = null;
	public SDKServiceBindingStub mtDrSDKClient = null;
	
	public EmayHttpHandler(HttpInterface hie) {
		super(hie);
		serviceLocator = new SDKService_ServiceLocator();			
	}

	@Override
	public GmmsMessage drRequest(GmmsMessage message, A2PCustomerInfo cst) {
		
		return null;
	}

	public List<GmmsMessage> parseMTDRRequestList(GmmsMessage message,A2PCustomerInfo cst){
		
		A2PSingleConnectionInfo singleInfo = (A2PSingleConnectionInfo)cst;
		String softwareSerialNo = singleInfo.getChlAcctName();
		String key = singleInfo.getChlPassword();
		
		List<GmmsMessage> list = null;
		try {
			if(mtDrSDKClient == null){
				mtDrSDKClient = serviceLocator.initSDKClient(cst); 
			}
			
		} catch (ServiceException e) {
			 log.error("Construct SDK Client instance failed when invoke parseListRequest(), the exception:"+e);
	         return null;
		}
		StatusReport[] statusReport = null;
        try {
        	statusReport = mtDrSDKClient.getReport(softwareSerialNo,key);
        	
		} catch (RemoteException e) {
			log.error("invoke getReport() of SDK failed, the exception is:"+e);
			return null;
		}
		
		List<HttpParam> params = hi.getMtDRRequest().getParamList();
		
		if(statusReport != null){
			list = new ArrayList<GmmsMessage>();
			for(StatusReport report:statusReport){
				
				GmmsMessage msg = new GmmsMessage();
				msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
				String drStatus = null;
				for (HttpParam hp : params) {
					String pval = hp.getParam();
					String mval = null;
					String oproperty = hp.getOppsiteParam();
					if ("StatusCode".equalsIgnoreCase(pval)) {
						Object  o = report.getProperty(oproperty);
						if(o == null){
							msg.setStatus(GmmsStatus.INVALID_MSG_FIELD);
							log.error("Received DR request with StatusCode is null from Emay.");
						}else{
							Integer reportStatus = (Integer)o;
							drStatus = String.valueOf(reportStatus);							
							HttpStatus httpStatus = null;
							if("0".equalsIgnoreCase(drStatus)){
								httpStatus = new HttpStatus(drStatus,null);								
							}else{
								String errCode = report.getErrorCode();
								httpStatus = new HttpStatus(errCode,null);	
							}
							GmmsStatus gmmsStatus = hi.mapHttpStatus2GmmsStatus(httpStatus, msg.getMessageType());
							if(gmmsStatus==null){
								msg.setStatus(GmmsStatus.UNKNOWN);
							}else{
								msg.setStatus(gmmsStatus);
							}
						}									
					}else if("outMsgID".equalsIgnoreCase(pval)){
						Object o = report.getProperty(oproperty);
						if(o == null){							
							msg.setStatus(GmmsStatus.INVALID_MSG_FIELD);
							log.error("Received DR request with outMsgID is null from Emay.");
						}else{
							Long  seqID = (Long)o;
							mval = String.valueOf(seqID);
							msg.setProperty(pval, mval);
						}
					}else{
						mval = (String)report.getProperty(oproperty);
						msg.setProperty(pval, mval);
					}									
				}//end of for params
				
				log.debug("Received DR request with Status:"+drStatus+" and outMsgID:"+msg.getOutMsgID());	
				if(msg.getOutMsgID() == null){
					String outMsgid = MessageIdGenerator.generateCommonStringID();
					msg.setMsgID(outMsgid);
				}
				msg.setRSsID(cst.getSSID());
				list.add(msg);
			}
		}
		return list;
	}
	
	
	@Override
	public GmmsMessage newRequest(GmmsMessage message, A2PCustomerInfo cst) {
		try {
			if(mtRequSDKClient == null){
				mtRequSDKClient = serviceLocator.initSDKClient(cst); 
			}
			
		} catch (ServiceException e) {
			 log.error("Construct SDK Client instance failed when invoke newRequest(), the exception:"+e);
	         return null;
		}	
		List<HttpParam> params = hi.getMtSubmitRequest().getParamList();
		MT mt = new MT();
		String outMsgid = message.getOutMsgID();

		for (HttpParam hp : params) {
			String pval = hp.getParam();
			String mval = null;
			if("recipientAddress".equalsIgnoreCase(pval)){
				String[] mobiles = new String[1];
				mval = HttpUtils.getParameter(hp, message, cst);
				mobiles[0] = mval;
				String oproperty = hp.getOppsiteParam();
				mt.setProperty(mt,oproperty,mobiles);
				continue;
			}else if("senderAddress".equalsIgnoreCase(pval)){
				mval = HttpUtils.getParameter(hp, message, cst);
				boolean isMo = false;
				String[] numberList = cst.getSpecialServiceNumList();
				if(numberList !=null && numberList.length>0){
					for(String number:numberList){
						if(mval.startsWith(number)){
							mval = mval.substring(number.length());
							isMo = true;
							break;
						}						
					}//end of for
					if(!isMo){
						mval = "";
					}
				}else{
					mval = "";
				}
				String oproperty = hp.getOppsiteParam();
				mt.setProperty(mt,oproperty,mval);
				continue;
			}else if("outMsgID".equalsIgnoreCase(pval)){
				if(outMsgid==null){
					outMsgid = MessageIdGenerator.generateCommonStringID();
					message.setOutMsgID(outMsgid);
				}
				long smsID = Long.valueOf(outMsgid);
				String oproperty = hp.getOppsiteParam();
				mt.setProperty(mt,oproperty,smsID);
				continue;
			}else{
				mval = HttpUtils.getParameter(hp, message, cst);		
				String oproperty = hp.getOppsiteParam();
				mt.setProperty(mt,oproperty,mval);
			}
		}
	
		try {
			int p = mtRequSDKClient.sendSMS(mt.getSoftwareSerialNo(), mt.getKey(), mt.getSendTime(), mt.getMobiles(), mt.getSmsContent(), mt.getAddSerial(), mt.getSrcCharset(),mt.getSmsPriority(), mt.getSmsID());
			if(log.isInfoEnabled()){
				log.info(message,"Received resp with reqStatus:"+p);
			}
			HttpStatus httpStatus = new HttpStatus(String.valueOf(p),null);
			GmmsStatus gmmsStatus = hi.mapHttpStatus2GmmsStatus(httpStatus, message.getMessageType());
			if(gmmsStatus==null){
				message.setStatus(GmmsStatus.UNKNOWN_ERROR);
			}else{
				message.setStatus(gmmsStatus);
			}
			if(outMsgid != null){
				message.setOutMsgID(outMsgid);
			}
			message.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
			return message;
		} catch (RemoteException e) {
			log.error("invoke sendSMS() of SDK failed, the exception is:"+e);
			message.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
			message.setStatus(GmmsStatus.COMMUNICATION_ERROR);
			return message;
		}
	}

	public List<GmmsMessage> parseMORequestList(GmmsMessage message,A2PCustomerInfo cst){
		A2PSingleConnectionInfo singleInfo = (A2PSingleConnectionInfo)cst;
		String softwareSerialNo = singleInfo.getChlAcctName();
		String key = singleInfo.getChlPassword();
		List<GmmsMessage> list = null;
		try {

			if(moRequSDKClient == null){
				moRequSDKClient = serviceLocator.initSDKClient(cst); 
			}
			
		} catch (ServiceException e) {
			 log.error("Construct SDK Client instance failed when invoke moListRequest(), the exception:"+e);
	         return null;
		}
		
		//invoke SDK API to get MO messages:
        Mo[] moList = null;
        try {
        	moList = moRequSDKClient.getMO(softwareSerialNo,key);
		} catch (RemoteException e) {
			log.error("invoke getMO() of SDK failed, the exception is:"+e);
			return null;
		}
		
		String mtype = "";
		if(singleInfo.getChlCharset() == null){
			log.warn("operator:"+cst.getSSID()+" does not configure charset item.");
			mtype = "utf-8";
		}else{			
			mtype =(String)singleInfo.getChlCharset();
		}
		
		List<HttpParam> params = hi.getMoSubmitRequest().getParamList();
		
		if(moList!=null){
			list = new ArrayList<GmmsMessage>();
			for(Mo m:moList){
				GmmsMessage msg = new GmmsMessage();	
				msg.setOSsID(cst.getSSID());
				msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT);
				String  msgID = MessageIdGenerator.generateCommonMsgID(cst.getSSID());
				msg.setMsgID(msgID);
				msg.setInMsgID(msgID);
				for (HttpParam hp : params) {
					String pval = hp.getParam();					
					Object mval = null;
					String oproperty = hp.getOppsiteParam();
					mval = m.getProperty(oproperty);
					if(mval == null){						
						continue;
					}
					if ("senderAddress".equalsIgnoreCase(pval)) {
						if(mval.toString().length() <= 0){
							msg.setStatus(GmmsStatus.SENDER_ADDR_ERROR);
						}					
						msg.setProperty(pval, mval.toString());
					}else if("recipientAddress".equalsIgnoreCase(pval)){
						if(mval.toString().length() <= 0){
							msg.setStatus(GmmsStatus.RECIPIENT_ADDR_ERROR);
						}
						msg.setProperty(pval, mval.toString());
					}else if("textContent".equalsIgnoreCase(pval)){
						if(mval.toString().length() <= 0){
							msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
						}else{
							try {
								msg.setTextContent(mval.toString());
								msg.setMessageSize(msg.getTextContent().getBytes(mtype).length);
//								msg.setContentType(GmmsMessage.AIC_CS_UCS2);
							} catch (Exception e) {
								log.error(msg,"parseHttpContent() failed, mtype:"+mtype+" and exception is:"+e);
							}
						}
					}else{
						msg.setProperty(pval, mval.toString());
					} 					
				}
				msg.setContentType(GmmsMessage.AIC_CS_UCS2);
				list.add(msg);
			}
		}
		return list;
	}
}
