package com.king.gmms.processor;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import com.king.framework.SystemLogger;
import com.king.gmms.Constant;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.session.Session;
import com.king.gmms.connectionpool.systemmanagement.ConnectionManagementForCore;
import com.king.gmms.customerconnectionfactory.InternalCoreEngineConnectionFactory;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.domain.A2PMultiConnectionInfo;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.messagequeue.OperatorMessageQueue;
import com.king.gmms.threadpool.RunnableMsgTask;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageStoreManager;
import com.king.rest.util.StringUtility;

public class AutoSendInDRProcessorThread extends RunnableMsgTask{
	private static SystemLogger log = SystemLogger
			.getSystemLogger(AutoSendInDRProcessorThread.class);

	private A2PCustomerManager ctm;
	private GmmsUtility gmmsUtility;
	private InternalCoreEngineConnectionFactory factory = null;
	private MessageProcessor processor = null;
	private DBBackupHandler dbHandler = null;
	private MessageStoreManager msm = null;
	private boolean isEnableMgt = false;
	private List<String> persistentProtocols = null;
	private ModuleManager moduleManager = null;
	private ConnectionManagementForCore connectionManager = null;
	private String moduleName = null;

	public AutoSendInDRProcessorThread(GmmsMessage msg) {
		this.message = msg;
		gmmsUtility = GmmsUtility.getInstance();
		ctm = gmmsUtility.getCustomerManager();
		factory = InternalCoreEngineConnectionFactory.getInstance();
		processor = new MessageProcessor();
		dbHandler = DBBackupHandler.getInstance();
		msm = gmmsUtility.getMessageStoreManager();
		isEnableMgt = gmmsUtility.isSystemManageEnable();
		if (isEnableMgt) {
			connectionManager = ConnectionManagementForCore.getInstance();
			persistentProtocols = connectionManager.getPersistentConnProtocol();
		}
		moduleManager = ModuleManager.getInstance();
		moduleName =  System.getProperty("module");
	}

	@Override
	public void run() {
		if (message == null) {
			return;
		}
		A2PCustomerInfo oInfo = ctm.getCustomerBySSID(message.getOSsID());
		String delayTimeInterval = oInfo.getDrDelayTimeInSec();
		int delayTime = GmmsUtility.getRandomValueByDelayTimeInterval(delayTimeInterval);
		GmmsMessage drMsg = new GmmsMessage(message);
		drMsg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		if (GmmsUtility.isModifySuccessDR(oInfo.getSSID(), oInfo.getDrSucRatio(), oInfo.getDrBiasRatio())) {
			drMsg.setStatus(GmmsStatus.DELIVERED);
		}else {
			drMsg.setStatus(GmmsStatus.UNDELIVERABLE);
		}
		msm.insertMsgForDRDelay(drMsg, delayTime);
	}
	
	private boolean isBlackholed(GmmsMessage message) {
		try {
			int ossid = message.getOSsID();
			A2PCustomerInfo oCustomerInfo = ctm.getCustomerBySSID(ossid);
			Map<String, Integer> blackholePercentMap = oCustomerInfo.getBlackholePercentMap();
			
			if (ctm.inCurrentA2P(message.getRA2P())             // in current A2P
					&& gmmsUtility.getBlackholeSsid()>0         // has blackhole ssid
					&& blackholePercentMap.keySet().size()>0    // ossid got blackhole conf
					&& message.getOriginalQueue() == null) { // don't blackhole msg from RMQ																
				
				// get blackhole percent via ossid_rop
				String key = ossid + "_" + message.getRoperator();
				Integer blackholePercent = blackholePercentMap.get(key);
				
				// get blackhole percent via ossid
				if (blackholePercent == null) {
					blackholePercent = blackholePercentMap.get(ossid+"");
				}
				
				// random process
				if (blackholePercent != null) {
					if (log.isInfoEnabled()) {
						log.info(message, "get conf blackholePercent:{}", blackholePercent);
					}
					
					if (blackholePercent == 0) {
						return false;
					}
					
					if (blackholePercent == 100) {
						return true;
					}
					
					Random r = new Random();
					double tmp = r.nextDouble();
					if (tmp < blackholePercent/100.0) {
						if(log.isInfoEnabled()){
							log.info(message, "message blackholed.");
						}
						return true;
					}
				}
			}
		} catch (Exception e) {
			log.warn(message, "error occured when decide isBlackholed", e);
		}
		return false;
	}
	
	private boolean isBlackholedNew(GmmsMessage message) {
		try {
			int ossid = message.getOSsID();
			A2PCustomerInfo ocustomer = ctm.getCustomerBySSID(ossid);
			Map<String, Integer> blackholePercentMap = ocustomer.getBlackholePercentMap();
			
			if (ctm.inCurrentA2P(message.getRA2P())             // in current A2P
					&& gmmsUtility.getBlackholeSsid()>0         // has blackhole ssid
					&& blackholePercentMap.keySet().size()>0    // ossid got blackhole conf
					&& message.getOriginalQueue() == null) { // don't blackhole msg from RMQ
				
				//for portal customer, check the batch number > blackholeMinnumber or not.
				String inmsgid = message.getInMsgID();
				if (inmsgid.startsWith("p") && inmsgid.contains("_")) {
					String[] numbers = inmsgid.split("_");
					if (numbers.length==3) {
						int blockMinNumber = ocustomer.getBlackholebatchMinNumberForPortalCust();
						if (blockMinNumber>Integer.parseInt(numbers[1])) {
							return false;
						}
					}
				}else {
					//blackhole by timer
					int blackholeTimer = ocustomer.getBlackholeTimerInSec();
					int blackholeNumber = ocustomer.getBlackholeMinNumberByTimer();
					Long number = gmmsUtility.getRedisClient().incBlackhole("blackhole_"+ossid);
					//log.info("redis blackhole number is {},{}", number, blackholeNumber);
					gmmsUtility.getRedisClient().setExpire("blackhole_"+ossid, blackholeTimer);
					if (number != null && number == 1) {						
						return false;
					}else if (blackholeNumber>number) {
						return false;
					}
				}
				// get blackhole percent via ossid_rop
				String key = ossid + "_" + message.getRoperator();
				Integer blackholePercent = blackholePercentMap.get(key);
				
				// get blackhole percent via ossid
				if (blackholePercent == null) {
					blackholePercent = blackholePercentMap.get(ossid+"");
				}
				
				// random process
				if (blackholePercent != null) {
					if (log.isInfoEnabled()) {
						log.info(message, "get conf blackholePercent:{}", blackholePercent);
					}
					
					if (blackholePercent == 0) {
						return false;
					}
					
					if (blackholePercent == 100) {
						return true;
					}
					
					Random r = new Random();
					double tmp = r.nextDouble();
					if (tmp < blackholePercent/100.0) {
						if(log.isInfoEnabled()){
							log.info(message, "message blackholed.");
						}
						return true;
					}
				}
			}
		} catch (Exception e) {
			log.warn(message, "error occured when decide isBlackholed", e);
		}
		return false;
	}
	
	private void transactBlackholedMessage(GmmsMessage message) {
		try {
			message.setActionCode(-1);
			message.setOutMsgID(message.getMsgID());
			message.setOutTransID(String.valueOf(System.currentTimeMillis()));
			
			// 1)	set message status to GmmsStatus.SUCCESS
			message.setStatus(GmmsStatus.SUCCESS);
			// 2)	set message rssid to blackhole ssid
			message.setRSsID(gmmsUtility.getBlackholeSsid());
			// 3)	cdr OUT_SUBMIT
			gmmsUtility.getCdrManager().logOutSubmitRes(message, true);
			
			// 4)	set message type to GmmsMessage.MSG_TYPE_DELIVERY_REPORT
			message.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
			message.setDateIn(new Date());
			message.setDeliveryChannel(this.moduleName);
			message.setStatus(GmmsStatus.DELIVERED);
			// 5)	cdr OUT_DR			
			gmmsUtility.getCdrManager().logOutDeliveryReportRes(message);
			
			// 6)	set DR status by dr success ratio
			A2PCustomerInfo cst= ctm.getCustomerBySSID(message.getOSsID());
			if (cst.getBlackholeDRSucRatio() == 0) {
				message.setStatusCode(cst.getBlackholeDrStatusCode());
				message.setStatusText(GmmsStatus.getStatus(message.getStatusCode()).getText());
			}else {
				if (GmmsUtility.isModifySuccessDR(cst.getSSID(), cst.getBlackholeDRSucRatio(), cst.getDrBiasRatio())) {
					message.setStatus(GmmsStatus.DELIVERED);
				}else {
					message.setStatus(GmmsStatus.UNDELIVERABLE);
				}
			}
			
			
			// 7)	If ossid need DR,  send IN_DR
//			if (message.getDeliveryReport()) {
//				msm.sendDRMessage(message);
//			}
			if (message.getDeliveryReport()) {
				if(message.getInClientPull() == 2){
					msm.updateMsgForDRQuery(message);
				}else{
					//msm.sendDRMessage(message);
					int sendDrImmediately = cst.getSendDelayDRImmediately();
					if(sendDrImmediately == 1){
						msm.sendDRMessage(message);
					}else{
						String delayTimeInterval = cst.getDrDelayTimeInSec();
						int delayTime = GmmsUtility.getRandomValueByDelayTimeInterval(delayTimeInterval);
						msm.insertMsgForDRDelay(message, delayTime);	
					}
				}
			}
		} catch(Exception e) {
			log.error(message, "transactBlackholedMessage error", e);
		}
	}
	
	/**
	 * isSupport system management
	 * 
	 * @param server
	 * @return
	 */
	private boolean isSupportMgt(A2PCustomerInfo server) {
		if (server == null || server.getProtocol() == null) {
			return false;
		}
		String protocol = server.getProtocol().toLowerCase();
		if (isEnableMgt && persistentProtocols != null
				&& persistentProtocols.contains(protocol)) {
			return true;
		}
		return false;
	}

	private void transactNewMessage(GmmsMessage message) {

		A2PCustomerInfo server = ctm.getCustomerBySSID(message.getRSsID());

		if (processor.antiBinary(message, server)) {
			message.setStatus(GmmsStatus.BinaryFilter);
			msm.handleOutSubmitRes(message);
			return;
		}

		if (processor.antiSpam(message, server)) {
			message.setStatus(GmmsStatus.SPAMED);
			msm.handleOutSubmitRes(message);
			return;
		}
		List<String> rejectRegions = server.getSmsOptionRejectRegions();
		String msisdn = message.getRecipientAddress();
		if (msisdn.startsWith("86") 
				&& rejectRegions!=null 
				&& !rejectRegions.isEmpty()
				&& gmmsUtility.checkReciptAddressRegions(rejectRegions, msisdn.substring(2))) {
			message.setStatus(GmmsStatus.SPAMED);
			msm.handleOutSubmitRes(message);
			return;
		}
		
		//do replace content
		A2PCustomerInfo ocustomer = ctm.getCustomerBySSID(message.getOSsID());
		Map<String, Map<String, String>> replaceContentMap = ocustomer.getSmsOptionReplaceContent();
		if (replaceContentMap !=null && replaceContentMap.size()>0) {
			try {
				for (Map.Entry<String, Map<String, String>> entity: replaceContentMap.entrySet()) {
					if (entity.getKey().equalsIgnoreCase("all")) {
						String contentString = message.getTextContent();
						Map<String, String> contentReplacementMap = entity.getValue();
						if (contentReplacementMap != null && !contentReplacementMap.isEmpty()) {
							for (Map.Entry<String, String> entry : contentReplacementMap.entrySet()) {
								String key = entry.getKey();
								String value = entry.getValue();
								if (value != null) {
									contentString = contentString.replaceAll(key, value.trim());
									if (!contentString.equals(message.getTextContent())) {
										message.setTextContent(contentString);
										message.setMessageSize(contentString.getBytes(message.getContentType()).length);																		
									}								
								}
							}
						}
					}else {
						String recipitAddr = message.getRecipientAddress();
						if (recipitAddr.startsWith("+")) {
							recipitAddr = recipitAddr.substring(1);
						}
						if (recipitAddr.startsWith(entity.getKey())) {
							String contentString = message.getTextContent();
							Map<String, String> contentReplacementMap = entity.getValue();
							if (contentReplacementMap != null && !contentReplacementMap.isEmpty()) {
								for (Map.Entry<String, String> entry : contentReplacementMap.entrySet()) {
									String key = entry.getKey();
									String value = entry.getValue();
									if (value != null) {
										contentString = contentString.replaceAll(key, value.trim());
										if (!contentString.equals(message.getTextContent())) {
											message.setTextContent(contentString);
											message.setMessageSize(contentString.getBytes(message.getContentType()).length);	
										}																		
									}
								}
							}
						}
					}										
				}
			} catch (Exception e) {
				log.error("do replace message content error", e);
			}
			
		}
		//copy textContent to inTextContent for backup routing
		message.setInTextContent(message.getTextContent());
		message.setInContentType(message.getContentType());
		
		//do replace Content by TemplateId in routing config	
		try {
			if(ctm.isContentReplace(message.getOSsID(), message.getRSsID(), message.getRecipientAddress(), message.getSenderAddress())){
				String textContent = message.getTextContent();
				String regex = ocustomer.getTemplatecoderegex();
				String rex = "(?:[0-9-]){4,}";
				if(regex!=null && !"".equalsIgnoreCase(regex)) {
					rex = regex;
				}
				Pattern p = Pattern.compile(rex);
				  Matcher m = p.matcher(textContent);
				 if(m.find()) {
					 String textContentType = null;
					 if (GmmsMessage.AIC_CS_ASCII.equalsIgnoreCase(message.getContentType())
							 || message.getContentType().startsWith("ISO-8859")) {
						 textContentType = GmmsMessage.AIC_CS_ASCII;
					  }else{
						  textContentType = GmmsMessage.AIC_CS_UCS2;
					  }				 
					 List<String> replaceContent = ctm.getTemplateContent(m.group(), textContentType, message.getOSsID(),
							 message.getRSsID(), message.getRecipientAddress(), message.getSenderAddress());
					if (replaceContent!=null) {
						String type = replaceContent.get(0);
						String text = replaceContent.get(1);
						 message.setTextContent(text);
						 message.setContentType(type);
						 //模板替换后，要把7bit的标志去掉
						 if(GmmsMessage.AIC_CS_UCS2.equalsIgnoreCase(type)){
							 message.setGsm7bit(false);
						 }
						 message.setMessageSize(text.getBytes(type).length);
						 if (replaceContent.size()==3) {
							message.setSenderAddress(replaceContent.get(2));							
						 }
					}else {
						log.error(message, "the template Content is null in contentTemplate.cfg");						
						message.setStatus(GmmsStatus.TEMPLATE_FAIL);
						if (ocustomer.isDrSuccForTemplateReplaceFailed()) {
							transactTemplateFailedMessageAndReturnSuccessDR(message);
						}else {							
							dbHandler.putMsg(message);
						}						
						
						return;
					}
					 
				 }else {
					 log.info(message, "do the replace content template failed, the code can't found in content [{}]", message.getTextContent());
					 message.setStatus(GmmsStatus.TEMPLATE_FAIL);
					 if (ocustomer.isDrSuccForTemplateReplaceFailed()) {
							transactTemplateFailedMessageAndReturnSuccessDR(message);
						}else {							
							dbHandler.putMsg(message);
						}						
						return;
				 }
					
			}
		} catch (Exception e) {
			log.error(message,"do replace content template error", e);
		}
		
		LinkedList<GmmsMessage> messageList = new LinkedList<GmmsMessage>();

		try {
			// judge RA2P is current A2P
			if (ctm.inCurrentA2P(message.getRA2P())) {
				// content is text type
				if (GmmsMessage.AIC_MSG_TYPE_TEXT.equalsIgnoreCase(message
						.getGmmsMsgType())) {														
					//check the DC support content template or not
					if (message.getTextContent().startsWith(Constant.CONTENT_TEMPLATE_KEYWORD)) {
						A2PCustomerInfo rInfo = ctm.getCustomerBySSID(message.getRSsID());
						if (rInfo.isSupportTemplateParemeterDelivery()) {
							message.setContentTemplateParamter(true);
							if (rInfo.isSupportTemplateParemeterSignature()) {
								if (message.getTextContent().contains(Constant.CONTENT_SIGNATURE_KEYWORD)) {
									message.setContentSignature(true);
								}else {
									message.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
									dbHandler.putMsg(message);
									return;
								}							
							}
						}else {
							String textContent = message.getTextContent();
							int signatureIndex = textContent.indexOf(Constant.CONTENT_SIGNATURE_KEYWORD);
							int templateIndex = textContent.indexOf(Constant.CONTENT_TEMPLATE_KEYWORD);
							String template = "";
							String signature = "";
							if (templateIndex>-1) {
								if (signatureIndex>0) {
									template = textContent.substring(templateIndex+Constant.CONTENT_TEMPLATE_KEYWORD.length(), signatureIndex);
									signature = textContent.substring(signatureIndex+Constant.CONTENT_SIGNATURE_KEYWORD.length(), textContent.length());						
								}else {
									template = textContent.substring(templateIndex+Constant.CONTENT_TEMPLATE_KEYWORD.length(), textContent.length());
								}
						    	
							}else {
								message.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
								dbHandler.putMsg(message);
								return;
							}
							
							String templateId = template.trim();
							String paramters = "";
							int templateIdIndex = template.indexOf("=");
					    	if (templateIdIndex>-1) {
								templateId = template.substring(0, templateIdIndex).trim();
								paramters = template.substring(templateIdIndex+1).trim();
							}
					    	String templateContent = ctm.getContentTpl().getTemplateContentMaps(templateId);
					    	if (StringUtility.stringIsNotEmpty(paramters)) {
								String[] paraArry = paramters.split(",");
								for (String para : paraArry) {
									templateContent = templateContent.replaceFirst("\\{\\}", para);									
								}
							}
					    	
					    	message.setTextContent(templateContent);
					    	message.setMessageSize(templateContent.getBytes(message.getContentType()).length);
					    	message.setContentIsChanged(true);
					    	message.setContentType(ctm.getContentTpl().getTemplateContentTypeMaps(templateId));
					    	if (StringUtility.stringIsNotEmpty(signature)) {
								processor.addTemplateSignature(message, signature);
							}				    	
						}												
					}else {
						// add content signature
						if (!message.hasContentSignature()) {
							processor.addContentSignature(message);
						}
					}
					
					// set char set
					processor.convertCharset(message);
					// judge message has added ott sender address
					if (!message.hasOttContentAddOaddr()) {
						processor.addOttContentOaddr(message, server);
					}
					// judge message has added sender address
					if (!message.hasAddForeword()) {
						processor.addForeword(message, server);
					}
					
					if (message.getSplitStatus() <= 0) {
						messageList = processor.processBinaryOrTextMessage(
								message, server);
						if(messageList == null || messageList.size() <= 0){
							dbHandler.putMsg(message);
						}
					} else {
						messageList.offer(message);
					}
				} else if (GmmsMessage.AIC_MSG_TYPE_BINARY
						.equalsIgnoreCase(message.getGmmsMsgType())) {
					if (message.getSplitStatus() <= 0) {
						messageList = processor.processBinaryOrTextMessage(
								message, server);
					} else {
						messageList.offer(message);
					}
				} else {
					messageList.offer(message);
				}
			} else {
				ModuleManager moduleManager = ModuleManager.getInstance();
				String channel = moduleManager.selectPeeringChannel();
				OperatorMessageQueue messagequeue = factory.getMessageQueue(
						message, channel);
				if (messagequeue != null) {
					message.setTransaction(null);
					message.setDeliveryChannel(message.getDeliveryChannel()+":"+channel);
					if (!messagequeue.putMsg(message)) {
						message.setStatus(GmmsStatus.COMMUNICATION_ERROR);
						dbHandler.putMsg(message);
					} else {
						if(log.isInfoEnabled()){
	    					log.info(message, "Send {} to {}",message.getMessageType(), channel);
						}
					}
				} else {
					message.setStatus(GmmsStatus.COMMUNICATION_ERROR);
					dbHandler.putMsg(message);
				}
				return;
			}
		} catch (Exception e) {
			log.error(message, e.toString());
			message.setStatus(GmmsStatus.SERVER_ERROR);
			dbHandler.putMsg(message);
			return;
		}
		
		//fake DR before outsubmit. 2021.01.22
		A2PCustomerInfo oInfo = ctm.getCustomerBySSID(message.getOSsID());
		A2PCustomerInfo rInfo = ctm.getCustomerBySSID(message.getRSsID());
		boolean isFakeDR = false;
		if (oInfo.isSmsOptionSendFakeDR() && rInfo.isSmsOptionSendDRBeforeOutSubmitInDC()
				&& rInfo.isNeedFakeDRForRecPrefix(message.getRecipientAddress())) {
			String delayTimeInterval = oInfo.getDrDelayTimeInSec();
			int delayTime = GmmsUtility.getRandomValueByDelayTimeInterval(delayTimeInterval);
			GmmsMessage drMsg = new GmmsMessage(message);
			drMsg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
			if (GmmsUtility.isModifySuccessDR(oInfo.getSSID(), oInfo.getDrSucRatio(), oInfo.getDrBiasRatio())) {
				drMsg.setStatus(GmmsStatus.DELIVERED);
			}else {
				drMsg.setStatus(GmmsStatus.UNDELIVERABLE);
			}
			msm.insertMsgForDRDelay(drMsg, delayTime);
			message.setFakeDR(false);
			isFakeDR = true;
		}

		// Select Queue
		String queue = server.getChlQueue();
		String moduleName = null;
		TransactionURI transaction = null;
		if (isSupportMgt(server)) {
			Session session = connectionManager.getSession(message, message
					.getRSsID(), queue);
			if (session != null) {
				transaction = session.getTransactionURI();
				//log.debug(message, "message processer handler get the transactionURL is {}", transaction);
				moduleName = transaction.getModule().getModule();
			} else if(!isInit(server)){//chlInit=no
				moduleName = moduleManager.selectChannel(queue);
				log.info(message, "Can not find customer session to send message, isInit=false, try random function module");
			} else {
				moduleName = moduleManager.selectChannel(queue);
				if(log.isInfoEnabled()){
					log.info(message,
						"Can not find customer session to send message, and try message to random function module");
				}
			}
		} else {
			moduleName = moduleManager.selectChannel(queue);
		}
		OperatorMessageQueue messagequeue = factory.getMessageQueue(message,
				moduleName);
		
		for (GmmsMessage msg : messageList) {			
			if (isFakeDR) {
				msg.setFakeDR(false);
			}
			if (moduleName == null) {
				msg.setStatus(GmmsStatus.SERVER_ERROR);
				if(log.isInfoEnabled()){
					log.info(msg, "Can not get the module name or session");
				}
				dbHandler.putMsg(msg);
				continue;
			}
			if (messagequeue != null) {
				msg.setTransaction(transaction);
				msg.setDeliveryChannel(msg.getDeliveryChannel()+":"+moduleName);
				if (!messagequeue.putMsg(msg)) {
					if(log.isInfoEnabled()){
						log.info(msg, "Can not put the message to sender queue");
					}
					msg.setStatus(GmmsStatus.SERVER_ERROR);
					dbHandler.putMsg(msg);
				} else {
					if(log.isInfoEnabled()){
						log.info(message, "Send {} to {}",message.getMessageType(), moduleName);
					}
				}
			} else {
				msg.setStatus(GmmsStatus.SERVER_ERROR);
				if(log.isInfoEnabled()){
					log.info(msg, "Can not find sender queue");
				}
				dbHandler.putMsg(msg);
			}

		}
		
		
	}
	
	private void transactTemplateFailedMessageAndReturnSuccessDR(GmmsMessage message) {
		try {
			message.setActionCode(-1);
			message.setOutMsgID(message.getMsgID());
			message.setOutTransID(String.valueOf(System.currentTimeMillis()));						
			// 3)	cdr OUT_SUBMIT
			gmmsUtility.getCdrManager().logOutSubmitRes(message, true);
			
			// 4)	set message type to GmmsMessage.MSG_TYPE_DELIVERY_REPORT
			message.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
			message.setDateIn(new Date());
			message.setDeliveryChannel(this.moduleName);
			message.setStatus(GmmsStatus.DELIVERED);
			if (message.getDeliveryReport()) {
				if(message.getInClientPull() == 2){
					msm.updateMsgForDRQuery(message);
				}else{
					msm.sendDRMessage(message);
				}
			}
		} catch(Exception e) {
			log.error(message, "transactTemplateFailedMessageAndReturnSuccessDR error", e);
		}
	}
	
	/**
	 * is init
	 * @param ci
	 * @return
	 */
	private boolean isInit(A2PCustomerInfo ci){
		if (ci.getConnectionType() == 1) {
			return ((A2PSingleConnectionInfo) ci).isChlInit();
		} else {
			return ((A2PMultiConnectionInfo) ci).isInit();
		}
	}
	private void transactDR(GmmsMessage message) {
		int cA2P = message.getCurrentA2P();
		int oA2P = message.getOA2P();
		if ((cA2P == oA2P) || ctm.vpOnSameA2P(cA2P, oA2P)) { // on the same A2P
			// 1.5 way/ott to recovery sender/recipient
			String origSender = message.getOriginalSenderAddr();
			if (origSender != null && !"".equalsIgnoreCase(origSender)) {
				message.setSenderAddress(origSender);
			}
			String origRecipient = message.getOriginalRecipientAddr();
			if (origRecipient != null && !"".equalsIgnoreCase(origRecipient)) {
				message.setRecipientAddress(origRecipient);
			}

			A2PCustomerInfo server = ctm.getCustomerBySSID(message.getOSsID());
			String queue = null;
			if (server == null) {
				message.setStatus(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT);
				dbHandler.putMsg(message);
				return;
			}
//			if (message.inClientPull()) {
//				queue = server.getReceiverQueue();
//			} else {
//				queue = server.getChlQueue();
//			}
			
			if(message.getInClientPull()==1)
			{
				queue = server.getReceiverQueue();
			}else
			{
				queue = server.getChlQueue();
			}
			

			String moduleName = null;
			TransactionURI transaction = null;
			if (isSupportMgt(server)) {
				Session session = connectionManager.getSession(message, message
						.getOSsID(), queue);
				if (session != null) {
					transaction = session.getTransactionURI();
					moduleName = transaction.getModule().getModule();
				} else if(!isInit(server)){//chlInit=no
					moduleName = moduleManager.selectChannel(queue);
					log.info(message, "Can not find customer session to send DR, isInit=false, try random function module");
				} else {
					if(log.isInfoEnabled()){
						log.info(message,
							"Can not find customer session to send message");
					}
				}
			} else {
				moduleName = moduleManager.selectChannel(queue);
			}

			if (moduleName == null) {
				if(log.isInfoEnabled()){
					log.info(message, "Can not find moduleName to send message.");
				}
				message.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT
						.getCode());
				dbHandler.putMsg(message);
				return;
			}

			OperatorMessageQueue messagequeue = factory.getMessageQueue(
					message, moduleName);

			if (messagequeue != null) {
				message.setTransaction(transaction);
				message.setDeliveryChannel(message.getDeliveryChannel()+":"+moduleName);
				if (!messagequeue.putMsg(message)) {
					if(log.isInfoEnabled()){
						log.info(message,
									"Can not put the message to sender queue");
					}
					message.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.getCode());
					dbHandler.putMsg(message);
				} else {
					if(log.isInfoEnabled()){
						log.info(message, "Send {} to {}",message.getMessageType(), moduleName);
					}
				}
			} else {
				if(log.isInfoEnabled()){
					log.info(message, "Can not find sender queue");
				}
				message.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT
						.getCode());
				dbHandler.putMsg(message);
			}
		} else if (ctm.isA2P(oA2P) || ctm.isPartition(oA2P)) {
			ModuleManager moduleManager = ModuleManager.getInstance();
			String channel = moduleManager.selectPeeringChannel();
			message.setDeliveryChannel(message.getDeliveryChannel()+":"+channel);
			InternalCoreEngineConnectionFactory factory = InternalCoreEngineConnectionFactory
					.getInstance();
			OperatorMessageQueue msgQueue = factory.getMessageQueue(message,
					channel);
			if (msgQueue == null) {
				log
						.warn(message,
								"This DR can't send out because can't get available session.");
				message.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT
						.getCode());
				dbHandler.putMsg(message);
			} else {
				message.setTransaction(null);
				if (!msgQueue.putMsg(message)) {
					message
							.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT
									.getCode());
					dbHandler.putMsg(message);
				} else {
					if(log.isInfoEnabled()){
						log.info(message, "Send {} to {}",message.getMessageType(), channel);
					}
				}
			}
		}
	}

	private void transactDRQuery(GmmsMessage message) {

		A2PCustomerInfo server = ctm.getCustomerBySSID(message.getRSsID());
		if (server == null) {
			message.setStatus(GmmsStatus.FAIL_QUERY_DELIVERREPORT);
			dbHandler.putMsg(message);
			return;
		}

		String queue = server.getChlQueue();

		String moduleName = null;
		TransactionURI transaction = null;
		if (isSupportMgt(server)) {
			Session session = connectionManager.getSession(message, message
					.getRSsID(), queue);
			if (session != null) {
				transaction = session.getTransactionURI();
				moduleName = transaction.getModule().getModule();
			} else if(!isInit(server)){//chlInit=no
				moduleName = moduleManager.selectChannel(queue);
			} else {
				if(log.isInfoEnabled()){
					log.info(message,
						"Can not find customer session to send message");
				}
			}
		} else {
			moduleName = moduleManager.selectChannel(queue);
		}

		if (moduleName == null) {
			message
					.setStatusCode(GmmsStatus.FAIL_QUERY_DELIVERREPORT
							.getCode());
			dbHandler.putMsg(message);
			return;
		}

		OperatorMessageQueue messagequeue = factory.getMessageQueue(message,
				moduleName);

		if (messagequeue != null) {
			message.setTransaction(transaction);
			if (!messagequeue.putMsg(message)) {
				if(log.isInfoEnabled()){
					log.info(message, "Can not put the message to sender queue");
				}
				message.setStatusCode(GmmsStatus.FAIL_QUERY_DELIVERREPORT
						.getCode());
				dbHandler.putMsg(message);
			} else {
				if(log.isInfoEnabled()){
					log.info(message, "Send {} to {}",message.getMessageType(), moduleName);
				}
			}
		} else {
			if(log.isInfoEnabled()){
				log.info(message, "Can not find sender queue");
			}
			message
					.setStatusCode(GmmsStatus.FAIL_QUERY_DELIVERREPORT
							.getCode());
			dbHandler.putMsg(message);
		}
	}
	
	public static void main(String[] args) {
		String ab= "WeSing 15633 adalah kode verifikasi WeSing kamu1234455.";
		String rex = "(?:[0-9-]){4,}";
		Pattern p = Pattern.compile(rex);
		  Matcher m = p.matcher(ab);
		 if(m.find()){
			 String code = m.group();
			 code = code.replaceAll("-", "");
			 System.out.println(code);
		 }
		
	}

}

