package com.king.gmms.processor;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.king.framework.SystemLogger;
import com.king.gmms.Constant;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.session.Session;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.domain.A2PMultiConnectionInfo;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.messagequeue.OperatorMessageQueue;
import com.king.gmms.metrics.MetricsCollector;
import com.king.gmms.metrics.MetricsNames;
import com.king.gmms.threadpool.RunnableMsgTask;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageStoreManager;
import com.king.rest.util.StringUtility;

public class MessageProcessorThread extends RunnableMsgTask{
	private static SystemLogger log = SystemLogger
			.getSystemLogger(MessageProcessorThread.class);

	private A2PCustomerManager ctm;
	private GmmsUtility gmmsUtility;
	private MessageProcessor processor = null;
	private DBBackupHandler dbHandler = null;
	private MessageStoreManager msm = null;
	private ModuleManager moduleManager = null;
	private String moduleName = null;

	public MessageProcessorThread(GmmsMessage msg) {
		this.message = msg;
		gmmsUtility = GmmsUtility.getInstance();
		ctm = gmmsUtility.getCustomerManager();
		processor = new MessageProcessor();
		dbHandler = DBBackupHandler.getInstance();
		msm = gmmsUtility.getMessageStoreManager();
		moduleManager = ModuleManager.getInstance();
		moduleName =  System.getProperty("module");
	}

	@Override
	public void run() {
		try {
			if (message == null) {
				return;
			}
			String messageType = message.getMessageType();
			message.setTransaction(null);//Clear old transactionURL
		if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(messageType)) {
			try {
				transactDR(message);
			} catch (Exception e) {
				log.error(message,
						"Occur error when routing this message,statuscode="
								+ message.getStatusCode(), e);
				message.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT
						.getCode());
				dbHandler.putMsg(message);
			}
		} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY
				.equalsIgnoreCase(messageType)) {
			try {
				transactDRQuery(message);
			} catch (Exception e) {
				log.error(message,
						"Occur error when routing this message,statuscode="
								+ message.getStatusCode(), e);
				message.setStatusCode(GmmsStatus.FAIL_QUERY_DELIVERREPORT
						.getCode());
				dbHandler.putMsg(message);
			}
		} else if (GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(messageType)
				|| GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(messageType)) {
			try {
				A2PCustomerInfo rCustomer = ctm.getCustomerBySSID(message.getRSsID());
				if (("blackhole".equalsIgnoreCase(rCustomer.getRole()) || isBlackholedNew(message))) {
					log.info(message, "put message to backhole");
					transactBlackholedMessage(message);
					return;
				} else {
					// the delivery window of the channel (rssid), not of the operator the routing found
					A2PCustomerInfo ropCust = ctm.getCustomerBySSID(message.getRSsID());
					if (ropCust != null) {
						// schedule delivery
						TimeZone ropTimeZone = ropCust.getSmsOptionTimeZone();
						int startHH = ropCust.getDeliveryStartHH();
						int startmm = ropCust.getDeliveryStartmm();
						int endHH = ropCust.getDeliveryEndHH();
						int endmm = ropCust.getDeliveryEndmm();
						// has scheduled conf 
						if (startHH > -1 && startmm > -1 
								&& endHH > -1 && endmm > -1) {
							Calendar custCalendar = Calendar.getInstance(ropTimeZone);
							custCalendar.set(Calendar.HOUR_OF_DAY, startHH);
			            	custCalendar.set(Calendar.MINUTE, startmm);
							Date startDate = custCalendar.getTime();
							
							custCalendar.set(Calendar.HOUR_OF_DAY, endHH);
			            	custCalendar.set(Calendar.MINUTE, endmm);
			            	Date endDate = custCalendar.getTime();
			            	
			            	if (log.isTraceEnabled()) {
			            		log.trace(message, "delivery time window is between ({}) and ({})", startDate.toGMTString(), endDate.toGMTString());
			            	}
			            	
			            	Date nowDate = new Date();
			            	if (nowDate.after(startDate) && nowDate.before(endDate)) {
			            		// now in delivery time window
			            		transactNewMessage(message);
			            		return;
			            	} else {
			            		// now not in window, update scheduleDeliveryTime to next day
			            		custCalendar.add(Calendar.DAY_OF_MONTH, 1);
			            		custCalendar.set(Calendar.HOUR_OF_DAY, startHH);
				            	custCalendar.set(Calendar.MINUTE, startmm);
				            	Date newScheduleDate = gmmsUtility.getGMTTime(custCalendar.getTime());
				            	message.setScheduleDeliveryTime(newScheduleDate);
				            	if (log.isInfoEnabled()) {
									log.info(message, "scheduleDeliveryTime update to next day ({})", custCalendar.getTime().toGMTString());
								}
				            	
				            	message.setActionCode(-1);
				            	if (message.getOriginalQueue() != null 
				            			&& message.getOriginalQueue().equalsIgnoreCase(MessageStoreManager.SMQ)) {
				            		gmmsUtility.getMessageStoreManager().updateByInMsgId(message, MessageStoreManager.SMQ);
								} else {
									// H-14(方案A): 兜底落库失败不再被静默吞掉，回退到 DBBackupHandler，避免调度消息丢失。
									if (!gmmsUtility.getMessageStoreManager().insertMessageToDB(message, MessageStoreManager.SMQ)) {
										log.warn(message, "insertMessageToDB(SMQ) failed for scheduled message, fallback to DBBackupHandler.");
										dbHandler.putMsg(message);
									}
								}
				            	
			            		return;
			            	}
						} else {
							// normal process
							transactNewMessage(message);
						}
					} else {
						transactNewMessage(message);
					}
				}
			} catch (Exception e) {
				log.error(message,
						"Occur error when routing this message,statuscode="
								+ message.getStatusCode(), e);
				message.setStatus(GmmsStatus.SERVER_ERROR);
				dbHandler.putMsg(message);
			}
		} else {
			log.warn(message, "Message type error:{}", message.getMessageType());
			message.setStatus(GmmsStatus.UNKNOWN_ERROR);
			dbHandler.putMsg(message);
		}
		} finally {
			com.king.db.DataManager.closeAllSessions();
		}
	}

	/**
	 * 同步处理入口 (C-1 修复 Option C / Step 1)。
	 *
	 * <p>复用 {@link #run()} 的全部逻辑(含各分支的 DB 兜底)，在调用方线程上同步执行。
	 * Downstream stream produce is synchronous; DB fallback is intentionally
	 * asynchronous and is considered acceptable once enqueued.
	 * <ul>
	 *   <li>{@code run()} 正常返回表示已到达下游 stream 或 DB fallback enqueue 终点，返回 {@code true}；</li>
	 *   <li>仅当下游 produce / fallback enqueue 抛异常时返回 {@code false}，由调用方据此不 ACK、留 PEL 重试。</li>
	 * </ul>
	 */
	public boolean processSync() {
		try {
			run();
			return true;
		} catch (Throwable t) {
			log.error(message, "MessageProcessorThread.processSync failed to reach durable terminus", t);
			return false;
		}
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
				String key = ossid + "_" + message.getRSsID();   // BlackholePercent R_SSID: the channel
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
					
					// 6.5 Optimization: use ThreadLocalRandom instead of new Random()
					double tmp = ThreadLocalRandom.current().nextDouble();
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
					Long number;
					if (com.king.gmms.messagequeue.StreamAttempt.once("cnt", "blackhole_" + ossid)) {
						number = gmmsUtility.getRedisClient().incBlackhole("blackhole_"+ossid);
						//log.info("redis blackhole number is {},{}", number, blackholeNumber);
						gmmsUtility.getRedisClient().setExpire("blackhole_"+ossid, blackholeTimer);
					} else {
						String counted = gmmsUtility.getRedisClient().getString("blackhole_" + ossid);
						number = counted == null ? 0L : Long.valueOf(counted.trim());
					}
					if (number != null && number == 1) {						
						return false;
					}else if (blackholeNumber>number) {
						return false;
					}
				}
				// get blackhole percent via ossid_rop
				String key = ossid + "_" + message.getRSsID();   // BlackholePercent R_SSID: the channel
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
					
					// 6.5 Optimization: use ThreadLocalRandom instead of new Random()
					double tmp = ThreadLocalRandom.current().nextDouble();
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
			if(message.getRSsID()<=0) {
				message.setRSsID(gmmsUtility.getBlackholeSsid());
			}			
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
			// 7) the customer had its fake DR (way 1) already
			if (FakeDr.given(message)) {
				log.info(message, "[DR-SKIP] ossid={} status={}: the customer had its fake DR, this blackhole DR is only recorded",
						message.getOSsID(), message.getStatusCode());
				return;
			}
			
			A2PCustomerInfo rcst= ctm.getCustomerBySSID(message.getRSsID());
			if(rcst.getBlackholeDRSucRatio() == 0) {
				message.setStatusCode(rcst.getBlackholeDrStatusCode());
				message.setStatusText(GmmsStatus.getStatus(message.getStatusCode()).getText());
			}else if (cst.getBlackholeDRSucRatio() == 0) {
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
	private void transactNewMessage(GmmsMessage message) {

		A2PCustomerInfo server = ctm.getCustomerBySSID(message.getRSsID());
		// a part cut from a message already done (retried from the DB): the steps below were done before it was split
		boolean whole = message.getSplitStatus() <= 0;
		if (whole) {
			startFromCustomerText(message);
		}

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
		
		A2PCustomerInfo ocustomer = ctm.getCustomerBySSID(message.getOSsID());
		if (whole) {
			//do replace content
			replaceContent(message, ocustomer);
		}

		//do replace Content by TemplateId in routing config	
		try {
			if(whole && ctm.isContentReplace(message)){
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
					 List<String> replaceContent = ctm.getTemplateContent(m.group(), textContentType, message);
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
		
		
		//do replace Content by TemplateId in vendor template config
		try {
			if (whole) ctm.doVendorTemplateReplace(message.getRSsID(), message);
			if(GmmsStatus.TEMPLATE_FAIL.getCode() == message.getStatusCode()) {
				if (ocustomer.isDrSuccForTemplateReplaceFailed()) {
					transactTemplateFailedMessageAndReturnSuccessDR(message);
				}else {							
					dbHandler.putMsg(message);
				}						
				return;
			}
		} catch (Exception e) {
			log.error(message,"do vendor replace content template error", e);
		}
		// the channel's steps the old internal session did (docs/通道外发处理恢复与替换缺陷修复设计.md); not for an MO to a customer
		if (whole && !isMoToCustomer(message, server) && !ChannelOutboundPreparer.prepare(message, server, msm)) {
			return;
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
				// to another A2P (peering): it went over the internal connection to the peering module, which the
				// stream mode does not have (docs/流模式下系统管理和模块间连接代码清理分析.md 3.4)
				log.warn(message, "MT for another A2P: no peering path in stream mode, COMMUNICATION_ERROR");
				message.setStatus(GmmsStatus.COMMUNICATION_ERROR);
				dbHandler.putMsg(message);
				return;
			}
		} catch (Exception e) {
			log.error(message, e.toString());
			message.setStatus(GmmsStatus.SERVER_ERROR);
			dbHandler.putMsg(message);
			return;
		}
		
		// Select Queue
		String queue = server.getChlQueue();
		// V4.1 Stateless Routing: Session-based sticky routing is disabled.
		// Core no longer tracks specific nodes for individual TCP connections.
		
		for (GmmsMessage msg : messageList) {			
			
			// V4.1 Decentralized HTTP Outbound: Dispatch via Redis Stream
			if (server != null && server.isHttpProtocol()) {
				String moduleName = moduleManager.selectChannel(queue);
				msg.setDeliveryChannel(moduleName); // Preserve selected module label
				if (com.king.gmms.messagequeue.StreamQueueManager.getInstance().produceHttpSubmitMessage(msg)) {
					MetricsCollector.getInstance().incrementCounter(MetricsNames.business(
							MetricsNames.FLOW_CORE, msg, MetricsNames.ACTION_ROUTING_SUCCESS));
					MetricsCollector.getInstance().incrementCounter(MetricsNames.business(
							MetricsNames.FLOW_CORE, msg, MetricsNames.ACTION_ROUTED_TO_REDIS));
					MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(
							MetricsNames.STAGE_OUT_SUBMIT, msg, true, MetricsNames.ACTION_ROUTED_TO_REDIS));
					if (log.isInfoEnabled()) {
						log.info(msg, "[MT-TO-CLIENT] rssid={} protocol=HTTP redisId={} path={}", msg.getRSsID(), msg.getRedisStreamID(), com.king.gmms.util.ModulePath.withSelf(msg.getModulePath()));
					}
					continue;
				} else {
					log.error(msg, "Failed to dispatch HTTP MT task to Redis Stream");
				}
			}

			msg.setTransaction(null);
			if (isMoToCustomer(msg, server)) {
				// a vendor's MO (the Client writes it as a Submit) routed to a customer: to the customer's Server as a
				// deliver_sm, not to stream:mt:routed (the Client's, nobody sends from there to a customer)
				msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY);
			}
			if (GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(msg.getMessageType())) {
				String moduleName = moduleManager.selectChannel(queue);
				msg.setDeliveryChannel(moduleName);
				if (com.king.gmms.messagequeue.StreamQueueManager.getInstance().produceServerDeliveryMessage(msg)) {
					MetricsCollector.getInstance().incrementCounter(MetricsNames.business(
							MetricsNames.FLOW_CORE, msg, MetricsNames.ACTION_ROUTING_SUCCESS));
					MetricsCollector.getInstance().incrementCounter(MetricsNames.business(
							MetricsNames.FLOW_CORE, msg, MetricsNames.ACTION_ROUTED_TO_REDIS));
					MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(
							MetricsNames.STAGE_OUT_SUBMIT, msg, true, MetricsNames.ACTION_ROUTED_TO_REDIS));
					if(log.isInfoEnabled()){
						log.info(msg, "[MO-TO-SERVER] rssid={} module={} redisId={}", msg.getRSsID(), moduleName,
								msg.getRedisStreamID());
					}
				} else {
					MetricsCollector.getInstance().incrementCounter(MetricsNames.business(
							MetricsNames.FLOW_CORE, msg, MetricsNames.ACTION_ROUTING_FAILED));
					MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(
							MetricsNames.STAGE_OUT_SUBMIT, msg, true, MetricsNames.ACTION_ROUTING_FAILED));
					msg.setStatus(GmmsStatus.SERVER_ERROR);
					if(log.isInfoEnabled()){
						log.info(msg, "Failed to dispatch SMPP delivery task to Redis Stream for rssid {}, module {}",
								msg.getRSsID(), moduleName);
					}
					dbHandler.putMsg(msg);
				}
				continue;
			}
			if (com.king.gmms.messagequeue.StreamQueueManager.getInstance().produceOutboundMessage(msg)) {
				MetricsCollector.getInstance().incrementCounter(MetricsNames.business(
						MetricsNames.FLOW_CORE, msg, MetricsNames.ACTION_ROUTING_SUCCESS));
				MetricsCollector.getInstance().incrementCounter(MetricsNames.business(
						MetricsNames.FLOW_CORE, msg, MetricsNames.ACTION_ROUTED_TO_REDIS));
				MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(
						MetricsNames.STAGE_OUT_SUBMIT, msg, true, MetricsNames.ACTION_ROUTED_TO_REDIS));
				if(log.isInfoEnabled()){
					// held for a rate limited channel: [MT-HOLD] was logged instead, [MT-RELEASE] follows
					if (!com.king.gmms.messagequeue.ChannelReleaseDispatcher.getInstance().holds(msg)) {
						log.info(msg, "[MT-TO-CLIENT] rssid={} redisId={} path={}", msg.getRSsID(), msg.getRedisStreamID(), com.king.gmms.util.ModulePath.withSelf(msg.getModulePath()));
					}
				}
			} else {
				MetricsCollector.getInstance().incrementCounter(MetricsNames.business(
						MetricsNames.FLOW_CORE, msg, MetricsNames.ACTION_ROUTING_FAILED));
				MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(
						MetricsNames.STAGE_OUT_SUBMIT, msg, true, MetricsNames.ACTION_ROUTING_FAILED));
				msg.setStatus(GmmsStatus.SERVER_ERROR);
				if(log.isInfoEnabled()){
					log.info(msg, "Failed to dispatch SMPP MT task to Redis Stream for rssid {}", msg.getRSsID());
				}
				dbHandler.putMsg(msg);
			}

		}
		
	}
	
	/**
	 * Every send to a channel starts from what the customer sent (after Core's own steps on it): the text before the
	 * content replacement, the sender before the channel's steps, the recipient as it came in. A backup channel or a
	 * retry from the DB does its replacements once, for its own channel (docs/通道外发处理恢复与替换缺陷修复设计.md 3.3, 4.2).
	 */
	private void startFromCustomerText(GmmsMessage message) {
		String in = message.getInTextContent();
		if (in == null || in.isEmpty()) {
			message.setInTextContent(message.getTextContent());
			message.setInContentType(message.getContentType());
		} else if (!in.equals(message.getTextContent())) {
			String type = message.getInContentType() == null ? message.getContentType() : message.getInContentType();
			message.setTextContent(in);
			message.setContentType(type);
			if (GmmsMessage.AIC_CS_UCS2.equalsIgnoreCase(type)) {
				message.setGsm7bit(false);
			}
			try {
				message.setMessageSize(in.getBytes(type).length);
			} catch (Exception e) {
				message.setMessageSize(in.length());
			}
		}
		String sender = message.getInSenderAddress();
		if (sender == null || sender.isEmpty()) {
			message.setInSenderAddress(message.getSenderAddress());
		} else {
			message.setSenderAddress(sender);
		}
		String recipient = message.getOriginalRecipientAddr();
		if (recipient != null && !recipient.isEmpty()) {
			message.setRecipientAddress(recipient);
		}
	}

	/**
	 * SMSOptionContentReplacementPolicy of the customer: prefix=all first, then the prefixes of the recipient from the
	 * shortest to the longest, each group's rules in the order written. A rule that fails is skipped alone.
	 */
	private void replaceContent(GmmsMessage message, A2PCustomerInfo ocustomer) {
		Map<String, Map<String, String>> groups = ocustomer == null ? null : ocustomer.getSmsOptionReplaceContent();
		if (groups == null || groups.isEmpty()) {
			return;
		}
		String recipient = message.getRecipientAddress();
		if (recipient != null && recipient.startsWith("+")) {
			recipient = recipient.substring(1);
		}
		List<String> prefixes = new ArrayList<String>();
		for (String prefix : groups.keySet()) {
			if (!"all".equalsIgnoreCase(prefix) && recipient != null && recipient.startsWith(prefix)) {
				prefixes.add(prefix);
			}
		}
		Collections.sort(prefixes, new Comparator<String>() {
			public int compare(String a, String b) { return a.length() - b.length(); }
		});
		for (String prefix : groups.keySet()) {
			if ("all".equalsIgnoreCase(prefix)) {
				prefixes.add(0, prefix);
			}
		}
		String text = message.getTextContent();
		for (String prefix : prefixes) {
			boolean all = "all".equalsIgnoreCase(prefix);
			for (Map.Entry<String, String> rule : groups.get(prefix).entrySet()) {
				String key = rule.getKey(), value = rule.getValue();
				if (value == null) {
					continue;
				}
				try {
					if (!all && key.equals(value) && key.contains("@phone#")) {
						text = gmmsUtility.replacePhonePattern(text, key, recipient);
					} else {
						Pattern pattern = Pattern.compile(key);
						text = pattern.matcher(text).replaceAll(replacement(value.trim(), pattern.matcher("").groupCount()));
					}
				} catch (Exception e) {
					log.warn(message, "[CONTENT-REPLACE] ossid={} prefix={} rule {}:{} skipped: {}", message.getOSsID(), prefix, key, value, e.toString());
				}
			}
		}
		if (!text.equals(message.getTextContent())) {
			message.setTextContent(text);
			try {
				message.setMessageSize(text.getBytes(message.getContentType()).length);
			} catch (Exception e) {
				message.setMessageSize(text.length());
			}
		}
	}

	/**
	 * a replacement for replaceAll: $n stays a reference to group n of the key when the key has that group, any other
	 * $ and \ are literal ("price:$5" gives "$5")
	 */
	static String replacement(String value, int groups) {
		StringBuilder b = new StringBuilder();
		for (int i = 0; i < value.length(); i++) {
			char c = value.charAt(i);
			boolean reference = c == '$' && i + 1 < value.length() && Character.isDigit(value.charAt(i + 1))
					&& value.charAt(i + 1) - '0' <= groups;
			if (c == '\\' || (c == '$' && !reference)) {
				b.append('\\');
			}
			b.append(c);
		}
		return b.toString();
	}

	/**
	 * an MO: from a vendor (cm Role: Vendor, as the CDR marks it with ServiceTypeID 3) to a customer (not a vendor).
	 * A vendor's message routed on to another vendor stays an MT.
	 */
	private boolean isMoToCustomer(GmmsMessage msg, A2PCustomerInfo rCustomer) {
		if (!GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg.getMessageType())
				|| rCustomer == null || "Vendor".equalsIgnoreCase(rCustomer.getRole())) {
			return false;
		}
		A2PCustomerInfo oCustomer = ctm.getCustomerBySSID(msg.getOSsID());
		return oCustomer != null && "Vendor".equalsIgnoreCase(oCustomer.getRole());
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
			ChannelOutboundPreparer.prefix4DrMo(message, server);
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
			moduleName = moduleManager.selectChannel(queue);

			if (moduleName == null) {
				// normal for HTTP customers and stream delivery: the DR goes to the stream below
				if(log.isDebugEnabled()){
					log.debug(message, "Can not find moduleName to send message.");
				}
				// V4.0 Fallback: Even if no direct moduleName found, we produce to Redis
				// Core shouldn't care about specific module name in the async model
			}

			// V4.1 High-Reliability Decentralized DR Push
			boolean success = false;
			String streamKey = null;
			com.king.gmms.messagequeue.StreamQueueManager streamQueueManager =
					com.king.gmms.messagequeue.StreamQueueManager.getInstance();
			boolean http = server != null && server.isHttpProtocol();
			if (http) {
				message.setDeliveryChannel(moduleName);
			} else {
				// Old logic for Carrier DR ingestion or Sticky routing
				streamKey = streamQueueManager.getDrPendingQueue(message);
				preserveDrTargetModule(message, streamKey);
			}
			// in a batch of vendor DRs: written with the batch, before its ack (docs/DR消费减少Redis往返设计.md 3.4)
			final String drStream = streamKey;
			if (streamQueueManager.deferDeliveryReport(http ? streamQueueManager.httpDeliveryReportKey(message) : streamKey,
					message, new com.king.gmms.messagequeue.StreamQueueManager.DrProduceTail() {
						public void produced(boolean ok) {
							afterDrProduced(message, drStream, ok);
						}
					})) {
				return;
			}
			success = http ? streamQueueManager.produceHttpDeliveryReport(message)
					: streamQueueManager.produceDeliveryReportToQueue(message, streamKey);
			afterDrProduced(message, streamKey, success);
		} else if (ctm.isA2P(oA2P) || ctm.isPartition(oA2P)) {
			// DR for another A2P (peering): no internal connection in stream mode, as the MT above
			log.warn(message, "This DR can't send out because can't get available session.");
			message.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.getCode());
			dbHandler.putMsg(message);
		}
	}

	private void preserveDrTargetModule(GmmsMessage message, String streamKey) {
		if (message == null || streamKey == null) {
			return;
		}
		String targetModule = extractDrTargetModule(streamKey);
		if (targetModule == null || targetModule.trim().length() == 0) {
			return;
		}
		String current = message.getDeliveryChannel();
		String currentModule = extractModuleName(current);
		String localModule = System.getProperty("module");
		if (currentModule == null || currentModule.trim().length() == 0
				|| localModule != null && localModule.equalsIgnoreCase(currentModule)) {
			message.setDeliveryChannel(targetModule);
		}
	}

	private String extractDrTargetModule(String streamKey) {
		if (streamKey == null) {
			return null;
		}
		String prefix = "stream:dr:pending:";
		if (!streamKey.startsWith(prefix)) {
			return null;
		}
		String rest = streamKey.substring(prefix.length());
		int firstColon = rest.indexOf(':');
		if (firstColon < 0 || firstColon + 1 >= rest.length()) {
			return null;
		}
		String moduleAndShard = rest.substring(firstColon + 1);
		int lastColon = moduleAndShard.lastIndexOf(':');
		if (lastColon > 0) {
			return moduleAndShard.substring(0, lastColon);
		}
		return moduleAndShard;
	}

	private String extractModuleName(String deliveryChannel) {
		if (deliveryChannel == null || deliveryChannel.trim().length() == 0) {
			return null;
		}
		int idx = deliveryChannel.lastIndexOf(':');
		if (idx >= 0 && idx + 1 < deliveryChannel.length()) {
			return deliveryChannel.substring(idx + 1);
		}
		return deliveryChannel;
	}

	/** After the DR to a customer is produced (at once, or with its batch of vendor DRs) */
	private void afterDrProduced(GmmsMessage message, String streamKey, boolean success) {
		if (!success) {
			MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(
					MetricsNames.STAGE_OUT_DR, message, false, MetricsNames.ACTION_ROUTING_FAILED));
			if(log.isInfoEnabled()){
				log.info(message,
							"Can not produce the DR message to Redis Stream");
			}
			message.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.getCode());
			dbHandler.putMsg(message);
		} else {
			MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(
					MetricsNames.STAGE_OUT_DR, message, false, MetricsNames.ACTION_ROUTED_TO_REDIS));
			if(log.isDebugEnabled()){
				log.debug(message, "Success to produce DR message to Redis Stream. stream={}", streamKey);
			}
			if(log.isInfoEnabled()){
				if (streamKey != null) {
					log.info(message, "[DR-TO-SERVER] ossid={} status={} stream={} redisId={} path={}", message.getOSsID(),
							message.getStatusCode(), streamKey, message.getRedisStreamID(), com.king.gmms.util.ModulePath.withSelf(message.getModulePath()));
				} else {
					log.info(message, "[DR-TO-HTTP] ossid={} status={} redisId={} path={}", message.getOSsID(),
							message.getStatusCode(), message.getRedisStreamID(), com.king.gmms.util.ModulePath.withSelf(message.getModulePath()));
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
		moduleName = moduleManager.selectChannel(queue);

		if (moduleName == null) {
			message
					.setStatusCode(GmmsStatus.FAIL_QUERY_DELIVERREPORT
							.getCode());
			dbHandler.putMsg(message);
			return;
		}

		// a DR query to the channel went over the internal connection: none in stream mode (queries are not used)
		if(log.isInfoEnabled()){
			log.info(message, "Can not find sender queue");
		}
		message.setStatusCode(GmmsStatus.FAIL_QUERY_DELIVERREPORT.getCode());
		dbHandler.putMsg(message);
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
