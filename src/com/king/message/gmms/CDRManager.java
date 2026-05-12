package com.king.message.gmms;

import java.util.Date;

import com.king.framework.*;
import com.king.gmms.GmmsUtility;
import com.king.gmms.PhoneUtils;
import com.king.gmms.domain.A2PCustomerInfo;

import java.text.SimpleDateFormat;

/**
 * <p>
 * Title: CDRManager
 * </p>
 * <p>
 * Description: manage CDR
 * </p>
 * <p>
 * Copyright: Copyright (c) 2001-2010
 * </p>
 * <p>
 * Company: King.Inc
 * </p>
 * 
 * @version 6.1
 * @author: Jesse Duan
 */
public class CDRManager {
	private static SystemLogger log = SystemLogger.getSystemLogger(CDRManager.class);
	private GmmsUtility gmmsUtility;
	private static long maxFileTime;
	private static int maxFileSize;
	private CDRWriter cdrWriter = null;
	private CDRAsynWriter cdrAsynWriter = null;

	public CDRManager() {
		gmmsUtility = GmmsUtility.getInstance();
		maxFileTime = gmmsUtility.getCdrMaxTime() * 1000L;
		maxFileSize = gmmsUtility.getCdrMaxSize() * 1024;
		cdrWriter = CDRWriter.getInstance();
		cdrWriter.initialize();
		cdrAsynWriter = CDRAsynWriter.getInstance();
		cdrAsynWriter.initialize();
	}

	public void close() {
		CDRWriter.getInstance().stopCDRFileMonitor();
	}

	public static long getMaxFileTime() {
		return maxFileTime;
	}

	public static int getMaxFileSize() {
		return maxFileSize;
	}

	private boolean add(CDR cdr) {
		String cdrStr = makeCDR(cdr);
		log.debug(new StringBuilder(" ").append(cdr.getMsgID())
				.append(" CDR: ").append(cdrStr).toString());
		/*
		 * log.fatal(new StringBuilder(" ").append(cdr.getMsgID())
		 * .append(" CDR: ").append(cdrStr).toString());
		 */
		try {
			A2PCustomerInfo oCustomer = gmmsUtility.getCustomerManager().getCustomerBySSID(cdr.getOSsID());
			A2PCustomerInfo rCustomer = gmmsUtility.getCustomerManager().getCustomerBySSID(cdr.getRSsID());		    
		     if((oCustomer!=null && oCustomer.isSmsOptionWrMonitorCDR())||(rCustomer!=null && rCustomer.isSmsOptionWrMonitorCDR())) {
		    	 String redisCDRStr = makeRedisCDR(cdr);
		 		gmmsUtility.getRedisClient().asyncLpush("CDR", redisCDRStr);
		     }
		} catch (Exception e) {
			// TODO: handle exception
		}
		
		if (!cdrAsynWriter.recoreCDR(cdrStr + "\n")) {
			return cdrWriter.recoreCDR(cdrStr + "\n");
		} else {
			return true;
		}
	}

	private boolean add4StoreDR(CDR cdr) {
		String stmt = null;
		try {
			StringBuilder sb = new StringBuilder(1024);
			sb.append("0").append(",");
			sb.append(makeCdrStr(cdr.getCdrType())).append(",");
			sb.append(makeCdrStr(cdr.getInTransID())).append(",");
			sb.append(makeCdrStr(cdr.getOutTransID())).append(",");
			sb.append(makeCdrStrEnclosed(cdr.getInMsgID())).append(",");
			sb.append(makeCdrStrEnclosed(cdr.getOutMsgID())).append(",");
			sb.append(cdr.getOSsID()).append(",");
			sb.append(cdr.getRSsID()).append(",");
			sb.append(makeCdrStr(cdr.getGmmsMsgType())).append(",");
			sb.append(makeCdrStr(cdr.getMessageType())).append(",");
			sb.append(cdr.getMessageSize()).append(",");
			if (cdr.getOriginalSenderAddr() != null
					&& cdr.getOriginalSenderAddr().trim().length() > 0) {
				sb.append(makeCdrStrEnclosed(cdr.getOriginalSenderAddr()))
						.append(",");
			} else {
				sb.append(makeCdrStr(cdr.getProtocolVersion())).append(",");
			}
			
			sb.append(makeCdrStr(cdr.getSenderAddrType())).append(",");
			//RecipientAddrType or rmncmcc
			if (cdr.getRMncMcc() != null
					&& cdr.getRMncMcc().trim().length() > 0) {
				sb.append(makeCdrStrEnclosed(cdr.getRMncMcc()))
						.append(",");
			} else {
				sb.append(makeCdrStr(cdr.getRecipientAddrType())).append(",");
			}
			// 1.5 way special process			
			sb.append(makeCdrStrEnclosed(cdr.getSenderAddress())).append(
					",");
			// 1.5 way special process
			if (cdr.getOriginalRecipientAddr() != null
					&& cdr.getOriginalRecipientAddr().trim().length() > 0) {
				sb.append(makeCdrStrEnclosed(cdr.getOriginalRecipientAddr()))
						.append(",");
			} else {
				sb.append(makeCdrStr(cdr.getRecipientAddress())).append(",");
			}

			sb.append(formatDate(cdr.getTimeStamp())).append(",");
			sb.append(formatDate(cdr.getExpiryDate())).append(",");
			sb.append((cdr.getDeliveryReport() ? "1" : "0")).append(",");
			sb.append(cdr.getPriority()).append(",");
			sb.append(
					makeCdrStrEnclosed(gmmsUtility.modifybackslash(cdr.isGsm7bit()?GmmsMessage.AIC_CS_ASCII:cdr
							.getContentType(), 1))).append(",");
			sb.append(
					makeCdrStrEnclosed(gmmsUtility.modifybackslash(cdr
							.getTextContent(), 1))).append(",");
			sb.append(cdr.getStatusCode()).append(",");
			sb.append(makeCdrStr(cdr.getStatusText())).append(",");
			
//			sb.append((cdr.inClientPull() ? "1" : "0")).append(",");
			
			sb.append((cdr.getInClientPull())).append(",");
			
			sb.append((cdr.outClientPull() ? "1" : "0")).append(",");
			sb.append(makeCdrStrEnclosed(cdr.getDeliveryChannel())).append(",");
			sb.append(formatDate(cdr.getDateIn())).append(",");
			sb.append(cdr.getMilterActionCode()).append(",");
			sb.append(cdr.getOoperator()).append(",");
			sb.append((cdr.getRoperator()/10000>0?cdr.getRoperator()%10000:0)).append(",");
			sb.append(cdr.getActionCode()).append(",");
			sb.append(cdr.getCurrentA2P()).append(",");
			sb.append(cdr.getOA2P()).append(",");
			sb.append(cdr.getRA2P()).append(",");
			try {
				String routingRssids = cdr.getRoutingSsIDs();
				if((CDR.TYPE_IN_DELI_REP.equalsIgnoreCase(cdr.getCdrType()))
						||routingRssids == null || "".equalsIgnoreCase(routingRssids.trim())){
					sb.append(makeCdrStrEnclosed(cdr.getMsgID())).append(",");
				}else{
					String[] rssidArr = routingRssids.trim().split(",");
					if(rssidArr.length>2 && !rssidArr[0].contains("0")){
						sb.append(makeCdrStrEnclosed(cdr.getMsgID()+"-"+rssidArr[rssidArr.length-1])).append(",");
					}else{
						sb.append(makeCdrStrEnclosed(cdr.getMsgID())).append(",");
					}
				}
			} catch (Exception e) {
				sb.append(makeCdrStrEnclosed(cdr.getMsgID())).append(",");
			}
			//sb.append(makeCdrStrEnclosed(cdr.getMsgID())).append(",");
			sb.append(cdr.getSplitStatus()).append(",");
			sb.append(makeCdrStr(cdr.getSarMsgRefNum())).append(",");
			sb.append(cdr.getSarTotalSeqments() == 0? 1: cdr.getSarTotalSeqments()).append(",");
			sb.append(cdr.getSarSegmentSeqNum() == 0? 1: cdr.getSarSegmentSeqNum()).append(",");
			sb.append(new Date().getTime()).append(",");
			sb.append((cdr.isInCsm() ? "1" : "0")).append(",");
			sb.append(cdr.getServiceTypeID()).append(",");
			sb.append(formatDate(cdr.getScheduleDeliveryTime())).append(",");
			BillingStatus billingStatus = BillingStatus.getStatus(cdr.getStatusText());
			sb.append(billingStatus.getBillingCode());
			stmt = sb.toString();
			log.info(new StringBuilder(" ").append(cdr.getMsgID()).append(
					" CDR: ").append(sb).toString());
			/*
			 * log.fatal(new StringBuilder(" ").append(cdr.getMsgID()).append(
			 * " CDR: ").append(sb).toString());
			 */
			A2PCustomerInfo oCustomer = gmmsUtility.getCustomerManager().getCustomerBySSID(cdr.getOSsID());
			A2PCustomerInfo rCustomer = gmmsUtility.getCustomerManager().getCustomerBySSID(cdr.getRSsID());
			try {
				if((oCustomer!=null && oCustomer.isSmsOptionWrMonitorCDR())||(rCustomer!=null && rCustomer.isSmsOptionWrMonitorCDR())) {
					StringBuilder cdrToRedis = new StringBuilder(1024);
					cdrToRedis.append("0").append(",");
					cdrToRedis.append(makeCdrStr(cdr.getCdrType())).append(",");			
					cdrToRedis.append(makeCdrStrEnclosed(cdr.getInMsgID())).append(",");
					cdrToRedis.append(makeCdrStrEnclosed(cdr.getOutMsgID())).append(",");
					cdrToRedis.append(cdr.getOSsID()).append(",");
					cdrToRedis.append(cdr.getRSsID()).append(",");			
					cdrToRedis.append(cdr.getMessageSize()).append(",");			
					// 1.5 way special process			
					cdrToRedis.append(makeCdrStrEnclosed(cdr.getSenderAddress())).append(
							",");
					// 1.5 way special process
					if (cdr.getOriginalRecipientAddr() != null
							&& cdr.getOriginalRecipientAddr().trim().length() > 0) {
						cdrToRedis.append(makeCdrStrEnclosed(cdr.getOriginalRecipientAddr()))
								.append(",");
					} else {
						cdrToRedis.append(makeCdrStr(cdr.getRecipientAddress())).append(",");
					}
					cdrToRedis.append(formatDate(cdr.getTimeStamp())).append(",");
					cdrToRedis.append(cdr.getStatusCode()).append(",");	
					cdrToRedis.append(makeCdrStrEnclosed(cdr.getDeliveryChannel())).append(",");
					try {
						String routingRssids = cdr.getRoutingSsIDs();
						if((CDR.TYPE_IN_DELI_REP.equalsIgnoreCase(cdr.getCdrType()))
								||routingRssids == null || "".equalsIgnoreCase(routingRssids.trim())){
							cdrToRedis.append(makeCdrStrEnclosed(cdr.getMsgID())).append(",");
						}else{
							String[] rssidArr = routingRssids.trim().split(",");
							if(rssidArr.length>2 && !rssidArr[0].contains("0")){
								cdrToRedis.append(makeCdrStrEnclosed(cdr.getMsgID()+"-"+rssidArr[rssidArr.length-1])).append(",");
							}else{
								cdrToRedis.append(makeCdrStrEnclosed(cdr.getMsgID())).append(",");
							}
						}
					} catch (Exception e) {
						cdrToRedis.append(makeCdrStrEnclosed(cdr.getMsgID())).append(",");
					}
					cdrToRedis.append(
							makeCdrStrEnclosed(gmmsUtility.modifybackslash(cdr
									.getTextContent(), 1))).append(",");
					cdrToRedis.append(new Date().getTime());
					gmmsUtility.getRedisClient().asyncLpush("CDR", cdrToRedis.toString());
				}
			} catch (Exception e) {
				// TODO: handle exception
			}
			
			
			if (!cdrAsynWriter.recoreCDR(stmt + "\n")) {
				return cdrWriter.recoreCDR(stmt + "\n");
			} else {
				return true;
			}
		} catch (Exception ex) {
			log.error(ex, ex);
		}
		return false;
	}
	
	private String makeRedisCDR(CDR cdr) {
		StringBuilder cdrToRedis = new StringBuilder(1024);		
				cdrToRedis.append("0").append(",");
				cdrToRedis.append(makeCdrStr(cdr.getCdrType())).append(",");			
				cdrToRedis.append(makeCdrStrEnclosed(cdr.getInMsgID())).append(",");
				cdrToRedis.append(makeCdrStrEnclosed(cdr.getOutMsgID())).append(",");
				cdrToRedis.append(cdr.getOSsID()).append(",");
				cdrToRedis.append(cdr.getRSsID()).append(",");			
				cdrToRedis.append(cdr.getMessageSize()).append(",");			
				// 1.5 way special process			
				cdrToRedis.append(makeCdrStrEnclosed(cdr.getSenderAddress())).append(
						",");
				// 1.5 way special process
				if (cdr.getOriginalRecipientAddr() != null
						&& cdr.getOriginalRecipientAddr().trim().length() > 0) {
					cdrToRedis.append(makeCdrStrEnclosed(cdr.getOriginalRecipientAddr()))
							.append(",");
				} else {
					cdrToRedis.append(makeCdrStr(cdr.getRecipientAddress())).append(",");
				}
				cdrToRedis.append(formatDate(cdr.getTimeStamp())).append(",");
				cdrToRedis.append(cdr.getStatusCode()).append(",");	
				cdrToRedis.append(makeCdrStrEnclosed(cdr.getDeliveryChannel())).append(",");
				try {
					String routingRssids = cdr.getRoutingSsIDs();
					if((CDR.TYPE_IN_DELI_REP.equalsIgnoreCase(cdr.getCdrType()))
							||routingRssids == null || "".equalsIgnoreCase(routingRssids.trim())){
						cdrToRedis.append(makeCdrStrEnclosed(cdr.getMsgID())).append(",");
					}else{
						String[] rssidArr = routingRssids.trim().split(",");
						if(rssidArr.length>2 && !rssidArr[0].contains("0")){
							cdrToRedis.append(makeCdrStrEnclosed(cdr.getMsgID()+"-"+rssidArr[rssidArr.length-1])).append(",");
						}else{
							cdrToRedis.append(makeCdrStrEnclosed(cdr.getMsgID())).append(",");
						}
					}
				} catch (Exception e) {
					cdrToRedis.append(makeCdrStrEnclosed(cdr.getMsgID())).append(",");
				}
				cdrToRedis.append(
						makeCdrStrEnclosed(gmmsUtility.modifybackslash(cdr
								.getTextContent(), 1))).append(",");
				cdrToRedis.append(new Date().getTime());
		
		return cdrToRedis.toString();
	}

	private String makeCDR(CDR cdrRecord) {
		String stmt = null;
		try {
			StringBuilder sb = new StringBuilder(1024);
			sb.append("0").append(",");
			sb.append(makeCdrStr(cdrRecord.getCdrType())).append(",");
			sb.append(makeCdrStr(cdrRecord.getInTransID())).append(",");
			sb.append(makeCdrStr(cdrRecord.getOutTransID())).append(",");
			sb.append(makeCdrStrEnclosed(cdrRecord.getInMsgID())).append(",");
			sb.append(makeCdrStrEnclosed(cdrRecord.getOutMsgID())).append(",");
			sb.append(cdrRecord.getOSsID()).append(",");
			sb.append(cdrRecord.getRSsID()).append(",");
			sb.append(makeCdrStr(cdrRecord.getGmmsMsgType())).append(",");
			sb.append(makeCdrStr(cdrRecord.getMessageType())).append(",");
			sb.append(cdrRecord.getMessageSize()).append(",");
			if (cdrRecord.getOriginalSenderAddr() != null
					&& cdrRecord.getOriginalSenderAddr().trim().length() > 0) {
				sb.append(makeCdrStrEnclosed(cdrRecord.getOriginalSenderAddr()))
						.append(",");
			} else {
				sb.append(makeCdrStr(cdrRecord.getProtocolVersion())).append(",");
			}
			
			sb.append(makeCdrStr(cdrRecord.getSenderAddrType())).append(",");
			//RecipientAddrType or rmncmcc
			if (cdrRecord.getRMncMcc() != null
					&& cdrRecord.getRMncMcc().trim().length() > 0) {
				sb.append(makeCdrStrEnclosed(cdrRecord.getRMncMcc()))
						.append(",");
			} else {
				sb.append(makeCdrStr(cdrRecord.getRecipientAddrType())).append(",");
			}
			// 1.5 way special process			
			if((CDR.TYPE_OUT_SUBMIT.equalsIgnoreCase(cdrRecord.getCdrType()))
				    && cdrRecord.getOutsender() != null && !"".equalsIgnoreCase(cdrRecord.getOutsender().trim())){
				sb.append(makeCdrStrEnclosed(cdrRecord.getOutsender())).append(",");
			}else{
				sb.append(makeCdrStrEnclosed(cdrRecord.getSenderAddress()))
				.append(",");
			}
			// 1.5 way special process
			if (cdrRecord.getOriginalRecipientAddr() != null
					&& cdrRecord.getOriginalRecipientAddr().trim().length() > 0) {
				sb
						.append(
								makeCdrStrEnclosed(cdrRecord
										.getOriginalRecipientAddr())).append(
								",");
			} else {
				sb.append(makeCdrStr(cdrRecord.getRecipientAddress())).append(
						",");
			}
			sb.append(formatDate(cdrRecord.getTimeStamp())).append(",");
			sb.append(formatDate(cdrRecord.getExpiryDate())).append(",");
			sb.append((cdrRecord.getDeliveryReport() ? "1" : "0")).append(",");
			sb.append(cdrRecord.getPriority()).append(",");
			sb.append(
					makeCdrStrEnclosed(gmmsUtility.modifybackslash(cdrRecord.isGsm7bit()?GmmsMessage.AIC_CS_ASCII:cdrRecord
							.getContentType(), 1))).append(",");
			sb.append(
					makeCdrStrEnclosed(gmmsUtility.modifybackslash(cdrRecord
							.getTextContent(), 1))).append(",");
			int code = cdrRecord.getStatusCode();
			sb.append((code == -1 ? 0 : code)).append(",");
			sb.append(makeCdrStr(cdrRecord.getStatusText())).append(",");
			
//			sb.append((cdrRecord.inClientPull() ? "1" : "0")).append(",");
			sb.append((cdrRecord.getInClientPull())).append(",");
			
			sb.append((cdrRecord.outClientPull() ? "1" : "0")).append(",");
			sb.append(makeCdrStrEnclosed(cdrRecord.getDeliveryChannel())).append(",");
			sb.append(formatDate(new Date())).append(",");
			sb.append(cdrRecord.getMilterActionCode()).append(",");
			sb.append(cdrRecord.getOoperator()).append(",");
			sb.append((cdrRecord.getRoperator()/10000>0?cdrRecord.getRoperator()%10000:0)).append(",");
			sb.append(cdrRecord.getActionCode()).append(",");
			sb.append(cdrRecord.getCurrentA2P()).append(",");
			sb.append(cdrRecord.getOA2P()).append(",");
			sb.append(cdrRecord.getRA2P()).append(",");
			try {
				String routingRssids = cdrRecord.getRoutingSsIDs();
				if((CDR.TYPE_IN_DELI_REP.equalsIgnoreCase(cdrRecord.getCdrType()))
						||routingRssids == null || "".equalsIgnoreCase(routingRssids.trim())){
					sb.append(makeCdrStrEnclosed(cdrRecord.getMsgID())).append(",");
				}else{
					String[] rssidArr = routingRssids.trim().split(",");
					if(rssidArr.length>2 && !rssidArr[0].contains("0")){
						sb.append(makeCdrStrEnclosed(cdrRecord.getMsgID()+"-"+rssidArr[rssidArr.length-1])).append(",");
					}else{
						sb.append(makeCdrStrEnclosed(cdrRecord.getMsgID())).append(",");
					}
				}
			} catch (Exception e) {
				sb.append(makeCdrStrEnclosed(cdrRecord.getMsgID())).append(",");
			}
			//sb.append(makeCdrStrEnclosed(cdrRecord.getMsgID())).append(",");
			sb.append(cdrRecord.getSplitStatus()).append(",");
			sb.append(makeCdrStr(cdrRecord.getSarMsgRefNum())).append(",");
			sb.append(cdrRecord.getSarTotalSeqments() == 0? 1: cdrRecord.getSarTotalSeqments()).append(",");
			sb.append(cdrRecord.getSarSegmentSeqNum() == 0? 1: cdrRecord.getSarSegmentSeqNum()).append(",");
			sb.append(new Date().getTime()).append(",");
			sb.append((cdrRecord.isInCsm() ? "1" : "0")).append(",");
			sb.append(cdrRecord.getServiceTypeID()).append(",");
			sb.append(formatDate(cdrRecord.getScheduleDeliveryTime())).append(",");
			sb.append(cdrRecord.hasRetryFlag()? "1" : "0");
			stmt = sb.toString();
		} catch (Exception ex) {
			log.error(ex, ex);
		}
		return stmt;
	}

	public boolean logInSubmit(GmmsMessage msg) {
		try {
			int ossid = msg.getOSsID();
			if(gmmsUtility.getCustomerManager()
			.getCustomerBySSID(ossid)
			.isNeedStatisticMsgCount()){
				gmmsUtility.getRedisClient().asyncHashInc("StMsgCount", "IN:"+ossid);
			}
			
			if(gmmsUtility.getCustomerManager()
					.getCustomerBySSID(ossid)
					.isNeedStatisticMsgCountByCountry()){
				String cc = PhoneUtils.getRegionCodeByPhone(msg.getRecipientAddress());
				gmmsUtility.getRedisClient().asyncHashInc("StMsgCount", "IN:"+ossid+":"+cc);
			}
			
		} catch (Exception e) {
			
		}
		CDR cdr = new CDR(msg);
		cdr.setCdrType(CDR.TYPE_IN_SUBMIT);
		return add(cdr);
	}

	public boolean logInSubmitPartial(GmmsMessage msg) {
		CDR cdr = new CDR(msg);
		cdr.setCdrType(CDR.TYPE_IN_SUBMIT_PARTIAL);
		return add(cdr);
	}

	public boolean logInSubmitRes(GmmsMessage msg) {
		return true;
	}

	public boolean logOutSubmitReq(GmmsMessage msg) {
		return true;
	}

	public boolean logOutSubmitReq(GmmsMessage msg, boolean needDR) {
		return true;
	}

	public boolean logOutSubmitRes(GmmsMessage msg) {
		try {
			int rssid = msg.getRSsID();
			if(gmmsUtility.getCustomerManager()
			.getCustomerBySSID(rssid)
			.isNeedStatisticMsgCount()&& msg.getStatusCode()!=2500){
				gmmsUtility.getRedisClient().asyncHashInc("StMsgCount", "OUT:"+rssid);
			}
			if(gmmsUtility.getCustomerManager()
					.getCustomerBySSID(rssid)
					.isNeedStatisticMsgCountByCountry()&& msg.getStatusCode()!=2500){
				String cc = PhoneUtils.getRegionCodeByPhone(msg.getRecipientAddress());
				gmmsUtility.getRedisClient().asyncHashInc("StMsgCount", "OUT:"+rssid+":"+cc);						
			}
		} catch (Exception e) {
			
		}
		CDR cdr = new CDR(msg);
		cdr.setCdrType(CDR.TYPE_OUT_SUBMIT);
		return add(cdr);
	}

	public boolean logOutSubmitRes(GmmsMessage msg, boolean needDR) {
		try {
			int rssid = msg.getRSsID();
			if(gmmsUtility.getCustomerManager()
			.getCustomerBySSID(rssid)
			.isNeedStatisticMsgCount() && msg.getStatusCode()!=2500){
				gmmsUtility.getRedisClient().asyncHashInc("StMsgCount", "OUT:"+rssid);
			}
			if(gmmsUtility.getCustomerManager()
					.getCustomerBySSID(rssid)
					.isNeedStatisticMsgCountByCountry()&& msg.getStatusCode()!=2500){
				String cc = PhoneUtils.getRegionCodeByPhone(msg.getRecipientAddress());
				gmmsUtility.getRedisClient().asyncHashInc("StMsgCount", "OUT:"+rssid+":"+cc);						
			}
		} catch (Exception e) {
			
		}
		CDR cdr = new CDR(msg);
		cdr.setDeliveryReport(needDR);
		cdr.setCdrType(CDR.TYPE_OUT_SUBMIT);
		return add(cdr);
	}

	public boolean logInDelivery(GmmsMessage msg) {
		CDR cdr = new CDR(msg);
		cdr.setCdrType(CDR.TYPE_IN_DELI);
		return add(cdr);
	}

	public boolean logInDeliveryPartial(GmmsMessage msg) {
		CDR cdr = new CDR(msg);
		cdr.setCdrType(CDR.TYPE_IN_DELI_PARTIAL);
		return add(cdr);
	}

	public boolean logInDeliveryRes(GmmsMessage msg) {
		return true;
	}

	public boolean logOutDeliveryReq(GmmsMessage msg) {
		return true;
	}

	public boolean logOutDeliveryReq(GmmsMessage msg, boolean needDR) {
		return true;
	}

	public boolean logOutDeliveryRes(GmmsMessage msg) {
		CDR cdr = new CDR(msg);
		cdr.setCdrType(CDR.TYPE_OUT_DELI);
		return add(cdr);
	}

	public boolean logOutDeliveryRes(GmmsMessage msg, boolean needDR) {
		CDR cdr = new CDR(msg);
		cdr.setDeliveryReport(needDR);
		cdr.setCdrType(CDR.TYPE_OUT_DELI);
		return add(cdr);
	}

	public boolean logInDeliveryReportReq(GmmsMessage msg) {
		return true;
	}

	public boolean logInDeliveryReportRes(GmmsMessage msg) {
		if(log.isTraceEnabled()){
			log.trace(msg, "updating CDR after sending out DR");
		}
		CDR cdr = new CDR(msg);
		cdr.setCdrType(CDR.TYPE_IN_DELI_REP);
		return add(cdr);
	}

	public boolean logOutDeliveryReportReq(GmmsMessage msg) {
		return true;
	}

	public boolean logOutDeliveryReportRes(GmmsMessage msg) {
		try {
			int rssid = msg.getRSsID();
			if(gmmsUtility.getCustomerManager()
			.getCustomerBySSID(rssid)
			.isNeedStatisticMsgCount()){
				gmmsUtility.getRedisClient().asyncHashInc("StMsgCount", "OUTDR:"+rssid);
			}
			
			if(gmmsUtility.getCustomerManager()
					.getCustomerBySSID(rssid)
					.isNeedStatisticMsgCountByCountry()){
				String cc = PhoneUtils.getRegionCodeByPhone(msg.getRecipientAddress());
				gmmsUtility.getRedisClient().asyncHashInc("StMsgCount", "OUTDR:"+rssid+":"+cc);						
			}
			
		} catch (Exception e) {
			
		}
		CDR cdr = new CDR(msg);
		cdr.setCdrType(CDR.TYPE_OUT_DELI_REP);
		return add(cdr);
	}

	public boolean logOutDeliveryReportRes4StoreDR(GmmsMessage msg) {
		try {
			int rssid = msg.getRSsID();
			if(gmmsUtility.getCustomerManager()
			.getCustomerBySSID(rssid)
			.isNeedStatisticMsgCount()){
				gmmsUtility.getRedisClient().asyncHashInc("StMsgCount", "OUTDR:"+rssid);
			}
			if(gmmsUtility.getCustomerManager()
					.getCustomerBySSID(rssid)
					.isNeedStatisticMsgCountByCountry()){
				String cc = PhoneUtils.getRegionCodeByPhone(msg.getRecipientAddress());
				gmmsUtility.getRedisClient().asyncHashInc("StMsgCount", "OUTDR:"+rssid+":"+cc);						
			}
		} catch (Exception e) {
			
		}
		CDR cdr = new CDR(msg);
		cdr.setCdrType(CDR.TYPE_OUT_DELI_REP);
		return add4StoreDR(cdr);
	}

	/* Added by Bill, August,2004 */
	public boolean logOutReadReplyReport(GmmsMessage msg) {
		CDR cdr = new CDR(msg);
		cdr.setCdrType(CDR.TYPE_OUT_READ_REPLY_REP);
		return add(cdr);
	}

	public boolean logInReadReplyReport(GmmsMessage msg) {
		CDR cdr = new CDR(msg);
		cdr.setCdrType(CDR.TYPE_IN_READ_REPLY_REP);
		return add(cdr);
	}

	private String formatDate(Date d) {
		if (d == null) {
			return "NULL";
		}
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return formatter.format(d);
	}

	private String makeCdrStr(String strCdr) {
		if (strCdr == null) {
			return "NULL";
		}
		return strCdr;
	}

	public String makeCdrStrEnclosed(String strCdr) {
		if (strCdr == null) {
			return "NULL";
		}
		strCdr = strCdr.replaceAll("\"", "\"\"");
		return new StringBuilder("\"").append(strCdr).append("\"").toString();
	}
	
	public static void main(String[] args) {
		String s = ",12,";
		String[] arr = s.split(",");
		for(int i=0; i<arr.length; i++){
			System.out.println("--i--"+arr[i]+"--");
		}
	}
}
