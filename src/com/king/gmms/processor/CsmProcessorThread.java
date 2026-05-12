package com.king.gmms.processor;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.routing.DeliveryRouterHandler;
import com.king.gmms.threadpool.RunnableMsgTask;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageStoreManager;

public class CsmProcessorThread extends RunnableMsgTask {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(CsmProcessorThread.class);

	private MessageStoreManager msm;
	

	private DeliveryRouterHandler deliveryRouterHandler = null;
	private DBBackupHandler dbHandler = null;
	CsmIntegrityCache csmIntegrityCache = null;
	private A2PCustomerManager ctm;

	public CsmProcessorThread(GmmsMessage msg) {
		this.message = msg;
		msm = GmmsUtility.getInstance().getMessageStoreManager();
		deliveryRouterHandler = DeliveryRouterHandler.getInstance();
		dbHandler = DBBackupHandler.getInstance();
		 csmIntegrityCache = CsmProcessorHandler.getInstance().getCSMCache();
		 ctm = GmmsUtility.getInstance().getCustomerManager();
	}

	@Override
	public void run() {
		if (message == null) {
			return;
		}
		if(log.isInfoEnabled()){
			log.info(message, 	"CsmThread transact this message,{}",message.toString());
		}
		transact(message);
	}

	/**
	 * @param message
	 * @see com.king.gmms.jms.GmmsThread#transact(com.king.message.gmms.GmmsMessage)
	 */
	protected void transact(final GmmsMessage message) {
		try {
			CsmKeyInfo csmKeyInfo = CsmUtility.getCsmKeyInfoFromGmmsMessage(message);
			
			// check whether cache exist
			csmIntegrityCache.checkCsmq(csmKeyInfo);

			// add check, in case of cc 02 01; cc 02 03; cc 02 02
			if (CsmUtility.isValidCsms(message)) {
				// insert current msg to cache
				csmIntegrityCache.putAndInsertCsmq(csmKeyInfo, message);
			} else {
				log.info(message,
						"csm messge params illegal, SarSegmentSeqNum="
								+ message.getSarSegmentSeqNum()
								+ " ,SarTotalSeqments="
								+ message.getSarTotalSeqments());

				// error msg, insert to db for expired process
				msm.sendToCsmq(message);
				return;
			}
			
			// log in_submit CDR for csm
			msm.handleInSubmit4Csm(message);
			
			// assemble
			GmmsMessage assembledMsg = csmIntegrityCache.assembleMessages(csmKeyInfo, message);
			if (assembledMsg != null) {
				////charge recipient 重复次数
				A2PCustomerInfo oCustomer = ctm.getCustomerBySSID(assembledMsg.getOSsID());
				int count = oCustomer.getSmsOptionRecipientMaxSendCountIn24H();
				if(count>0){
					String key = "Duplicate:"+assembledMsg.getRecipientAddress();
					long currentCount = GmmsUtility.getInstance().getRedisClient().incrString(key);
					if(currentCount == 1){
						GmmsUtility.getInstance().getRedisClient().setExpire(key, 24*60*60);
					}
					if(currentCount>count){
						assembledMsg.setStatus(GmmsStatus.RECIPIENT_ERROR_BY_MAX_COUNT);
						assembledMsg.setRSsID(GmmsUtility.getInstance().getBlackholeSsid());
						msm.handleOutSubmitResForInnack(assembledMsg);
						return;
					}
				}
				
				// send to normal message queue
				while (!deliveryRouterHandler.putMsg(assembledMsg)) {
					try {
						Thread.sleep(50);
					} catch (InterruptedException e) {
						log.warn("Interrupted!", e);
					}
				}

				
			}
		} catch (Exception e) {
			log.error(message, "Occur error when transact csm message", e);
			if (message != null) {
				message.setStatus(GmmsStatus.SERVER_ERROR);
				dbHandler.putMsg(message);
			}
		}
	}

}
