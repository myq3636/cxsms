package com.king.gmms.client;

import java.util.ArrayList;
import java.util.List;

import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.systemmanagement.ConnectionManagementForFunction;
import com.king.gmms.customerconnectionfactory.InternalAgentConnectionFactory;
import com.king.gmms.customerconnectionfactory.MultiSmppClientFactory;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PMultiConnectionInfo;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.domain.ModuleManager;
import com.king.message.gmms.GmmsMessage;

public class MultiSmppClient extends AbstractClient {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(MultiSmppClient.class);
	private final String PROTOCOL_NAME = "SMPP";
	private MultiSmppClientFactory customerFactory = null;
	public MultiSmppClient() {
		customerFactory = MultiSmppClientFactory.getInstance();
	}

	private void initConnectionFactory() {
		try {
			ArrayList<Integer> alSsid = gmmsUtility.getCustomerManager()
					.getSsidByProtocol(PROTOCOL_NAME);
			if (alSsid != null && alSsid.size() > 0) {
				A2PCustomerInfo ci;
				for (int i = 0; i < alSsid.size(); i++) {
					int ssid = alSsid.get(i);
					log.trace("{} get ssid: {}", PROTOCOL_NAME, ssid);
					ci = gmmsUtility.getCustomerManager().getCustomerBySSID(
							ssid);
					/**
					 * single connection type
					 */
					if (ci.getConnectionType() == 1) {
						if ((((A2PSingleConnectionInfo) ci).isChlInit())
								&& ctm.inCurrentA2P(ctm.getConnectedRelay(ssid,
										GmmsMessage.AIC_MSG_TYPE_TEXT))) {
							customerFactory.initConnectionFactory(ssid, ci
									.getConnectionType());
						}
					} else {
						if (((A2PMultiConnectionInfo) ci).isInit()
								&& ctm.inCurrentA2P(ctm.getConnectedRelay(ssid,
										GmmsMessage.AIC_MSG_TYPE_TEXT))) {
							if(log.isInfoEnabled()){
								log.info("Start the client {} directly.", ssid);
							}
							customerFactory.initConnectionFactory(ssid, ci
									.getConnectionType());
						}
					}
				}
			} // end of size > 0
			else {
				log.info("No client is started directly.");
			}
		} catch (Exception ex) {
			log.debug(ex, ex);
		}
	}

	public boolean startService() {
		initConnectionFactory();
		if (!initSystemManagement()) {
			log.warn("module register failed!");
		}
		customerFactory.initializeSession();
		
		log.info("Redis stream mode enabled, skip legacy internal agent connections.");
		
		return true;
	}

	public boolean stopService() {
		beforeStop();
		stopOutboundConsumer();
		stopRedisHeartbeat();
		return true;
	}

	/**
	 * send stop request
	 */
	public void beforeStop() {
		// Module stopping handled implicitly by heartbeat expiry
	}
}
