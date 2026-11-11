package com.king.gmms.processor;

import java.util.Date;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageStoreManager;

/**
 * DRs the customer gets in place of the vendor's (docs/假DR逻辑重新设计.md):
 * <ol>
 * <li>customer SMSOptionSendDRByInSubmitInCU / SMSOptionSendFakeDRByInSubmitRecPrefixInCU: when the MT is routed;</li>
 * <li>channel SMSOptionSendDRByOutSubmit (recipients in SMSOptionRecPrefixForFakeDR): when the vendor accepted it,
 *     unless the customer has SMSOptionFakeDRDisable;</li>
 * <li>customer SMSOptionDRSuccessRatio: a real vendor DR that is not DELIVERED becomes DELIVERED by that ratio;</li>
 * <li>customer SMSOptionFakeDRTimeoutSec: no final vendor DR that long after the accept ({@link FakeDrTimeoutScheduler}).</li>
 * </ol>
 * Once a fake DR is decided the MT carries {@link #GIVEN_BIT}; MessageStoreManager then sends the customer no other
 * DR for it (real, failure or expiry DRs are only recorded). The bits live in milterActionCode, which is kept in
 * Redis, the DB tables and the stream entries, so the decision follows the MT wherever it goes.
 */
public final class FakeDr {
	private static final SystemLogger log = SystemLogger.getSystemLogger(FakeDr.class);
	/** milterActionCode: the customer's final DR was a fake DR */
	public static final int GIVEN_BIT = 15;
	/** milterActionCode: way 4 is timing this MT */
	public static final int TIMER_BIT = 16;

	private FakeDr() {
	}

	public static boolean given(GmmsMessage m) {
		return m != null && m.getMilterActionCode(GIVEN_BIT);
	}

	public static boolean timerOn(GmmsMessage m) {
		return m != null && m.getMilterActionCode(TIMER_BIT);
	}

	private static A2PCustomerInfo customer(int ssid) {
		return GmmsUtility.getInstance().getCustomerManager().getCustomerBySSID(ssid);
	}

	private static MessageStoreManager store() {
		return GmmsUtility.getInstance().getMessageStoreManager();
	}

	/** SMSOptionFakeDRSuccessRatio with the SMSOptionDRSuccessBiasValue spread: 100 always DELIVERED, 0 never */
	public static GmmsStatus fakeStatus(A2PCustomerInfo cust) {
		int ratio = cust == null ? 100 : cust.getFakeDrSuccessRatio();
		if (ratio >= 100) {
			return GmmsStatus.DELIVERED;
		}
		if (ratio <= 0) {
			return GmmsStatus.UNDELIVERABLE;
		}
		return GmmsUtility.isModifySuccessDR(cust.getSSID(), ratio, cust.getDrBiasRatio()) ? GmmsStatus.DELIVERED
				: GmmsStatus.UNDELIVERABLE;
	}

	/** the fake DR: a copy of the MT, without the bits of the MT */
	static GmmsMessage build(GmmsMessage mt, GmmsStatus status) {
		GmmsMessage dr = new GmmsMessage(mt);
		dr.setMilterActionCode(GIVEN_BIT, false);
		dr.setMilterActionCode(TIMER_BIT, false);
		dr.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		dr.setStatus(status);
		dr.setDrErrorCode(com.king.gmms.domain.DrErrorConfig.getInstance().exactCode(status.getCode()));
		dr.setRetriedNumber(0);
		dr.setNextRetryTime(null);
		dr.setDateIn(new Date());
		dr.setInTimeMs(System.currentTimeMillis());
		dr.setOutTimeMs(0L);
		dr.setDeliveryChannel(System.getProperty("module"));
		return dr;
	}

	private static void send(GmmsMessage mt, GmmsMessage dr, String mode, int delaySec) {
		mt.setMilterActionCode(GIVEN_BIT, true);
		log.info(mt, "[FAKE-DR] mode={} ossid={} rssid={} status={} delaySec={}", mode, mt.getOSsID(), mt.getRSsID(),
				dr.getStatusCode(), delaySec);
		store().insertMsgForDRDelay(dr, delaySec);
	}

	/** way 1: the MT is routed and about to go to the Client */
	public static void onRouted(GmmsMessage m) {
		try {
			if (m == null || !GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(m.getMessageType()) || given(m)
					|| m.getRetriedNumber() >= 1 || !m.getDeliveryReport()) {
				return;
			}
			// a backup route (more than three channels tried) is not a new MT
			String rssids = m.getRoutingSsIDs();
			if (rssids != null && rssids.length() - rssids.replace(",", "").length() > 2) {
				return;
			}
			A2PCustomerInfo cust = customer(m.getOSsID());
			if (cust == null || !cust.isFakeDrByInSubmit(m.getRecipientAddress())) {
				return;
			}
			send(m, build(m, fakeStatus(cust)), "IN_SUBMIT", GmmsUtility.getRandomValueByDelayTimeInterval(cust.getDrDelayTimeInSec()));
		} catch (Exception e) {
			log.warn(m, "fake DR on routing failed (ignored): {}", e.getMessage());
		}
	}

	/** way 2, or way 4 starts timing: the vendor accepted the first part; before the MT is stored for its DR */
	public static void onOutSubmitOk(GmmsMessage m, A2PCustomerInfo cust, A2PCustomerInfo channel) {
		if (m == null || cust == null || given(m) || !m.getDeliveryReport() || m.getSplitStatus() > 1) {
			return;
		}
		String recipient = m.getRecipientAddress();
		if (channel != null && !m.hasDRFailedReroute() && channel.isFakeDrByOutSubmit(recipient) && !cust.isFakeDrDisable()) {
			send(m, build(m, fakeStatus(cust)), "OUT_SUBMIT", GmmsUtility.getRandomValueByDelayTimeInterval(channel.getDrDelayTimeInSec()));
			return;
		}
		if (cust.getFakeDrTimeoutSec() > 0 && m.getOutMsgID() != null && m.getOutMsgID().length() > 0
				&& FakeDrTimeoutScheduler.getInstance().schedule(m.getOutMsgID(), cust.getFakeDrTimeoutSec())) {
			m.setMilterActionCode(TIMER_BIT, true);
			log.debug(m, "fake DR timeout {}s started. outMsgId={}", cust.getFakeDrTimeoutSec(), m.getOutMsgID());
		}
	}

	/** way 4: the timeout of this MT is up; mt is the MT as stored for its DR */
	static void onTimeout(GmmsMessage mt) {
		A2PCustomerInfo cust = customer(mt.getOSsID());
		String how = cust == null ? "success" : cust.getFakeDrTimeoutStatus();
		GmmsStatus status = "fail".equals(how) ? GmmsStatus.UNDELIVERABLE
				: "ratio".equals(how) ? fakeStatus(cust) : GmmsStatus.DELIVERED;
		send(mt, build(mt, status), "TIMEOUT", 0);
	}

	/**
	 * A vendor DR matched to its MT: a final one ends the way-4 timing; when the timeout DR was sent already,
	 * the MT counts as given, the vendor's DR is only recorded.
	 */
	public static void onVendorDr(GmmsMessage m) {
		if (!timerOn(m)) {
			return;
		}
		int code = m.getStatusCode();
		if (code == GmmsStatus.ACCEPT.getCode() || code == GmmsStatus.ENROUTE.getCode()) {
			return;
		}
		boolean timeoutSent = !FakeDrTimeoutScheduler.getInstance().cancel(m.getOutMsgID());
		m.setMilterActionCode(TIMER_BIT, false);
		if (timeoutSent) {
			m.setMilterActionCode(GIVEN_BIT, true);
			log.info(m, "[DR-SKIP] outMsgId={} status={}: the timeout DR was sent before, the vendor DR is only recorded",
					m.getOutMsgID(), code);
		}
	}

	/** way 3: a real vendor DR to the customer, not DELIVERED, becomes DELIVERED by SMSOptionDRSuccessRatio */
	public static void rewriteRealDr(GmmsMessage dr, A2PCustomerInfo cust) {
		if (dr == null || cust == null || given(dr) || cust.getDrSucRatio() <= 0
				|| dr.getStatusCode() == GmmsStatus.DELIVERED.getCode()
				|| !cust.isDrSuccessRatioRecipient(dr.getRecipientAddress())) {
			return;
		}
		boolean hit = cust.getDrSucRatio() >= 100
				|| GmmsUtility.isModifySuccessDR(cust.getSSID(), cust.getDrSucRatio(), cust.getDrBiasRatio());
		if (hit) {
			log.info(dr, "[DR-REWRITE] ossid={} from={} to={}", dr.getOSsID(), dr.getStatusCode(), GmmsStatus.DELIVERED.getCode());
			dr.setStatus(GmmsStatus.DELIVERED);
			dr.setDrErrorCode("000");
		}
	}
}
