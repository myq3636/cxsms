package com.king.gmms.processor;

import java.util.Map;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageStoreManager;

/**
 * What the old Core did for a channel when it handed an MT to the Client over its internal session
 * (InternalCoreEngineSession.sendNewMessage, f49bd6d), which stream mode never called
 * (docs/通道外发处理恢复与替换缺陷修复设计.md): the sender length of the channel, the channel's sender, recipient and content
 * lists, the channel's SenderReplacement and Prefix4NewMT; and Prefix4DRMO of a DR to the customer (sendReport).
 * Each send to a channel starts from the addresses before the channel's steps (MessageProcessorThread restores them).
 */
public final class ChannelOutboundPreparer {
	private static final SystemLogger log = SystemLogger.getSystemLogger(ChannelOutboundPreparer.class);

	private ChannelOutboundPreparer() {
	}

	/**
	 * The channel's steps on an MT about to be sent to it. False: refused by a list of the channel, its failure result
	 * already handled (handleOutSubmitRes), not to be sent.
	 */
	public static boolean prepare(GmmsMessage msg, A2PCustomerInfo channel, MessageStoreManager msm) {
		if (channel == null) {
			return true;
		}
		A2PCustomerManager ctm = GmmsUtility.getInstance().getCustomerManager();
		controlSenderLength(msg, channel);
		// an rp entry of the channel's whitelist replaces the sender and sets originalSenderAddr: the customer's DR keeps the customer's
		String customerSender = msg.getOriginalSenderAddr();
		GmmsStatus refused = checkLists(msg, channel, ctm);
		msg.setOriginalSenderAddr(customerSender);
		if (refused != null) {
			log.info(msg, "[MT-CHANNEL] rssid={} refused by the channel's list: {} sender={} recipient={}", channel.getSSID(),
					refused.getCode(), msg.getSenderAddress(), msg.getRecipientAddress());
			msg.setStatus(refused);
			if (refused == GmmsStatus.RECIPIENT_ERROR_BY_BL) {
				msm.handleOutSubmitRes(msg, true);
			} else {
				msm.handleOutSubmitRes(msg);
			}
			return false;
		}
		String sender = msg.getSenderAddress(), recipient = msg.getRecipientAddress();
		try {
			if (channel.isSmsOptionAdvancedSenderReplacement()) {
				ctm.doSenderAddressReplace(channel.getSSID(), msg);
			}
		} catch (Exception e) {
			log.error(msg, "do sender replace of the channel failed", e);
		}
		if (channel.isHandlePrefix4NewMT()) {
			String[] address = new String[] { msg.getSenderAddress(), msg.getRecipientAddress() };
			channel.handlePrefix4NewMt(address);
			msg.setSenderAddress(address[0]);
			msg.setRecipientAddress(address[1]);
		}
		if (!String.valueOf(sender).equals(msg.getSenderAddress()) || !String.valueOf(recipient).equals(msg.getRecipientAddress())) {
			log.info(msg, "[MT-CHANNEL] rssid={} sender {} -> {}, recipient {} -> {}", channel.getSSID(), sender, msg.getSenderAddress(),
					recipient, msg.getRecipientAddress());
		}
		return true;
	}

	/** SMSOptionOriNumberLenControl: under Min the Suffix is added once, over Max the sender is cut to Max (as before) */
	static void controlSenderLength(GmmsMessage msg, A2PCustomerInfo channel) {
		Map<String, String> control = channel.getOriNumberLenController();
		String sender = msg.getSenderAddress();
		if (control == null || control.isEmpty() || sender == null) {
			return;
		}
		try {
			String min = control.get("Min"), max = control.get("Max"), suffix = control.get("Suffix");
			String s = sender;
			if (min != null && s.length() < Integer.parseInt(min.trim())) {
				s = s + (suffix == null ? "" : suffix);
			}
			if (max != null && s.length() > Integer.parseInt(max.trim())) {
				s = s.substring(0, Integer.parseInt(max.trim()));
			}
			if (!s.equals(sender)) {
				log.info(msg, "[MT-CHANNEL] rssid={} SMSOptionOriNumberLenControl: sender {} -> {}", channel.getSSID(), sender, s);
				msg.setSenderAddress(s);
			}
		} catch (Exception e) {
			log.error(msg, "do sender address length check error", e);
		}
	}

	/** the channel's lists, then the default vendor lists (-2): null when the MT may go */
	static GmmsStatus checkLists(GmmsMessage msg, A2PCustomerInfo channel, A2PCustomerManager ctm) {
		int rssid = channel.getSSID();
		if (!"VIP".equalsIgnoreCase(channel.getCustomerTypeBySender())) {
			String recipient = msg.getRecipientAddress();
			if (recipient.startsWith("+")) recipient = recipient.substring(1);
			if (recipient.startsWith("00")) recipient = recipient.substring(2);
			if (recipient.startsWith("0")) recipient = recipient.substring(1);
			if (channel.isNonPaSenderCheck(recipient)) {
				if (ctm.doSenderBlacklistCheck(-2, msg)) {
					return GmmsStatus.SENDER_ERROR_BY_BL;
				}
			} else if (ctm.doSenderBlacklistCheck(rssid, msg) || ctm.doSenderBlacklistCheck(-2, msg)) {
				return GmmsStatus.SENDER_ERROR_BY_BL;
			}
			if (!ctm.doVendorSenderWhitelistCheck(rssid, msg)) {
				return GmmsStatus.SENDER_ERROR_BY_WL;
			}
		}
		if (ctm.doRecipientBlacklistCheck(rssid, msg) || ctm.doRecipientBlacklistCheck(-2, msg)) {
			return GmmsStatus.RECIPIENT_ERROR_BY_BL;
		}
		if (ctm.doContentBlacklistCheck(rssid, msg) || ctm.doContentBlacklistCheck(-2, msg)) {
			return GmmsStatus.SPAMED_CTBL;
		}
		return null;
	}

	/** SMSOption{Add,Remove,Replace}{Ori,Rec}Prefix4DRMO of the customer on its DR (after its own addresses are back) */
	public static void prefix4DrMo(GmmsMessage dr, A2PCustomerInfo customer) {
		if (customer == null || !customer.isHandlePrefix4DRMO()) {
			return;
		}
		String[] address = new String[] { dr.getSenderAddress(), dr.getRecipientAddress() };
		customer.handlePrefix4DrMo(address);
		dr.setSenderAddress(address[0]);
		dr.setRecipientAddress(address[1]);
	}
}
