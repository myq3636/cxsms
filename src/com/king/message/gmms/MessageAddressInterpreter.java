package com.king.message.gmms;

import java.util.HashMap;
import java.util.Map;

import com.king.db.Data;
import com.king.db.DataManagerException;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.domain.A2PSingleConnectionInfo;

/**
 * <p>
 * Title: MessageAddressInterpreter
 * </p>
 * <p>
 * Description: A utility class that does the message format transforming and
 * address resolving
 * </p>
 * <p>
 * Copyright: Copyright (c) 2004
 * </p>
 * <p>
 * Company:
 * </p>
 * 
 * @author Jesse Duan
 * @version 6.1
 */
public class MessageAddressInterpreter {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(MessageAddressInterpreter.class);
	private GmmsUtility gmmsUtility = GmmsUtility.getInstance();

	/**
	 * Modified by Neal to remove keyword from content, 2005.03.30 When
	 * recipient address is a shortcode, then place keyword in the recipient
	 * address
	 * 
	 * @param msg
	 *            GmmsMessage original message
	 * @param oServer
	 *            Server
	 * @return int
	 */
	public int shortcode2ServerID(GmmsMessage msg,
			A2PSingleConnectionInfo oServer) {
		int rSsid = 0;

		String sysId = null; // example: King
		String shortcode = null; // example: 78467
		String serverId = null; // example: MsgRay

		String source = null;

		if (oServer.getSystemId() != null) {
			sysId = oServer.getSystemId();
		} else {
			log.warn("SysID is null!");
			return rSsid;
		}
		A2PCustomerManager cm = gmmsUtility.getCustomerManager();
		if (msg.getRecipientAddress() != null) {
			shortcode = msg.getRecipientAddress();

			if (this.isShortcode(shortcode)) {
				if (GmmsMessage.AIC_MSG_TYPE_TEXT.equalsIgnoreCase(msg
						.getGmmsMsgType())) { // if it's a Txt msg, then take
												// the keyword from message body

					String expand = oServer.getShortCodeIfExpand();
					if (expand == null || "no".equalsIgnoreCase(expand)) { // short
																			// code
																			// cant
																			// expand

						source = msg.getTextContent();
						// the first word stops at any white space or (,;:-) is
						// the keyword
						if (source != null && source.length() > 0) {
							source = source.trim();

							String start = source
									.split("\\s|\\,|\\;|\\:|\\-|\\.")[0];
							if (sysId.equals(start)) { // content start with
														// SysID
								// when test with WebEx, only remove SysID and a
								// "."
								serverId = "MsgRay";
								msg.setTextContent(source.substring(start
										.length() + 1));
							} else { // content start with serverId
								serverId = start;
								if (serverId.length() + 1 < source.length()) {
									msg.setTextContent(source
											.substring(serverId.length() + 1));
								}
							}
							msg.setRecipientAddress(serverId);
							rSsid = cm.getSsidByShortcode(shortcode);
							msg.setRSsID(rSsid);
						}
					} else { // shortcode can expand
						source = msg.getRecipientAddress();
						if ("sender".equalsIgnoreCase(expand)) {
							// RecipientAddress is like 98467+PSN

							Map<String, Integer> hm = cm.getAllShortcode();
							for (String shortCode : hm.keySet()) {
								shortcode = shortCode;
								int index = source.indexOf(shortcode);
								if (index > -1) {
									String psn = source.substring(shortcode
											.length());
									msg.setRecipientAddress(psn); // remove
																	// shortcode
																	// from
																	// RecipientAddress
								}
								rSsid = cm.getSsidByShortcode(shortcode);
								msg.setRSsID(rSsid);
								break;
							}
						} else if ("sysid".equalsIgnoreCase(expand)) {
							// RecipientAddress is like 9500+8111
							// need to remove SysID from RecipientAddress,
							// and remove serverID and "." from textContent
							sysId = oServer.getSystemId();
							if (sysId == null) {
								log.warn("Can't get SysID from CCB!");
								return rSsid;
							}
							int index = source.indexOf(sysId);
							if (index > 0) {
								shortcode = source.substring(0, index);
								rSsid = cm.getSsidByShortcode(shortcode);
								msg.setRSsID(rSsid);
								try {
									A2PCustomerInfo temp = gmmsUtility
											.getCustomerManager()
											.getCustomerBySSID(rSsid);
									if (temp == null)
										return 0;

									serverId = temp.getServerID();

									msg.setRecipientAddress(serverId);
									String content = msg.getTextContent();
									if (content.startsWith(serverId)) {
										// remove serverId from TextContent
										msg
												.setTextContent(content
														.substring(serverId
																.length() + 1));
									}
								} catch (Exception ex) {
									log.error(ex, ex);
								}
							}
						} else if ("both".equalsIgnoreCase(expand)
								|| "all".equalsIgnoreCase(expand)) {
							// RecipientAddress is shortcode + SysID + from
							// remove shortcode and SysID from RecipientAddress
							sysId = oServer.getSystemId();
							if (sysId == null) {
								log.warn("Can't get SysID from CCB!");
								return rSsid;
							}
							int index = source.indexOf(sysId);
							if (index > 0) {
								shortcode = source.substring(0, index);
								rSsid = cm.getSsidByShortcode(shortcode);
								msg.setRSsID(rSsid);
								String psn = source.substring(index
										+ sysId.length());
								msg.setRecipientAddress(psn);
							}
						}
					}
				}
			}
		}
		return rSsid;
	}

	/**
	 * Use ServerID in the sender address, and recipient phone number to resolve
	 * short code
	 * 
	 * @param msg
	 *            GmmsMessage
	 * @param server
	 *            Server
	 */
	public void serverID2Shortcode(GmmsMessage msg,
			A2PSingleConnectionInfo server) {
		String source = msg.getSenderAddress();
		String serverID = null; // msg.getKeyword();
		String sysId = server.getSystemId();

		A2PCustomerInfo customer = null;
		A2PCustomerManager ctm = gmmsUtility.getCustomerManager();
		if (serverID != null && this.isServerID(serverID)) {
			try {
				customer = ctm.getCustomerByServerID(serverID);
				if (customer == null) {
					throw new Exception(
							"Unable to resolve customer with the serverID: "
									+ serverID);
				}

				// use recipient address to resolve shortcode, and replace
				// sender
				// address with the shortcode
				String shortcode = customer
						.resolveShortCode(unifyPhoneNumber(msg
								.getRecipientAddress()));

				if (GmmsMessage.AIC_MSG_TYPE_TEXT.equalsIgnoreCase(msg
						.getGmmsMsgType())
						|| GmmsMessage.AIC_MSG_TYPE_BINARY.equalsIgnoreCase(msg
								.getGmmsMsgType())) {
					// if it's a Txt or txt/binary msg, and if the text content
					// does not already contain the keyword (server ID)
					// then add it to the text content
					String expand = server.getShortCodeIfExpand();
					if (expand == null || "no".equalsIgnoreCase(expand)) {
						// Due the recipient doesn't support shortcode expand,
						// need to add sysid information to text content.
						msg.setSenderAddress(shortcode);
						if (sysId != null) {
							msg.setTextContent(sysId + "." + source + "."
									+ msg.getTextContent());
						} else { // use dedicate short code
							if (!msg.getTextContent().startsWith(serverID)) {
								msg.setTextContent(serverID + "." + source
										+ "." + msg.getTextContent());
							}
						}
					} else { // shortcode can expand
						if ("sender".equalsIgnoreCase(expand)) {
							// replace Sender with shortcode+from, such as
							// 78467+psn
							msg.setSenderAddress(shortcode
									+ msg.getSenderAddress());
						} else if ("sysid".equalsIgnoreCase(expand)) {
							// replace Sender with SysId, and add serverID in
							// textContent
							// such as 9500+8111
							msg.setSenderAddress(shortcode + sysId);
							if (!msg.getTextContent().startsWith(serverID)) {
								msg.setTextContent(serverID + "."
										+ msg.getTextContent());
							}
						} else if ("both".equalsIgnoreCase(expand)) { // shortcode
																		// +
																		// SysID
																		// +
																		// from
							msg.setSenderAddress(shortcode + sysId
									+ msg.getSenderAddress());
						}
					}
				}
				// else if(GmmsMessage.AIC_MSG_TYPE_MMS.equalsIgnoreCase(msg.
				// getGmmsMsgType())) {
				// // if it's an MMS, and if the subject line does not already
				// //contain the keyword (serverID)
				// // then add it to the subject
				// if(server.getShortCodeIfExpand() == null ||
				// "yes".equalsIgnoreCase(server.getShortCodeIfExpand())) {
				// if(!msg.getSubject().startsWith(serverID)) {
				// msg.setSubject(serverID + " " + msg.getSubject());
				// }
				// }
				// else { // Due the recipient doesn't support shortcode expand,
				// //need to add sysid information
				// if(!msg.getSubject().startsWith(serverID)) {
				// msg.setSubject(server.getSysId() + " " + serverID +
				// " " + msg.getSubject());
				// }
				// else {
				// msg.setSubject(server.getSysId() +
				// " " + msg.getSubject());
				// }
				// }
				// }
				if(log.isTraceEnabled()){
					log.trace("Sender is: {} Recipient is: {} Content is: {}", msg
						.getSenderAddress(), msg.getRecipientAddress(), msg
						.getTextContent());
				}
			} catch (Exception e) {
				log.error(e, e);
			}
		}
	}

	/**
	 * Unifies different phone number format by stripping off all charactors
	 * other than the numbers 0~9
	 * 
	 * @param phoneNumber
	 *            String
	 * @return String clean-uped number
	 */
	private String unifyPhoneNumber(String phoneNumber) {
		if (phoneNumber == null) {
			return null;
		}
		char[] nums = phoneNumber.toCharArray();
		char[] cleanNum = new char[nums.length];
		int j = 0;

		for (char num : nums) {
			if (num >= '0' && num <= '9') {
				cleanNum[j++] = num;
			}
		}
		return (new String(cleanNum)).trim();
	}

	/**
	 * isPhoneNumber Matches the phone number with following format + 1 (408)
	 * 324-1830
	 * 
	 * @param address
	 *            String
	 * @return boolean
	 */
	public boolean isPhoneNumber(String address) {
		if (isShortcode(address)) {
			return false;
		}
		return address.matches("^\\+{0,1}[0-9]{9,}$");
	}

	/**
	 * isServerID Matches the Server ID with alphanumeric, "_" and "."
	 * 
	 * @param address
	 *            String
	 * @return boolean
	 */
	public boolean isServerID(String address) {
		if (address.indexOf(".") > 0) {
			return false; // added by Neal 2005.04.07
		}
		return address.matches("^[a-zA-Z](\\w|\\.)+$");
	}

	/**
	 * isShortcode Matches the shortcode with 4~8 digits of number
	 * 
	 * @param address
	 *            String
	 * @return boolean
	 */
	public boolean isShortcode(String address) {
		return gmmsUtility.getCustomerManager().isShortcode(address)
				&& address.matches("^(\\d){4,8}$");
	}

	public void add(Data data) throws DataManagerException {
		throw new DataManagerException("No Implement Management");
	}
}
