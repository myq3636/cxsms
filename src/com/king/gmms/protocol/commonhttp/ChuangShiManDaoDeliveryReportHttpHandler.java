package com.king.gmms.protocol.commonhttp;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.domain.http.HttpInterface;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

public class ChuangShiManDaoDeliveryReportHttpHandler extends HttpHandler {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(ChuangShiManDaoDeliveryReportHttpHandler.class);

	public ChuangShiManDaoDeliveryReportHttpHandler(HttpInterface hie) {
		super(hie);
		// TODO Auto-generated constructor stub
	}

	@Override
	public String makeRequest(GmmsMessage message, String urlEncoding,
			A2PCustomerInfo cst) throws UnsupportedEncodingException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String makeResponse(HttpStatus hs, GmmsMessage message,
			A2PCustomerInfo cst) throws IOException, ServletException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public HttpStatus parseRequest(GmmsMessage message,
			HttpServletRequest request) throws ServletException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void parseResponse(GmmsMessage message, String resp) {
		// TODO Auto-generated method stub

	}

	public HttpStatus parseListRequest(List<GmmsMessage> msgs,
			HttpServletRequest request) throws ServletException, IOException {
		try {
			int rssid = -1;

			A2PCustomerInfo csts = null;
			A2PSingleConnectionInfo sInfo = null;
			String protocol = hi.getInterfaceName();
			if (protocol != null && protocol.trim().length() > 0) {
				ArrayList<Integer> alSsid = gmmsUtility.getCustomerManager()
						.getSsidByProtocol(protocol);
				if (alSsid == null || alSsid.size() < 1) {
					log
							.warn(
									"getSsid by interfaceName {} failed in parseListRequest() function.",
									protocol);
					return drFail;
				}
				rssid = alSsid.get(0);
				csts = gmmsUtility.getCustomerManager()
						.getCustomerBySSID(rssid);
				sInfo = (A2PSingleConnectionInfo) csts;
			}

			if (sInfo == null) {
				return drFail;
			}

			String[] messages = null;
			String args = request.getParameter("args");
			
			// throttling control process
			if (!super.checkIncomingThrottlingControl(csts.getSSID(), args)) {
				return drFail;
			}

			if (args != null && !("").equals(args.trim())) {
				messages = args.split(";");
			} else {
				return drFail;
			}

			for (String msg : messages) {
				String[] parmeters = msg.split(",");
				if (parmeters.length != 6) {
					return drFail;
				} else {

					String reportID = parmeters[0];
					String teFuNumber = parmeters[1];
					String mobile = parmeters[2];
					String rrid = parmeters[3];
					String statusCode = parmeters[4];
					String time = parmeters[5];

					GmmsMessage drMsg = new GmmsMessage();
					drMsg.setMsgID(rrid);
					drMsg.setOutMsgID(rrid);
					drMsg.setRecipientAddress(mobile);
					GmmsStatus gs = null;
					try {
						gs = processStatus(statusCode, null);
					} catch (Exception e) {
						log
								.error(drMsg,
										"process message status error in parseListRequest() function." + e.toString());
					}
					drMsg.setStatus(gs);
					if(log.isDebugEnabled()){
		        		log.debug(drMsg, "outmsgid:{}" , rrid);
					}
					drMsg.setRSsID(rssid);
					msgs.add(drMsg);
				}
			}

			return drSuccess;
		} catch (Exception e) {
			log.error(e, e);
			return drFail;
		}
	}

	private GmmsStatus processStatus(String statusCode, String statusText)
			throws Exception {
		if(log.isDebugEnabled()){
			log.debug("DR statuscode={},statusText={}", statusCode, statusText);
		}
		GmmsStatus gs = GmmsStatus.UNKNOWN;
		try {
			HttpStatus hs = new HttpStatus(statusCode, statusText);
			gs = hi.mapHttpDRStatus2GmmsStatus(hs);
		} catch (NumberFormatException e) {
			log.error("Parse status code error!", e);
			throw e;
		} catch (Exception e) {
			log.error("Process status error!", e);
			throw e;
		}
		return gs;
	}

}
