package com.king.gmms.protocol.commonhttp;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpParam;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

public class DirectSMSMessageHttpHandler extends CommonMessageHttpHandler {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(DirectSMSMessageHttpHandler.class);

	public DirectSMSMessageHttpHandler(HttpInterface hie) {
		super(hie);
	}

	/**
	 * parse submit response
	 */
	public void parseResponse(GmmsMessage msg, String resp) {

		String jobid = "";
		String reason_phrase = null;
		String resp_code = null;
		if (resp == null) {
			log.error("Invlid response format!");
			msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
			return;
		}
		int inde = resp.indexOf(":");
		if (inde < 1) {
			log.error("Invlid response format!");
			msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
			return;
		}
		String[] arr_r = new String[2];
		arr_r[0] = resp.substring(0, inde);
		arr_r[1] = resp.substring(inde + 1, resp.length());
		List<HttpParam> parameters = hi.getMtSubmitResponse().getParamList();
		for (HttpParam param : parameters) {
			String pval = param.getParam();
			String oval = param.getOppsiteParam();
			if(log.isDebugEnabled()){
				log.debug("pval={},oval={}", pval, oval);
			}
			if ("id".equalsIgnoreCase(arr_r[0])) {
				if ("outMsgID".equalsIgnoreCase(pval)) {
					jobid = arr_r[1].trim();
				}
				resp_code = "0";
				msg.setOutMsgID(jobid);
			} else if ("err".equalsIgnoreCase(arr_r[0])) {
				reason_phrase = arr_r[1].trim();
				// log.debug("reson:"+reason_phrase);
			}
		}
		// log.debug("jobid="+jobid+",msg_status_code="+msg_status_code+",reason_phrase="+reason_phrase);

		if(log.isDebugEnabled()){
    		log.debug(msg, "outMsgId={}", msg.getOutMsgID());
		}
		HttpStatus status = new HttpStatus(resp_code, reason_phrase);
		GmmsStatus gs = hi.mapHttpSubStatus2GmmsStatus(status);
		msg.setStatus(gs);
	}
}
