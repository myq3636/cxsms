package it;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.protocol.commonhttp.HttpHandler;
import com.king.gmms.protocol.commonhttp.HttpStatus;
import com.king.message.gmms.GmmsMessage;

/** Builds the submit request of the ClientHttpIT channel, as the vendor handlers of protocol/commonhttp do. */
public class ItHttpHandler extends HttpHandler {
	public ItHttpHandler(HttpInterface hie) { super(hie); }
	public String makeRequest(GmmsMessage m, String enc, A2PCustomerInfo cst) {
		return "from=" + m.getSenderAddress() + "&to=" + m.getRecipientAddress() + "&text=" + m.getTextContent() + "&ref=" + m.getMsgID();
	}
	public HttpStatus parseRequest(GmmsMessage m, HttpServletRequest r) throws ServletException, IOException { return null; }
	public String makeResponse(HttpStatus hs, GmmsMessage m, A2PCustomerInfo cst) { return ""; }
	public void parseResponse(GmmsMessage m, String resp) { }
}
