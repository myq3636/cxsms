package com.king.rest.util;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Date;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

public class RESTSDKClient {

	public SmsResponse sendSMS(SmsRequest request, String serverAddress)
			throws RequestException, IOException, ResponseException,
			DocumentException {

		try {
			GmmsStatus statusCode = null;

			if (request == null) {
				throw new RequestException(
						"SmsRequest parameter cann't be null");
			}
			if (!StringUtility.stringIsNotEmpty(serverAddress)) {
				throw new RequestException(
						"serverAddress parameter cann't be null");
			}
			URL url = new URL(serverAddress);
			HttpURLConnection connection = (HttpURLConnection) url
					.openConnection();
			connection.setDoInput(true);
			connection.setDoOutput(true);
			connection.setUseCaches(false);
			connection.setRequestMethod("POST");

			connection.setRequestProperty("Content-Type",
					"application/xml;charset=UTF-8");

			String requestBody = convertSmsRequestToXmlData(request);

			DataOutputStream dos = new DataOutputStream(
					connection.getOutputStream());
			if (requestBody != null) {
				dos.writeBytes(requestBody);
			}
			dos.flush();

			BufferedReader br = null;

			InputStreamReader in = new InputStreamReader(
					connection.getInputStream());
			br = new BufferedReader(in);

			StringBuffer strResponse = new StringBuffer();
			String readLine;
			while ((readLine = br.readLine()) != null) {
				strResponse.append(readLine.trim() + "\r\n");
			}

			int httpstatus = connection.getResponseCode();
			String strStatus = String.valueOf(httpstatus);
			if (strStatus.startsWith("4")) {
				switch (httpstatus) {
				case 401:
					statusCode = (GmmsStatus.AUTHENTICATION_ERROR);
					break;
				case 403:
					statusCode = (GmmsStatus.AUTHENTICATION_ERROR);
					break;
				case 408:
					statusCode = (GmmsStatus.COMMUNICATION_ERROR);
					break;
				default:
					statusCode = (GmmsStatus.CLIENT_ERROR);
					break;
				}
			} else if (strStatus.startsWith("5")) {
				switch (httpstatus) {
				case 504:
					statusCode = (GmmsStatus.COMMUNICATION_ERROR);
					break;
				case 511:
					statusCode = (GmmsStatus.AUTHENTICATION_ERROR);
					break;
				case 598:
					statusCode = (GmmsStatus.COMMUNICATION_ERROR);
					break;
				case 599:
					statusCode = (GmmsStatus.COMMUNICATION_ERROR);
					break;
				default:
					statusCode = (GmmsStatus.SERVER_ERROR);
					break;
				}
			} else if (strStatus.startsWith("2")) {
				if (httpstatus >= 200 && httpstatus < 300) {
					statusCode = (GmmsStatus.SUCCESS);
				} else {
					statusCode = (GmmsStatus.UNKNOWN_ERROR);
				}
			} else {
				statusCode = (GmmsStatus.UNKNOWN_ERROR);
			}
			if (statusCode == GmmsStatus.SUCCESS) {

				String str = strResponse.toString();
				SmsResponse resp = this.parseXmlToSmsResponse(str);
				return resp;
			} else {
				SmsResponse resp = new SmsResponse();
				resp.setStatusCode(String.valueOf(statusCode.getCode()));
				resp.setStatueText(statusCode.getText());
				return resp;
			}

		} catch (IOException e) {
			throw e;
		}

	}

	public DRResponse queryDeliveryReport(DRRequest request,
			String serverAddress) throws ResponseException, RequestException,
			IOException, DocumentException {

		try {

			GmmsStatus statusCode = null;

			if (request == null) {
				throw new RequestException("DRRequest parameter cann't be null");
			}

			if (!StringUtility.stringIsNotEmpty(serverAddress)) {
				throw new RequestException(
						"serverAddress parameter cann't be null");
			}

			String[] vpArray = this.convertDRRequestToNameValuePair(request);

			URL url = new URL(serverAddress + "?" + vpArray[0].toString() + "&"
					+ vpArray[1].toString() + "&" + vpArray[2].toString() + "&"
					+ vpArray[3].toString());

			System.out.println("url:" + url);

			HttpURLConnection connection = (HttpURLConnection) url
					.openConnection();
			connection.setDoInput(true);
			connection.setDoOutput(true);
			connection.setUseCaches(false);
			connection.setRequestMethod("GET");
			connection.setRequestProperty("Content-Type",
					"application/xml;charset=UTF-8");

			InputStream in = connection.getInputStream();
			BufferedReader data = new BufferedReader(new InputStreamReader(in));
			StringBuffer respones = new StringBuffer();
			String line = data.readLine();

			while (line != null) {
				respones.append(line + "\r\n");
				line = data.readLine();
			}
			data.close();

			int httpstatus = connection.getResponseCode();
			String strStatus = String.valueOf(httpstatus);
			if (strStatus.startsWith("4")) {
				switch (httpstatus) {
				case 401:
					statusCode = (GmmsStatus.AUTHENTICATION_ERROR);
					break;
				case 403:
					statusCode = (GmmsStatus.AUTHENTICATION_ERROR);
					break;
				case 408:
					statusCode = (GmmsStatus.COMMUNICATION_ERROR);
					break;
				default:
					statusCode = (GmmsStatus.CLIENT_ERROR);
					break;
				}
			} else if (strStatus.startsWith("5")) {
				switch (httpstatus) {
				case 504:
					statusCode = (GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT);
					break;
				case 511:
					statusCode = (GmmsStatus.REJECTED);
					break;
				case 598:
					statusCode = (GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT);
					break;
				case 599:
					statusCode = (GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT);
					break;
				default:
					statusCode = (GmmsStatus.UNKNOWN);
					break;
				}
			} else if (strStatus.startsWith("2")) {
				if (httpstatus >= 200 && httpstatus < 300) {
					statusCode = (GmmsStatus.SUCCESS);
				} else {
					statusCode = (GmmsStatus.UNKNOWN_ERROR);
				}
			} else {
				statusCode = (GmmsStatus.UNKNOWN_ERROR);
			}

			if (statusCode == GmmsStatus.SUCCESS) {

				String str = respones.toString();
				DRResponse resp = this.parseXmlToDRResponse(str);
				return resp;
			} else {
				DRResponse resp = new DRResponse();
				resp.setStatusCode(String.valueOf(statusCode.getCode()));
				resp.setStatusText(statusCode.getText());
				return resp;
			}

		} catch (IOException e) {
			throw e;

		} catch (DocumentException e) {
			throw e;
		}

	}

	public static void main(String[] args) throws RequestException,
			IOException, ResponseException, DocumentException {
		

	}

	private SmsResponse parseXmlToSmsResponse(String xmlStr)
			throws ResponseException, DocumentException {
		try {
			if (StringUtility.stringIsNotEmpty(xmlStr)) {

				Document doc = DocumentHelper.parseText(xmlStr);
				Element rootElt = doc.getRootElement();
				if (rootElt.getName() != null
						&& "SMSResponse".equalsIgnoreCase(rootElt.getName()
								.trim())) {
					SmsResponse response = new SmsResponse();
					Element msgIdElt = rootElt.element("MessageID");
					if (msgIdElt != null) {
						response.setMessageId(msgIdElt.getTextTrim());
					}
					Element statusCodeElt = rootElt.element("Statuscode");
					if (statusCodeElt != null) {
						response.setStatusCode(statusCodeElt.getTextTrim());
					}
					Element statusTextElt = rootElt.element("Statustext");
					if (statusTextElt != null) {
						response.setStatueText(statusTextElt.getTextTrim());
					}
					return response;
				} else {
					throw new ResponseException(
							"Send SMS Response is not a correct format!");
				}

			} else {
				throw new ResponseException(
						"Send SMS Response Content is null!");
			}
		} catch (DocumentException e) {

			throw e;
		}

	}

	private DRResponse parseXmlToDRResponse(String xmlStr)
			throws ResponseException, DocumentException {
		try {

			if (StringUtility.stringIsNotEmpty(xmlStr)) {

				Document doc = DocumentHelper.parseText(xmlStr);
				Element rootElt = doc.getRootElement();
				if (rootElt.getName() != null
						&& "deliveryReport".equalsIgnoreCase(rootElt.getName()
								.trim())) {
					DRResponse response = new DRResponse();
					Element msgIdElt = rootElt.element("MessageID");
					if (msgIdElt != null) {
						response.setMessageId(msgIdElt.getTextTrim());
					}
					Element statusCodeElt = rootElt.element("Statuscode");
					if (statusCodeElt != null) {
						response.setStatusCode(statusCodeElt.getTextTrim());
					}
					Element statusTextElt = rootElt.element("Statustext");
					if (statusTextElt != null) {
						response.setStatusText(statusTextElt.getTextTrim());
					}
					return response;
				} else {
					throw new ResponseException(
							"Query Delivery Response is not a correct format!");
				}

			} else {
				throw new ResponseException("Query Delivery Response  is null!");
			}
		} catch (DocumentException e) {

			throw e;
		}

	}

	private String convertSmsRequestToXmlData(SmsRequest request)
			throws RequestException {
		if (request == null) {
			throw new RequestException("SmsRequest parameter cann't be null");
		} else {
			Document doc = DocumentHelper.createDocument();
			Element rootElmt = doc.addElement("SMSRequest");
			String username = request.getUsername();
			if (StringUtility.stringIsNotEmpty(username)) {
				Element userNameElmt = rootElmt.addElement("UserName");
				userNameElmt.setText(username);

			} else {
				throw new RequestException("UserName cann't be null");
			}

			String recipient = request.getRecipient();
			if (!StringUtility.stringIsNotEmpty(recipient)) {
				throw new RequestException("Recipient cann't be null");
			}

			String password = request.getPassword();
			if (StringUtility.stringIsNotEmpty(password)) {

				String md5 = username + ":" + password + ":" + recipient;
				String md5Pwd = HttpUtils.md5Encrypt(md5);

				Element passwordElmt = rootElmt.addElement("Password");
				passwordElmt.setText(md5Pwd);
			} else {
				throw new RequestException("Password cann't be null");
			}

			Element subRootElt = rootElmt.addElement("SMSMessage");
			String sender = request.getSender();
			if (StringUtility.stringIsNotEmpty(sender)) {
				Element senderElmt = subRootElt.addElement("Sender");
				senderElmt.setText(sender);
			} else {
				throw new RequestException("Sender cann't be null");
			}

			Element recipientElmt = subRootElt.addElement("Recipient");
			recipientElmt.setText(recipient);

			String charset = request.getCharset();
			if (StringUtility.stringIsNotEmpty(charset)) {
				Element charsetElmt = subRootElt.addElement("Charset");
				charsetElmt.setText(charset);
			} else {
				throw new RequestException("Charset cann't be null");
			}

			StringBuffer contentStr = new StringBuffer();
			byte[] binaryContent = request.getBinaryContent();
			String content = null;
			if (charset.equalsIgnoreCase("binary") && binaryContent != null
					&& binaryContent.length > 0) { // text/binary
				for (int i = 0; i < binaryContent.length; i++) {
					contentStr.append(HttpUtils.format2Digits(Integer
							.toHexString(binaryContent[i])));
				}
				content = contentStr.toString();

			} else { // text
				try {
					content = java.net.URLEncoder.encode(request.getContent(),
							charset);
				} catch (UnsupportedEncodingException e) {
					throw new RequestException(e.getMessage());
				}
			}
			Element contentElmt = null;
			if (StringUtility.stringIsNotEmpty(content)) {
				contentElmt = subRootElt.addElement("Content");
				contentElmt.setText(content);
			} else {
				throw new RequestException("Content cann't be null");
			}

			Date expiredDate = request.getExpiredDate();
			String expiredDateStr = StringUtility.formatDate(expiredDate);
			if (StringUtility.stringIsNotEmpty(expiredDateStr)) {
				Element expiredDateElmt = subRootElt.addElement("ExpiryDate");
				expiredDateElmt.setText(expiredDateStr);
			} 
//			else {
//				throw new RequestException("ExpireDate cann't be null");
//			}

			int deliveryReport = request.getDeliveryReport();

			if (deliveryReport == 0 || deliveryReport == 1) {
				Element deliveryReportElmt = subRootElt
						.addElement("DeliveryReport");
				deliveryReportElmt.setText(String.valueOf(deliveryReport));
			} else {
				throw new RequestException(
						"DeliveryReport value must be 0 or 1");
			}

			Date timeStemp = request.getTimestamp();
			String timeStempStr = StringUtility.formatDate(timeStemp);
			if (StringUtility.stringIsNotEmpty(timeStempStr)) {
				Element timeStampElmt = subRootElt.addElement("Timestamp");
				timeStampElmt.setText(timeStempStr);
			}
//			else {
//				throw new RequestException("TimeStemp cann't be null");
//			}

			byte[] udh = request.getUdh();
			StringBuffer udhStr = new StringBuffer();
			if (udh != null && udh.length > 0) { // has UDH
				for (byte one : udh) {
					udhStr.append(HttpUtils.format2Digits(Integer
							.toHexString(one)));
				}
				Element udhElmt = subRootElt.addElement("UDH");
				udhElmt.setText(udhStr.toString());
			}
			
			int serviceTypeID = request.getServiceTypeID();

			if (serviceTypeID != 0) {
				Element serviceTypeIDElmt = subRootElt
						.addElement("ServiceTypeID");
				serviceTypeIDElmt.setText(String.valueOf(serviceTypeID));
			} 
			
			Date scheduleDeliveryTime  = request.getScheduleDeliveryTime();
			String scheduleDeliveryTimeStr  = StringUtility.formatDate(scheduleDeliveryTime );
			if (StringUtility.stringIsNotEmpty(scheduleDeliveryTimeStr)) {
				Element scheduleDeliveryTimeElmt = subRootElt.addElement("ScheduleDeliveryTime");
				scheduleDeliveryTimeElmt.setText(scheduleDeliveryTimeStr);
			} 

			return doc.asXML();

		}
	}

	private String[] convertDRRequestToNameValuePair(DRRequest request)
			throws RequestException {
		if (request == null) {
			throw new RequestException("DRRequest parameter cann't be null");
		} else {

			String[] nvpArray = new String[4];

			String username = request.getUsername();
			if (StringUtility.stringIsNotEmpty(username)) {
				String nvp = "UserName" + "=" + username;
				nvpArray[0] = nvp;

			} else {
				throw new RequestException("Username parameter cann't be null");
			}

			String recipient = request.getRecipient();
			if (StringUtility.stringIsNotEmpty(recipient)) {
				String nvp = "Recipient" + "=" + recipient;
				nvpArray[1] = nvp;
			} else {
				throw new RequestException("Recipient parameter cann't be null");
			}

			String password = request.getPassword();
			if (StringUtility.stringIsNotEmpty(password)) {
				String md5 = username + ":" + password + ":" + recipient;
				String md5Pwd = HttpUtils.md5Encrypt(md5);
				String nvp = "Password" + "=" + md5Pwd;
				nvpArray[2] = nvp;
			} else {
				throw new RequestException("Password parameter cann't be null");
			}
			String messageId = request.getMessageID();
			if (StringUtility.stringIsNotEmpty(messageId)) {
				String nvp = "MessageID" + "=" + messageId;
				nvpArray[3] = nvp;
			} else {
				throw new RequestException("MessageId parameter cann't be null");
			}

			return nvpArray;

		}

	}

}
