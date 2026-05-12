package com.king.redis;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URL;
import java.text.SimpleDateFormat;

import com.king.framework.SystemLogger;
import com.king.gmms.ha.ModuleURI;
import com.king.gmms.ha.TransactionURI;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.rest.util.StringUtility;
import com.sun.org.apache.bcel.internal.generic.NEW;
import com.sun.tools.internal.xjc.reader.xmlschema.bindinfo.BIConversion.Static;

import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

public class SerializableHandler {
	
	private static SystemLogger log = SystemLogger.getSystemLogger(SerializableHandler.class);
	
	private static char[] HEX_ENCODE_TALBE = new char[] { '0', '1', '2', '3',
			'4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };
	private static byte[] HEX_DECODE_TABLE = new byte[255];
	private final static String separator = "`";
	private final static String repl_sep = "@,<";
	
	static {
		HEX_DECODE_TABLE['0'] = 0;
		HEX_DECODE_TABLE['1'] = 1;
		HEX_DECODE_TABLE['2'] = 2;
		HEX_DECODE_TABLE['3'] = 3;
		HEX_DECODE_TABLE['4'] = 4;
		HEX_DECODE_TABLE['5'] = 5;
		HEX_DECODE_TABLE['6'] = 6;
		HEX_DECODE_TABLE['7'] = 7;
		HEX_DECODE_TABLE['8'] = 8;
		HEX_DECODE_TABLE['9'] = 9;
		HEX_DECODE_TABLE['A'] = 10;
		HEX_DECODE_TABLE['B'] = 11;
		HEX_DECODE_TABLE['C'] = 12;
		HEX_DECODE_TABLE['D'] = 13;
		HEX_DECODE_TABLE['E'] = 14;
		HEX_DECODE_TABLE['F'] = 15;
		HEX_DECODE_TABLE['a'] = 10;
		HEX_DECODE_TABLE['b'] = 11;
		HEX_DECODE_TABLE['c'] = 12;
		HEX_DECODE_TABLE['d'] = 13;
		HEX_DECODE_TABLE['e'] = 14;
		HEX_DECODE_TABLE['f'] = 15;
	}

	public static Object stringToObject(String s) throws Exception {
		if(s == null || "".equals(s.trim())) {
			return null;
		}
		// Refactored in Phase 1: Pure FastJSON deserialization (No backward compatibility)
		return com.alibaba.fastjson.JSON.parse(s);
	}

	public static String objectToString(Object obj) throws Exception {
		if(obj==null){
			return null;
		}
		// Refactored in Phase 1: FastJSON serialization
		return com.alibaba.fastjson.JSON.toJSONString(obj, com.alibaba.fastjson.serializer.SerializerFeature.WriteClassName);
	}

	public static java.util.Map<String, String> convertGmmsMessageToStreamHash(GmmsMessage msg) throws Exception {
		java.util.Map<String, String> hash = new java.util.HashMap<>();
		if (msg == null) {
			return hash;
		}
		hash.put("MsgID", msg.getMsgID() != null ? msg.getMsgID() : "");
		hash.put("DestAddr", msg.getRecipientAddress() != null ? msg.getRecipientAddress() : "");
		hash.put("SSID", msg.getRoutingSsIDs() != null ? msg.getRoutingSsIDs() : (msg.getOSsID() != 0 ? String.valueOf(msg.getOSsID()) : ""));
		hash.put("data", objectToString(msg));
		return hash;
	}

	public static byte[] parseByteArray(String s) {
		char[] chars = s.toCharArray();
		byte[] result = new byte[chars.length / 2];
		for (int i = 0, j = 0; i < result.length; i++, j += 2) {
			int high = HEX_DECODE_TABLE[chars[j]];
			int low = HEX_DECODE_TABLE[chars[j + 1]];
			result[i] = (byte) (0xff & ((high << 4) + low));
		}
		return result;
	}

	public static String formatByteArray(byte[] bytes) {
		StringBuffer result = new StringBuffer();
		for (int i = 0; i < bytes.length; i++) {
			result.append(HEX_ENCODE_TALBE[(bytes[i] & 0xf0) >>> 4]);
			result.append(HEX_ENCODE_TALBE[bytes[i] & 0x0f]);
		}
		return result.toString();
	}
	
	
		
		  public static GmmsMessage convertRedisMssage2GmmsMessage(String message){
		    	GmmsMessage msg = null;
		    	
		    	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		    	
		    	if(message!=null){
		    		try {
		    			String [] parameters = message.split(separator);
		    			if(parameters == null || parameters.length < 41){
		    				if(log.isInfoEnabled()){
								log.info("Failed to convert Redis String to GmmsMessage, since the number of parameter({}) is incorrect, and Redis String is {}",parameters.length,message);
							}
		    				return null;
		    			}
			    		msg = new GmmsMessage();
			    		for(int i = 0 ; i < parameters.length ; i++){
			    			if(parameters[i] == null || "null".equalsIgnoreCase(parameters[i])
			    					||"".equalsIgnoreCase(parameters[i])||parameters[i].length() <= 0){
			    				continue;
			    			}
			    			switch(i){
				    			case 0:
				    				msg.setInTransID(parameters[0]);
				    				break;
				    			case 1:
				    				msg.setOutTransID(parameters[1]);
				    				break;
				    			case 2:
				    				if(parameters[2].contains(repl_sep)){
			            				msg.setMsgID(parameters[2].replaceAll(repl_sep, separator));           				
			            			}else{
			            				msg.setMsgID(parameters[2]);
			            			}   
				    				break;
				    			case 3:
				    				if(parameters[3].contains(repl_sep)){
			            				msg.setInMsgID(parameters[3].replaceAll(repl_sep, separator));           				
			            			}else{
			            				msg.setInMsgID(parameters[3]);
			            			}   
				    				break;
				    			case 4:
				    				msg.setOutMsgID(parameters[4]);
				    				break;
				    			case 5:
				    				msg.setOoperator(Integer.parseInt(parameters[5]));
				    				break;
				    			case 6:
				    				msg.setRoperator(Integer.parseInt(parameters[6]));
				    				break;
				    			case 7:
				    				msg.setOSsID(Integer.parseInt(parameters[7]));
				    				break;
				    			case 8:
				    				msg.setRSsID(Integer.parseInt(parameters[8]));
				    				break;
				    			case 9:
				    				msg.setGmmsMsgType(parameters[9]);
				    				break;
				    			case 10:
				    				msg.setMessageSize(Integer.parseInt(parameters[10]));
				    				break;
				    			case 11:
				    				msg.setProtocolVersion(parameters[11]);
				    				break;
				    			case 12:
				    				if(parameters[12].contains(repl_sep)){
			            				msg.setSenderAddress(parameters[12].replaceAll(repl_sep, separator));           				
			            			}else{
			            				msg.setSenderAddress(parameters[12]);
			            			}     
				    				break;
				    			case 13:
				    				if(parameters[13].contains(repl_sep)){
			            				msg.setRecipientAddress(parameters[13].replaceAll(repl_sep, separator));           				
			            			}else{
			            				msg.setRecipientAddress(parameters[13]);
			            			}     
				    				break;
				    			case 14:
				    				msg.setOriginalSenderAddr(parameters[14]);
				    				break;
				    			case 15:
				    				msg.setOriginalRecipientAddr(parameters[15]);
				    				break;
				    			case 16:
				    				msg.setTimeStamp(dateFormat.parse(parameters[16]));
				    				break;
				    			case 17:
				    				msg.setExpiryDate(dateFormat.parse(parameters[17]));
				    				break;
				    			case 18:
				    				msg.setDeliveryReport("true".equalsIgnoreCase(parameters[18])?true:false);
				    				break;
				    				/*case 19:
					    			msg.setRetriedNumber(Integer.parseInt(parameters[19]));
				    				break;
					    		case 20:
					    			msg.setNextRetryTime(dateFormat.parse(parameters[20]));
				    				break;
						    	case 21:
						    		msg.setOMncMcc(parameters[21]);
									break;	
						    	case 22:
									msg.setRMncMcc(parameters[22]);
									break;*/
						    	case 19:
						    		msg.setSarMsgRefNum(parameters[19]);	
									break;
						    	case 20:
						    		msg.setSarTotalSegments(Integer.parseInt(parameters[20]));
									break;
						    	case 21:
						    		msg.setSarSegmentSeqNum(Integer.parseInt(parameters[21]));
									break;
						    	case 22:
						    		msg.setInCsm("true".equalsIgnoreCase(parameters[22])?true:false);
									break;
						    	case 23:
						    		msg.setPriority(Integer.parseInt(parameters[23]));
									break;
						    	case 24:
					    			if(parameters[24].contains(repl_sep)){
			            				msg.setContentType(parameters[24].replaceAll(repl_sep, separator));           				
			            			}else{
			            				msg.setContentType(parameters[24]);
			            			}  
									break;
						    	case 25:
				    				if(parameters[25].contains(repl_sep)){
			            				msg.setTextContent(parameters[25].replaceAll(repl_sep, separator));           				
			            			}else{
			            				msg.setTextContent(parameters[25]); 
			            			}	
									break;
						    	case 26:
						    		msg.setStatusCode(Integer.parseInt(parameters[26]));
									break;
						    	/*case 31:
						    		msg.setStatusText(parameters[31]);	
									break;*/
						    	case 27:
						    		String temp = parameters[27];
						    		if(temp == null || temp.equalsIgnoreCase("")){
						    			msg.setInClientPull(0);
						    		}else if ("true".equalsIgnoreCase(temp)|| "1".equalsIgnoreCase(temp)) {
										msg.setInClientPull(1);
									}else if("false".equalsIgnoreCase(temp)||"0".equalsIgnoreCase(temp)){
										msg.setInClientPull(0);
									}else if("2".equalsIgnoreCase(temp)) {
										msg.setInClientPull(2);
									}
									break;
						    	case 28:
						    		msg.setOutClientPull("true".equalsIgnoreCase(parameters[28])?true:false);
									break;						    	
						    	case 29:
						    		msg.setSenderAddrType(parameters[29]);
									break;
						    	case 30:
						    		msg.setRecipientAddrType(parameters[30]);
									break;
						    	case 31:
						    		msg.setMilterActionCode(Integer.parseInt(parameters[31]));
									break;
						    	case 32:
						    		msg.setRA2P(Integer.parseInt(parameters[32]));
									break;
						    	case 33:
						    		msg.setOA2P(Integer.parseInt(parameters[33]));
									break;
						    	case 34:
						    		msg.setCurrentA2P(Integer.parseInt(parameters[34]));
									break;
						    	case 35:
						    		msg.setActionCode(Integer.parseInt(parameters[35]));
									break;
						    	case 36:
						    		msg.setSplitStatus(Integer.parseInt(parameters[36]));
									break;
						    	case 37:
						    		msg.setConnectionID(parameters[37]);
									break;
						    	case 38:
						    		msg.setOperatorPriority(Integer.parseInt(parameters[38]));
									break;
						    	/*case 45:
						    		msg.setSpecialDataCodingScheme(parameters[45]);
									break;*/
						    	case 39:
						    		msg.setDateIn(dateFormat.parse(parameters[39]));
									break;
						    	case 40:
						    		if(GmmsMessage.AIC_MSG_TYPE_BINARY.equalsIgnoreCase(msg.getGmmsMsgType())){
			                    		BASE64Decoder base4=new BASE64Decoder();
			                    		msg.setMimeMultiPartData(base4.decodeBuffer(parameters[40]));
			                    	}
									break;
						    	case 41:
						    		msg.setMessageType(parameters[41]);
									break;
						    	case 42:
						    		msg.setSenderAddrTon(parameters[42]);
									break;
						    	case 43:
						    		msg.setRecipientAddrTon(parameters[43]);
									break;
						    	case 44:
						    		msg.setStatusText(parameters[44]);
						    		break;
						    	case 45:
						    		msg.setServiceTypeID(Integer.parseInt(parameters[45]));
									break;
						    	case 46:
				    				msg.setScheduleDeliveryTime(dateFormat.parse(parameters[46]));
				    				break;
						    	case 47:
				    				String inTranURL = parameters[47];
				    				if (inTranURL!=null) {
										TransactionURI uri = TransactionURI.fromString(inTranURL);
										msg.setInnerTransaction(uri);
									}
				    				break;
						    	case 48:
				    				if(parameters[48].contains(repl_sep)){
			            				msg.setInTextContent(parameters[48].replaceAll(repl_sep, separator));           				
			            			}else{
			            				msg.setInTextContent(parameters[48]); 
			            			}	
									break;
						    	case 49:
				    				String inContentType = parameters[49];
				    				if (inContentType!=null) {										
										msg.setInContentType(inContentType);
									}
				    				break;
						    	case 50:
				    				String routingSsIDs = parameters[50];
				    				if (routingSsIDs!=null) {										
										msg.setRoutingSsIDs(routingSsIDs);
									}
				    				break;
						    	case 51:
				    				String mncmcc = parameters[51];
				    				if (mncmcc!=null) {										
										msg.setRMncMcc(mncmcc);
									}
				    				break;
								default:
									if(log.isInfoEnabled()){
										log.info("Unknown parameter in Redis String, and Redis String is {}", message);
									}
									break;
			    			}
			    		}
			    		if(GmmsStatus.UNASSIGNED.getText().equalsIgnoreCase(msg.getStatusText())){
			    			msg.setStatus(GmmsStatus.getStatus(msg.getStatusCode()));
			    		}
					} catch (Exception e) {
						if(log.isInfoEnabled()){
							log.info("Failed to convert Redis String to GmmsMessage, Redis String is {}, and exception is {}",message,e.getStackTrace());
						}
						return null;
					}
		    	}
		    	return msg;
		    	
		    }
		  
		  public static GmmsMessage convertRedisMssage2GmmsMessageForCorePrioritry(String message){
		    	GmmsMessage msg = null;
		    	
		    	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		    	
		    	if(message!=null){
		    		try {
		    			String [] parameters = message.split(separator);
		    			if(parameters == null || parameters.length < 41){
		    				if(log.isInfoEnabled()){
								log.info("Failed to convert Redis String to GmmsMessage, since the number of parameter({}) is incorrect, and Redis String is {}",parameters.length,message);
							}
		    				return null;
		    			}
			    		msg = new GmmsMessage();
			    		for(int i = 0 ; i < parameters.length ; i++){
			    			if(parameters[i] == null || "null".equalsIgnoreCase(parameters[i])
			    					||"".equalsIgnoreCase(parameters[i])||parameters[i].length() <= 0){
			    				continue;
			    			}
			    			switch(i){
				    			case 0:
				    				msg.setInTransID(parameters[0]);
				    				break;
				    			case 1:
				    				msg.setOutTransID(parameters[1]);
				    				break;
				    			case 2:
				    				if(parameters[2].contains(repl_sep)){
			            				msg.setMsgID(parameters[2].replaceAll(repl_sep, separator));           				
			            			}else{
			            				msg.setMsgID(parameters[2]);
			            			}   
				    				break;
				    			case 3:
				    				if(parameters[3].contains(repl_sep)){
			            				msg.setInMsgID(parameters[3].replaceAll(repl_sep, separator));           				
			            			}else{
			            				msg.setInMsgID(parameters[3]);
			            			}   
				    				break;
				    			case 4:
				    				msg.setOutMsgID(parameters[4]);
				    				break;
				    			case 5:
				    				msg.setOoperator(Integer.parseInt(parameters[5]));
				    				break;
				    			case 6:
				    				msg.setRoperator(Integer.parseInt(parameters[6]));
				    				break;
				    			case 7:
				    				msg.setOSsID(Integer.parseInt(parameters[7]));
				    				break;
				    			case 8:
				    				msg.setRSsID(Integer.parseInt(parameters[8]));
				    				break;
				    			case 9:
				    				msg.setGmmsMsgType(parameters[9]);
				    				break;
				    			case 10:
				    				msg.setMessageSize(Integer.parseInt(parameters[10]));
				    				break;
				    			case 11:
				    				msg.setProtocolVersion(parameters[11]);
				    				break;
				    			case 12:
				    				if(parameters[12].contains(repl_sep)){
			            				msg.setSenderAddress(parameters[12].replaceAll(repl_sep, separator));           				
			            			}else{
			            				msg.setSenderAddress(parameters[12]);
			            			}     
				    				break;
				    			case 13:
				    				if(parameters[13].contains(repl_sep)){
			            				msg.setRecipientAddress(parameters[13].replaceAll(repl_sep, separator));           				
			            			}else{
			            				msg.setRecipientAddress(parameters[13]);
			            			}     
				    				break;
				    			case 14:
				    				msg.setOriginalSenderAddr(parameters[14]);
				    				break;
				    			case 15:
				    				msg.setOriginalRecipientAddr(parameters[15]);
				    				break;
				    			case 16:
				    				msg.setTimeStamp(dateFormat.parse(parameters[16]));
				    				break;
				    			case 17:
				    				msg.setExpiryDate(dateFormat.parse(parameters[17]));
				    				break;
				    			case 18:
				    				msg.setDeliveryReport("true".equalsIgnoreCase(parameters[18])?true:false);
				    				break;
				    				/*case 19:
					    			msg.setRetriedNumber(Integer.parseInt(parameters[19]));
				    				break;
					    		case 20:
					    			msg.setNextRetryTime(dateFormat.parse(parameters[20]));
				    				break;
						    	case 21:
						    		msg.setOMncMcc(parameters[21]);
									break;	
						    	case 22:
									msg.setRMncMcc(parameters[22]);
									break;*/
						    	case 19:
						    		msg.setSarMsgRefNum(parameters[19]);	
									break;
						    	case 20:
						    		msg.setSarTotalSegments(Integer.parseInt(parameters[20]));
									break;
						    	case 21:
						    		msg.setSarSegmentSeqNum(Integer.parseInt(parameters[21]));
									break;
						    	case 22:
						    		msg.setInCsm("true".equalsIgnoreCase(parameters[22])?true:false);
									break;
						    	case 23:
						    		msg.setPriority(Integer.parseInt(parameters[23]));
									break;
						    	case 24:
					    			if(parameters[24].contains(repl_sep)){
			            				msg.setContentType(parameters[24].replaceAll(repl_sep, separator));           				
			            			}else{
			            				msg.setContentType(parameters[24]);
			            			}  
									break;
						    	case 25:
				    				if(parameters[25].contains(repl_sep)){
			            				msg.setTextContent(parameters[25].replaceAll(repl_sep, separator));           				
			            			}else{
			            				msg.setTextContent(parameters[25]); 
			            			}	
									break;
						    	case 26:
						    		msg.setStatusCode(Integer.parseInt(parameters[26]));
									break;
						    	/*case 31:
						    		msg.setStatusText(parameters[31]);	
									break;*/
						    	case 27:
						    		String temp = parameters[27];
						    		if(temp == null || temp.equalsIgnoreCase("")){
						    			msg.setInClientPull(0);
						    		}else if ("true".equalsIgnoreCase(temp)|| "1".equalsIgnoreCase(temp)) {
										msg.setInClientPull(1);
									}else if("false".equalsIgnoreCase(temp)||"0".equalsIgnoreCase(temp)){
										msg.setInClientPull(0);
									}else if("2".equalsIgnoreCase(temp)) {
										msg.setInClientPull(2);
									}
									break;
						    	case 28:
						    		msg.setOutClientPull("true".equalsIgnoreCase(parameters[28])?true:false);
									break;						    	
						    	case 29:
						    		msg.setSenderAddrType(parameters[29]);
									break;
						    	case 30:
						    		msg.setRecipientAddrType(parameters[30]);
									break;
						    	case 31:
						    		msg.setMilterActionCode(Integer.parseInt(parameters[31]));
									break;
						    	case 32:
						    		msg.setRA2P(Integer.parseInt(parameters[32]));
									break;
						    	case 33:
						    		msg.setOA2P(Integer.parseInt(parameters[33]));
									break;
						    	case 34:
						    		msg.setCurrentA2P(Integer.parseInt(parameters[34]));
									break;
						    	case 35:
						    		msg.setActionCode(Integer.parseInt(parameters[35]));
									break;
						    	case 36:
						    		msg.setSplitStatus(Integer.parseInt(parameters[36]));
									break;
						    	case 37:
						    		msg.setConnectionID(parameters[37]);
									break;
						    	case 38:
						    		msg.setOperatorPriority(Integer.parseInt(parameters[38]));
									break;
						    	/*case 45:
						    		msg.setSpecialDataCodingScheme(parameters[45]);
									break;*/
						    	case 39:
						    		msg.setDateIn(dateFormat.parse(parameters[39]));
									break;
						    	case 40:
						    		if(GmmsMessage.AIC_MSG_TYPE_BINARY.equalsIgnoreCase(msg.getGmmsMsgType())){
			                    		BASE64Decoder base4=new BASE64Decoder();
			                    		msg.setMimeMultiPartData(base4.decodeBuffer(parameters[40]));
			                    	}
									break;
						    	case 41:
						    		msg.setMessageType(parameters[41]);
									break;
						    	case 42:
						    		msg.setSenderAddrTon(parameters[42]);
									break;
						    	case 43:
						    		msg.setRecipientAddrTon(parameters[43]);
									break;
						    	case 44:
						    		msg.setStatusText(parameters[44]);
						    		break;
						    	case 45:
						    		msg.setServiceTypeID(Integer.parseInt(parameters[45]));
									break;
						    	case 46:
				    				msg.setScheduleDeliveryTime(dateFormat.parse(parameters[46]));
				    				break;				    				
						    	case 47:
				    				String inTranURL = parameters[47];
				    				if (inTranURL!=null) {
										TransactionURI uri = TransactionURI.fromString(inTranURL);
										msg.setInnerTransaction(uri);
									}
				    				break;
				    			case 48:				    				
					    			msg.setRetriedNumber(StringUtility.stringIsNotEmpty(parameters[48])?Integer.parseInt(parameters[48]):0);
				    				break;
					    		case 49:					    			
					    			if (StringUtility.stringIsNotEmpty(parameters[49])) {
					    				msg.setNextRetryTime(dateFormat.parse(parameters[49]));
									}					    		
				    				break;
					    		case 50:
					    			msg.setDeliveryChannel(parameters[50]);					    		
				    				break;
					    		case 51:					    			
					    			if (StringUtility.stringIsNotEmpty(parameters[51])) {
					    				msg.setUdh(SerializableHandler.hexStringToBytes(parameters[51]));				    			
									}					    		
				    				break;
					    		case 52:					    			
					    			if(StringUtility.stringIsNotEmpty(parameters[52]) && parameters[52].contains(repl_sep)){
			            				msg.setInTextContent(parameters[52].replaceAll(repl_sep, separator));           				
			            			}else{
			            				msg.setInTextContent(parameters[52]); 
			            			}					    		
				    				break;
					    		case 53:
				    				String inContentType = parameters[53];
				    				if (inContentType!=null) {										
										msg.setInContentType(inContentType);
									}
				    				break;
					    		case 54:
				    				String routingSsIDs = parameters[54];
				    				if (routingSsIDs!=null) {										
										msg.setRoutingSsIDs(routingSsIDs);
									}
				    				break;
					    		case 55:
				    				String mncmcc = parameters[55];
				    				if (mncmcc!=null) {										
										msg.setRMncMcc(mncmcc);
									}
				    				break;
								default:
									if(log.isInfoEnabled()){
										log.info("Unknown parameter in Redis String, and Redis String is {}", message);
									}
									break;
			    			}
			    		}
			    		if(GmmsStatus.UNASSIGNED.getText().equalsIgnoreCase(msg.getStatusText())){
			    			msg.setStatus(GmmsStatus.getStatus(msg.getStatusCode()));
			    		}			   
					} catch (Exception e) {
						if(log.isInfoEnabled()){
							log.info("Failed to convert Redis String to GmmsMessage, Redis String is {}, and exception is {}",message,e.getStackTrace());
						}
						return null;
					}
		    	}
		    	return msg;
		    	
		    }
		  
		  public static String convertGmmsMessage2RedisMessage(GmmsMessage msg) {
				StringBuilder sb = new StringBuilder(10240);
				
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				
				try {
					sb.append(msg.getInTransID()).append(separator);
					sb.append(msg.getOutTransID()).append(separator);
					sb.append(msg.getMsgID()).append(separator);
					if(msg.getInMsgID()!=null && msg.getInMsgID().contains(separator)){
						sb.append(msg.getInMsgID().replaceAll(separator, repl_sep)).append(separator);
					}else{
						sb.append(msg.getInMsgID()).append(separator);
					}
					if(msg.getOutMsgID()!=null && msg.getOutMsgID().contains(separator)){
						sb.append(msg.getOutMsgID().replaceAll(separator, repl_sep)).append(separator);
					}else{
						sb.append(msg.getOutMsgID()).append(separator);
					}			
					sb.append(msg.getOoperator()).append(separator);
					sb.append(msg.getRoperator()).append(separator);
					sb.append(msg.getOSsID()).append(separator);
					sb.append(msg.getRSsID()).append(separator);
					sb.append(msg.getGmmsMsgType()).append(separator);			
					sb.append(msg.getMessageSize()).append(separator);
					if(msg.getProtocolVersion()!=null){
						sb.append(msg.getProtocolVersion()).append(separator);
					}else{
						sb.append(separator);
					}			
					if(msg.getSenderAddress()!=null && msg.getSenderAddress().contains(separator)){
						sb.append(msg.getSenderAddress().replaceAll(separator, repl_sep)).append(separator);
					}else{
						sb.append(msg.getSenderAddress()).append(separator);
					}
					if(msg.getRecipientAddress()!= null&&msg.getRecipientAddress().contains(separator)){
						sb.append(msg.getRecipientAddress().replaceAll(separator, repl_sep)).append(separator);
					}else{
						sb.append(msg.getRecipientAddress()).append(separator);
					}
					if(msg.getOriginalSenderAddr()== null){
						sb.append(separator);
					}else if(msg.getOriginalSenderAddr().contains(separator)){
						sb.append(msg.getOriginalSenderAddr().replaceAll(separator, repl_sep)).append(separator);
					}else{
						sb.append(msg.getOriginalSenderAddr()).append(separator);
					}
					if(msg.getOriginalRecipientAddr()== null){
						sb.append(separator);
					}else if(msg.getOriginalRecipientAddr().contains(separator)){
						sb.append(msg.getOriginalRecipientAddr().replaceAll(separator, repl_sep)).append(separator);
					} else{
						sb.append(msg.getOriginalRecipientAddr()).append(separator);
					}
					if(msg.getTimeStamp()!=null){
						sb.append(dateFormat.format(msg.getTimeStamp())).append(separator);
					}else{
						sb.append(separator);
					}
					if(msg.getExpiryDate()!=null){
						sb.append(dateFormat.format(msg.getExpiryDate())).append(separator);
					}else{
						sb.append(separator);
					}
					
					sb.append(msg.getDeliveryReport()).append(separator);
//					sb.append(msg.getRetriedNumber()).append(separator);
//					if(msg.getNextRetryTime()!=null){
//						sb.append(dateFormat.format(msg.getNextRetryTime())).append(separator);
//					}else{
//						sb.append(separator);
//					}			
//					sb.append(msg.getOMncMcc()).append(separator);
//					sb.append(msg.getRMncMcc()).append(separator);
					if(msg.getSarMsgRefNum()!=null && msg.getSarMsgRefNum().trim().length()>0){
						sb.append(msg.getSarMsgRefNum()).append(separator);
					}else{
						sb.append(separator);
					}			
					sb.append(msg.getSarTotalSeqments()).append(separator);					
					sb.append(msg.getSarSegmentSeqNum()).append(separator);
					sb.append(msg.isInCsm()).append(separator);
					sb.append(msg.getPriority()).append(separator);
					if(msg.getContentType()!=null && msg.getContentType().contains(separator)){
						sb.append(msg.getContentType().replaceAll(separator, repl_sep)).append(separator);
					}else{
						sb.append(msg.getContentType()).append(separator);
					}
					if(msg.getTextContent()!=null){
						String text = msg.getTextContent();				
						if(text.contains(separator)){
							text = msg.getTextContent().replaceAll(separator, repl_sep);	
						}			
						sb.append(text).append(separator);
					}else{
						sb.append(separator);
					}				
					sb.append(msg.getStatusCode()).append(separator);
//					sb.append(msg.getStatusText()).append(separator);
					
//					sb.append(msg.inClientPull()).append(separator);
					sb.append(msg.getInClientPull()).append(separator);
					
					sb.append(msg.outClientPull()).append(separator);
//					sb.append(msg.getDeliveryChannel()).append(separator);
					sb.append(msg.getSenderAddrType()).append(separator);
					sb.append(msg.getRecipientAddrType()).append(separator);
					sb.append(msg.getMilterActionCode()).append(separator);
					sb.append(msg.getRA2P()).append(separator);
					sb.append(msg.getOA2P()).append(separator);
					sb.append(msg.getCurrentA2P()).append(separator);
					sb.append(msg.getActionCode()).append(separator);
					sb.append(msg.getSplitStatus()).append(separator);
					sb.append(msg.getConnectionID()).append(separator);
					sb.append(msg.getOperatorPriority()).append(separator);
//					sb.append(msg.getSpecialDataCodingScheme()).append(separator);
					if(msg.getDateIn()!=null){
						sb.append(dateFormat.format(msg.getDateIn())).append(separator);
					}else{
						sb.append(separator);
					}
					if(msg.getMimeMultiPartData() != null && msg.getMimeMultiPartData().length > 0
						&& GmmsMessage.AIC_MSG_TYPE_BINARY.equalsIgnoreCase(msg.getGmmsMsgType())){
						BASE64Encoder base64=new BASE64Encoder();
						String text = base64.encode(msg.getMimeMultiPartData());
						sb.append(text).append(separator);
					}else{
						sb.append(separator);
					}
					sb.append(msg.getMessageType()).append(separator);
					sb.append(msg.getSenderAddrTon()).append(separator);
					sb.append(msg.getRecipientAddrTon()).append(separator);
					sb.append(msg.getStatusText());
					sb.append(separator).append(msg.getServiceTypeID());
					
					if(msg.getScheduleDeliveryTime()!=null){
						sb.append(separator).append(dateFormat.format(msg.getScheduleDeliveryTime()));
					}else{
						sb.append(separator);
					}
					if (msg.getInnerTransaction()!=null) {
						sb.append(separator).append(msg.getInnerTransaction().toString());
					}else {
						sb.append(separator);
					}
					
					if(msg.getInTextContent()!=null){
						String text = msg.getInTextContent();				
						if(text.contains(separator)){
							text = msg.getInTextContent().replaceAll(separator, repl_sep);	
						}			
						sb.append(separator).append(text);
					}else{
						sb.append(separator);
					}
					
					if(msg.getInContentType()!=null && msg.getInContentType().contains(separator)){
						sb.append(separator).append(msg.getInContentType().replaceAll(separator, repl_sep));
					}else{
						sb.append(separator).append(msg.getInContentType());
					}
					if (msg.getRoutingSsIDs()!=null) {
						sb.append(separator).append(msg.getRoutingSsIDs().toString());
					}else {
						sb.append(separator);
					}
					if (msg.getRMncMcc()!=null) {
						sb.append(separator).append(msg.getRMncMcc().toString());
					}else {
						sb.append(separator);
					}
					
				} catch (Exception e) {
					if(log.isInfoEnabled()){
						log.info(msg,"Failed to convert message to Redis string, and exception is {}", e.getMessage());
					}
					return null;
				}
				return sb.toString();
			}
		  
		  public static String convertGmmsMessage2RedisMessageForCorePriority(GmmsMessage msg) {
				StringBuilder sb = new StringBuilder(10240);
				
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				
				try {
					sb.append(msg.getInTransID()).append(separator);
					sb.append(msg.getOutTransID()).append(separator);
					sb.append(msg.getMsgID()).append(separator);
					if(msg.getInMsgID()!=null && msg.getInMsgID().contains(separator)){
						sb.append(msg.getInMsgID().replaceAll(separator, repl_sep)).append(separator);
					}else{
						sb.append(msg.getInMsgID()).append(separator);
					}
					if(msg.getOutMsgID()!=null && msg.getOutMsgID().contains(separator)){
						sb.append(msg.getOutMsgID().replaceAll(separator, repl_sep)).append(separator);
					}else{
						sb.append(msg.getOutMsgID()).append(separator);
					}			
					sb.append(msg.getOoperator()).append(separator);
					sb.append(msg.getRoperator()).append(separator);
					sb.append(msg.getOSsID()).append(separator);
					sb.append(msg.getRSsID()).append(separator);
					sb.append(msg.getGmmsMsgType()).append(separator);			
					sb.append(msg.getMessageSize()).append(separator);
					if(msg.getProtocolVersion()!=null){
						sb.append(msg.getProtocolVersion()).append(separator);
					}else{
						sb.append(separator);
					}			
					if(msg.getSenderAddress()!=null && msg.getSenderAddress().contains(separator)){
						sb.append(msg.getSenderAddress().replaceAll(separator, repl_sep)).append(separator);
					}else{
						sb.append(msg.getSenderAddress()).append(separator);
					}
					if(msg.getRecipientAddress()!= null&&msg.getRecipientAddress().contains(separator)){
						sb.append(msg.getRecipientAddress().replaceAll(separator, repl_sep)).append(separator);
					}else{
						sb.append(msg.getRecipientAddress()).append(separator);
					}
					if(msg.getOriginalSenderAddr()== null){
						sb.append(separator);
					}else if(msg.getOriginalSenderAddr().contains(separator)){
						sb.append(msg.getOriginalSenderAddr().replaceAll(separator, repl_sep)).append(separator);
					}else{
						sb.append(msg.getOriginalSenderAddr()).append(separator);
					}
					if(msg.getOriginalRecipientAddr()== null){
						sb.append(separator);
					}else if(msg.getOriginalRecipientAddr().contains(separator)){
						sb.append(msg.getOriginalRecipientAddr().replaceAll(separator, repl_sep)).append(separator);
					} else{
						sb.append(msg.getOriginalRecipientAddr()).append(separator);
					}
					if(msg.getTimeStamp()!=null){
						sb.append(dateFormat.format(msg.getTimeStamp())).append(separator);
					}else{
						sb.append(separator);
					}
					if(msg.getExpiryDate()!=null){
						sb.append(dateFormat.format(msg.getExpiryDate())).append(separator);
					}else{
						sb.append(separator);
					}
					
					sb.append(msg.getDeliveryReport()).append(separator);
//					sb.append(msg.getRetriedNumber()).append(separator);
//					if(msg.getNextRetryTime()!=null){
//						sb.append(dateFormat.format(msg.getNextRetryTime())).append(separator);
//					}else{
//						sb.append(separator);
//					}			
//					sb.append(msg.getOMncMcc()).append(separator);
//					sb.append(msg.getRMncMcc()).append(separator);
					if(msg.getSarMsgRefNum()!=null && msg.getSarMsgRefNum().trim().length()>0){
						sb.append(msg.getSarMsgRefNum()).append(separator);
					}else{
						sb.append(separator);
					}			
					sb.append(msg.getSarTotalSeqments()).append(separator);					
					sb.append(msg.getSarSegmentSeqNum()).append(separator);
					sb.append(msg.isInCsm()).append(separator);
					sb.append(msg.getPriority()).append(separator);
					if(msg.getContentType()!=null && msg.getContentType().contains(separator)){
						sb.append(msg.getContentType().replaceAll(separator, repl_sep)).append(separator);
					}else{
						sb.append(msg.getContentType()).append(separator);
					}
					if(msg.getTextContent()!=null){
						String text = msg.getTextContent();				
						if(text.contains(separator)){
							text = msg.getTextContent().replaceAll(separator, repl_sep);	
						}			
						sb.append(text).append(separator);
					}else{
						sb.append(separator);
					}				
					sb.append(msg.getStatusCode()).append(separator);
//					sb.append(msg.getStatusText()).append(separator);
					
//					sb.append(msg.inClientPull()).append(separator);
					sb.append(msg.getInClientPull()).append(separator);
					
					sb.append(msg.outClientPull()).append(separator);
//					sb.append(msg.getDeliveryChannel()).append(separator);
					sb.append(msg.getSenderAddrType()).append(separator);
					sb.append(msg.getRecipientAddrType()).append(separator);
					sb.append(msg.getMilterActionCode()).append(separator);
					sb.append(msg.getRA2P()).append(separator);
					sb.append(msg.getOA2P()).append(separator);
					sb.append(msg.getCurrentA2P()).append(separator);
					sb.append(msg.getActionCode()).append(separator);
					sb.append(msg.getSplitStatus()).append(separator);
					sb.append(msg.getConnectionID()).append(separator);
					sb.append(msg.getOperatorPriority()).append(separator);
//					sb.append(msg.getSpecialDataCodingScheme()).append(separator);
					if(msg.getDateIn()!=null){
						sb.append(dateFormat.format(msg.getDateIn())).append(separator);
					}else{
						sb.append(separator);
					}
					if(msg.getMimeMultiPartData() != null && msg.getMimeMultiPartData().length > 0
						&& GmmsMessage.AIC_MSG_TYPE_BINARY.equalsIgnoreCase(msg.getGmmsMsgType())){
						BASE64Encoder base64=new BASE64Encoder();
						String text = base64.encode(msg.getMimeMultiPartData());
						sb.append(text).append(separator);
					}else{
						sb.append(separator);
					}
					sb.append(msg.getMessageType()).append(separator);
					sb.append(msg.getSenderAddrTon()).append(separator);
					sb.append(msg.getRecipientAddrTon()).append(separator);
					sb.append(msg.getStatusText());
					sb.append(separator).append(msg.getServiceTypeID());
					
					if(msg.getScheduleDeliveryTime()!=null){
						sb.append(separator).append(dateFormat.format(msg.getScheduleDeliveryTime()));
					}else{
						sb.append(separator);
					}
					if (msg.getInnerTransaction()!=null) {
						sb.append(separator).append(msg.getInnerTransaction().toString());
					}else {
						sb.append(separator);
					}
					
					sb.append(separator).append(msg.getRetriedNumber()).append(separator);
					if(msg.getNextRetryTime()!=null){
						sb.append(dateFormat.format(msg.getNextRetryTime())).append(separator);
					}else{
						sb.append(separator);
					}
					sb.append(msg.getDeliveryChannel()).append(separator);	
					if (msg.getUdh() !=null && msg.getUdh().length>0) {
						sb.append(bytesToHexString(msg.getUdh())).append(separator);	
					}else {
						sb.append(separator);
					}
					
					if(msg.getInTextContent()!=null){
						String text = msg.getInTextContent();				
						if(text.contains(separator)){
							text = msg.getInTextContent().replaceAll(separator, repl_sep);	
						}			
						sb.append(text).append(separator);
					}else{
						sb.append(separator);
					}
					
					if(msg.getInContentType()!=null && msg.getInContentType().contains(separator)){
						sb.append(msg.getInContentType().replaceAll(separator, repl_sep)).append(separator);
					}else{
						sb.append(msg.getInContentType()).append(separator);
					}
					
					if (msg.getRoutingSsIDs()!=null) {
						sb.append(msg.getRoutingSsIDs().toString()).append(separator);
					}else {
						sb.append(separator);
					}
					if (msg.getRMncMcc()!=null) {
						sb.append(msg.getRMncMcc().toString()).append(separator);
					}else {
						sb.append(separator);
					}
									
				} catch (Exception e) {
					if(log.isInfoEnabled()){
						log.info(msg,"Failed to convert message to Redis string, and exception is {}", e.getMessage());
					}
					return null;
				}
				return sb.toString();
			}
		  
		  public static String bytesToHexString(byte[] src){  
			    StringBuilder stringBuilder = new StringBuilder("");  
			    if (src == null || src.length <= 0) {  
			        return null;  
			    }  
			    for (int i = 0; i < src.length; i++) {  
			        int v = src[i] & 0xFF;  
			        String hv = Integer.toHexString(v);  
			        if (hv.length() < 2) {  
			            stringBuilder.append(0);  
			        }  
			        stringBuilder.append(hv);  
			    }  
			    return stringBuilder.toString();  
			} 
		  
		  public static byte[] hexStringToBytes(String hexString) {  
			    if (hexString == null || hexString.equals("")) {  
			        return null;  
			    }  
			    hexString = hexString.toUpperCase();  
			    int length = hexString.length() / 2;  
			    char[] hexChars = hexString.toCharArray();  
			    byte[] d = new byte[length];  
			    for (int i = 0; i < length; i++) {  
			        int pos = i * 2;  
			        d[i] = (byte) (charToByte(hexChars[pos]) << 4 | charToByte(hexChars[pos + 1]));  
			    }  
			    return d;  
			} 
		  private static byte charToByte(char c) {  
			    return (byte) "0123456789ABCDEF".indexOf(c);  
			}  
	
	public static void main(String[] args) {
		String t = "43<,49<,1383_136454659330320034<,1275930034<,1001637964<,1383<,1378<,1383<,1378<,text<,10<,null<,11400123456<,829034<,null<,null<,2013-03-29 08:43:13<,2013-03-30 06:43:13<,true<,0<,<,null<,null<,null<,0<,0<,false<,-1<,a2p_smpp_4<,ASCII<,0<,Success<,true<,false<,MultiSmppServer3:CoreEngine1:MultiSmppClient2<,1<,1<,0<,1079<,1079<,1079<,-1<,0<,sure1.115_R<,5<,null<,2013-03-29 16:43:14<,<,Submit";
		byte[] s = new byte[3];
		s[0]=1;
		s[1]=2;
		s[2]=3;
		try {
			GmmsMessage msg = SerializableHandler.convertRedisMssage2GmmsMessage(t);
			
			System.out.println(msg);
			System.out.println(bytesToHexString(s));
		} catch (Exception e) {

			e.printStackTrace();
		}
	}
}
