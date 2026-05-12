package com.king.gmms.protocol.commonhttp;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.regex.Matcher;

import org.apache.commons.codec.binary.Hex;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.domain.http.HttpConstants;
import com.king.gmms.domain.http.HttpParam;
import com.king.message.gmms.GmmsMessage;

public class HttpUtils {
	private static SystemLogger log = SystemLogger.getSystemLogger(HttpUtils.class);
    private static final String HEXES = "0123456789ABCDEF";  

	/**
     * get parameter value as customer configuration
     * @param param
     * @param message
     * @return
     */
//    public static String getParameter(HttpParam param,GmmsMessage message,A2PCustomerInfo cst){
//    	String mval = "";
//    	String pval = param.getParam();
//		String ptype = param.getType();
//		String pvalue = param.getDefaultValue();
//		Object value = null;
//		if(pvalue!=null && !"".equals(pvalue)){
//			mval = pvalue;
//		}else{
//			if(HttpConstants.ELEM_ATTR_TYPE_CCB.equalsIgnoreCase(ptype) && cst!=null){
//				 A2PSingleConnectionInfo singleInfo = (A2PSingleConnectionInfo)cst;
//				 value = singleInfo.getProperty(pval);
//			}else{
//				 value = message.getProperty(pval);
//			}
//			if(value == null){
//				log.warn(message,"Get null property with param:{}",pval);
//				mval =  "";
//			}else if(value instanceof Integer||value instanceof Long||value instanceof Byte||value instanceof Short){
//				mval = ""+value;
//			}else if(value instanceof String){
//				mval = (String)value;
//			}else{
//				mval = value.toString();
//			}
//		}
//		return mval;
//    }
    
    public static String getParameter(HttpParam param, GmmsMessage message,
			A2PCustomerInfo cst) {
    	
		String mval = "";
		String pval = param.getParam();// standar
		String ptype = param.getType();

		// add by kevin
		String pCcbName = param.getCcbName();

		String pvalue = param.getDefaultValue();
		Object value = null;
		if (pvalue != null && !"".equals(pvalue)) {
			mval = pvalue;
		} else {
			if (HttpConstants.ELEM_ATTR_TYPE_CCB.equalsIgnoreCase(ptype)
					&& cst != null) {
				A2PSingleConnectionInfo singleInfo = (A2PSingleConnectionInfo) cst;
	
				if (pCcbName!=null && !"".equals(pCcbName.trim())) {
					value = singleInfo.getProperty(pCcbName.trim());
				} else {
					value = singleInfo.getProperty(pval);
				}

			} else {
				value = message.getProperty(pval);
			}
			if (value == null) {
				log.warn(message, "Get null property with param:{}", pval);
				mval = "";
			} else if (value instanceof Integer || value instanceof Long
					|| value instanceof Byte || value instanceof Short) {
				mval = "" + value;
			} else if (value instanceof String) {
				mval = (String) value;
			} else {
				mval = value.toString();
			}
		}
		return mval;
	}
    
    /**
     * Set parameter value as customer configuration
     * @param param
     * @param message
     * @return
     */
    public static void setParameter(HttpParam param,GmmsMessage message,A2PCustomerInfo cst,String value){
	   	String pval = param.getParam();
		String ptype = param.getType();
		if(value!=null && !"".equals(value)){
			Object mval = (Object)value;
			message.setProperty(pval, mval);
		}else if(HttpConstants.ELEM_ATTR_TYPE_CCB.equalsIgnoreCase(ptype) && (cst != null)){
				A2PSingleConnectionInfo singleInfo = (A2PSingleConnectionInfo)cst;
				Object property = singleInfo.getProperty(pval);
				message.setProperty(pval, property);
		}
    }
	/**
	 * get status code from dr status text
	 * @param state
	 * @return
	 */
     public static int getCodeFromDRStatus(String state) {
        if (state == null) {
            return 10900;
        }
        else if ("DELIVRD".equalsIgnoreCase(state)) {
            return 10000;
        }
        else if ("EXPIRED".equalsIgnoreCase(state)) {
            return 10200;
        }
        else if ("DELETED".equalsIgnoreCase(state)) {
            return 10300;
        }
        else if ("UNDELIV".equalsIgnoreCase(state)) {
            return 10400;
        }
        else if ("ACCEPTD".equalsIgnoreCase(state)) {
            return 10001;
        }
        else if ("REJECTD".equalsIgnoreCase(state)) {
            return 10500;
        }
        else if ("UNKNOWN".equalsIgnoreCase(state)) {
            return 10900;
        }
        else if ("Rejected by NotPaid".equalsIgnoreCase(state)) {
            return 10800;
        }
        else {
            return 10900;
        }
    }
    
 	/**
 	 * get Bytes By Hex
 	 * @param hexStr
 	 * @return
 	 */
     public static byte[] getBytesByHexString(String hexStr){
		 if(hexStr == null || "".equals(hexStr.trim())){
			 return null;
		 }
 		int udhLen = hexStr.length();
 		byte[] bytes = new byte[udhLen/2];
 		char [] chars = new char[udhLen];
 		hexStr.getChars(0, udhLen, chars, 0);
 		int len = chars.length;
 		StringBuilder sb = new StringBuilder();
 		int j = 0;
 		for(int i=0;i<len;i=i+2){
 			int v1 = HEXES.indexOf(Character.toUpperCase(chars[i]));
 			int v2 = HEXES.indexOf(Character.toUpperCase(chars[i+1]));
 			byte b = (byte)(v1 & 0xFF);
 			b = (byte)(b <<4);
 			b = (byte)(b | v2);
 			bytes[j++]=b;
 		}
 		return bytes;
 	}
     /**
  	 * 
  	 * @param s
  	 * @return
  	 */
      public static String format2Digits(String s) {
          if (s == null){
              return s;        	  
          }
      	  s = s.toUpperCase();
          if (s.length() == 2){
        	  return s;
          }
          if (s.length() == 1){
        	  return "0" + s;
          }
          return s.substring(s.length() - 2);
      }
      /**
       * md5 encryption
       * @param s
       * @return
       */
      public final static String md5Encrypt(String s) {  
    	  return encrypt(s,"MD5");
      }
      /**
       * md5 encryption
       * @param s
       * @return
       */
      public final static String shaEncrypt(String s) {  
    	  try {  
              byte[] strTemp = s.getBytes();  
              MessageDigest md = MessageDigest.getInstance("SHA");   
              md.update(strTemp);  
              byte[] bytes = md.digest();  
              return new String(org.apache.soap.encoding.soapenc.Base64.encode(bytes));
          } catch (Exception e) {  
        	  log.warn("encrypt error with method SHA",e);
              return null;  
          }  
      }
      /**
       * md5 encryption
       * @param s
       * @return
       */
      public final static String encrypt(String s,String method) {  
          char hexDigits[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',  
                  'a', 'b', 'c', 'd', 'e', 'f' };  
          try {  
              byte[] strTemp = s.getBytes();  
              MessageDigest md = MessageDigest.getInstance(method);   
              md.update(strTemp);  
              byte[] bytes = md.digest();  
              int j = bytes.length;  
              char str[] = new char[j * 2];  
              int k = 0;  
              for (int i = 0; i < j; i++) {  
                  byte byte0 = bytes[i];  
                  str[k++] = hexDigits[byte0 >>> 4 & 0xf];  
                  str[k++] = hexDigits[byte0 & 0xf];  
              }  
              return new String(str);  
          } catch (Exception e) {  
        	  log.warn("encrypt error with method "+method,e);
              return null;  
          }  
      }  
      
      public final static String encryptUP(String s,String method) {  
          char hexDigits[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',  
                  'A', 'B', 'C', 'D', 'E', 'F' };  
          try {  
              byte[] strTemp = s.getBytes();  
              MessageDigest md = MessageDigest.getInstance(method);   
              md.update(strTemp);  
              byte[] bytes = md.digest();  
              int j = bytes.length;  
              char str[] = new char[j * 2];  
              int k = 0;  
              for (int i = 0; i < j; i++) {  
                  byte byte0 = bytes[i];  
                  str[k++] = hexDigits[byte0 >>> 4 & 0xf];  
                  str[k++] = hexDigits[byte0 & 0xf];  
              }  
              return new String(str);  
          } catch (Exception e) {  
        	  log.warn("encrypt error with method "+method,e);
              return null;  
          }  
      }  
      
      /**
       * encryption
       * @param s
       * @return
       */
      public final static String encrypt(byte[] src,String method) {  
          char hexDigits[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',  
                  'a', 'b', 'c', 'd', 'e', 'f' };  
          try {  
              MessageDigest md = MessageDigest.getInstance(method);   
              md.update(src);  
              byte[] bytes = md.digest();  
              int j = bytes.length;  
              char str[] = new char[j * 2];  
              int k = 0;  
              for (int i = 0; i < j; i++) {  
                  byte byte0 = bytes[i];  
                  str[k++] = hexDigits[byte0 >>> 4 & 0xf];  
                  str[k++] = hexDigits[byte0 & 0xf];  
              }  
              return new String(str);  
          } catch (Exception e) {  
        	  log.warn("encrypt error with method "+method,e);
              return null;  
          }  
      }  
      /**
  	 * escape special characters
  	 * @param reg
  	 * @return
  	 */
  	public static String escapeSpecialChars(String reg){
  		if(null == reg){
  			return null;
  		}
  		final String SPECIAL_CHARS="\\\"'^$*+?./(){}|-<>[]";
  		for(char ch:SPECIAL_CHARS.toCharArray()){
  			if(reg.indexOf(ch)>=0){
  				if('\\' == ch){
  					reg = reg.replaceAll("\\\\", Matcher.quoteReplacement("\\\\"));
  				}else if('$' == ch){
  					reg = reg.replaceAll("\\$", "\\\\\\$");
  				}else if('\"' == ch){
  					reg = reg.replaceAll("\"", "\\\"");
  				}else{
  					reg = reg.replaceAll("\\"+ch, "\\\\"+ch);
  				}
  			}
  		}
  		return reg;
  	}
	/**
	 * unEscapeSpecialChars
	 * @param reg
	 * @return
	 */
  	public static String unEscapeSpecialChars(String reg) {
  		reg =  reg.replace("\\r", "\r");
		reg =  reg.replace("\\n", "\n");
		reg =  reg.replace("\\t", "\t");
		return reg;
	}
  	public static void main(String[] args) {
		System.out.println(HttpUtils.md5Encrypt("SMSsoftling:Lingksmskey:8613880454201"));
	}
  	
  	/**
  	 * 
  	 * @param csts
  	 * @param dateParam
  	 * @param value
  	 * @return
  	 * @throws Exception 
  	 */
  	public static Date parseHttpDate(A2PCustomerInfo csts,
			HttpParam dateParam, String value) throws Exception {

		if(log.isTraceEnabled()){
			log.trace("date before convert:{};HttpParam = {}", value,
				dateParam);
		}
		if (value == null || value.length() == 0 || csts == null) {
			return null;
		}
		Date date = null;
		String ptype = dateParam.getType();
		try {
			if (HttpConstants.EXPIRYDATE_TYPE_ABSOLUTE.equalsIgnoreCase(ptype)) {
				SimpleDateFormat sdf = new SimpleDateFormat(dateParam.getFormat());
				sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
				date = sdf.parse(value);
			} else if (HttpConstants.EXPIRYDATE_TYPE_RELATIVE
					.equalsIgnoreCase(ptype)) {
				Calendar now = Calendar.getInstance();
				if (dateParam.getFormat().equalsIgnoreCase(
						HttpTimeFormat.MILLISECOND)) {
					now.add(Calendar.MILLISECOND, Integer.parseInt(value));
				} else if (dateParam.getFormat().equalsIgnoreCase(
						HttpTimeFormat.MINUTE)) {
					now.add(Calendar.MINUTE, Integer.parseInt(value));
				} else if (dateParam.getFormat().equalsIgnoreCase(
						HttpTimeFormat.HOUR)) {
					now.add(Calendar.HOUR, Integer.parseInt(value));
				} else if (dateParam.getFormat().equalsIgnoreCase(
						HttpTimeFormat.DAY)) {
					now.add(Calendar.DAY_OF_MONTH, Integer.parseInt(value));
				}
				date = now.getTime();
			}
			
		} catch (Exception e) {
			//
		}
		
		if (date == null) {
			throw new Exception("Invalid Date");
			
		}		
		return date;
		
	}
  	
  	public static String getSHA256(String str) {
        MessageDigest messageDigest;
        String encodeStr = "";
        try {
            messageDigest = MessageDigest.getInstance("SHA-256");
            messageDigest.update(str.getBytes("UTF-8"));
            encodeStr = byte2Hex(messageDigest.digest());
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return encodeStr;
    }
  	
  	private static String byte2Hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        String temp = null;
        for (byte aByte : bytes) {
            temp = Integer.toHexString(aByte & 0xFF);
            if (temp.length() == 1) {
                // 1得到一位的进行补0操作
                sb.append("0");
            }
            sb.append(temp);
        }
        return sb.toString();
    }
  	
}
