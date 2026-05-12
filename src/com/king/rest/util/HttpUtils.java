package com.king.rest.util;

import java.security.MessageDigest;





public class HttpUtils {
	
    private static final String HEXES = "0123456789ABCDEF";  

	/**
     * get parameter value as customer configuration
     * @param param
     * @param message
     * @return
     */
  
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
        	 
              return null;  
          }  
      }
      
      public static String format2Digits(String s) {
  		if (s == null) {
  			return s;
  		}
  		s = s.toUpperCase();
  		if (s.length() == 2) {
  			return s;
  		}
  		if (s.length() == 1) {
  			return "0" + s;
  		}
  		return s.substring(s.length() - 2);
  	}
      
     
}
