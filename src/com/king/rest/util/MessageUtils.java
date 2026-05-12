package com.king.rest.util;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CharMatcher;
import com.king.framework.SystemLogger;
import com.king.gmms.util.charset.Convert;
import com.king.message.gmms.GmmsMessage;



/**
 *
 */
public class MessageUtils {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(MessageUtils.class);        
    public static int splitNum(final String content) {
        //TODO:实际上还要根据通道决定最大长度，有些通道不支持 7bit 编码就不能用160个字符
        //所以先统一使用140个字符拆分        
        int spitLength = 140;
        String charset = "ASCII";
        int msglen = 0;
        if (isGmsCharset(content)) {
        	try {
        		msglen = Convert.convert2GSM(content).length;
			} catch (Exception e) {
				// TODO: handle exception
			}
        	
            spitLength = 160;
        }else if(isLatinCharset(content)) {
        	try {
        		msglen = content.getBytes(GmmsMessage.AIC_CS_ISO8859_1).length;
			} catch (Exception e) {
				// TODO: handle exception
			}
        	        	
        }else { 
        	try {
        		msglen = content.getBytes(GmmsMessage.AIC_CS_UCS2).length;
			} catch (Exception e) {
				// TODO: handle exception
			}        	
        }
        return (msglen > spitLength)
                ? (int) Math.ceil((double) msglen / spitLength)
                : 1;

    }
    
    public static String getCharset(final String content) {        
        String charset = "ASCII";        
        if (isGmsCharset(content)) {
        	charset = GmmsMessage.AIC_CS_ASCII;
        }else if(isLatinCharset(content)) {
        	charset = GmmsMessage.AIC_CS_ASCII;        	        	
        }else { 
        	charset = GmmsMessage.AIC_CS_UCS2;
        }
        return charset;

    }

    


    public static long getCurrentPart(byte[] udh) {
        if (udh[1] == (byte) 0x00 && udh[2] == (byte) 0x03) {
            return (long) udh[5];
        } else if (udh[1] == (byte) 0x08 && udh[2] == (byte) 0x04) {
            return (long) udh[6];
        } else {
            return 0;
        }
    }


    public static long getTotalParts(byte[] udh) {
        if (udh[1] == (byte) 0x00 && udh[2] == (byte) 0x03) {
            return (long) udh[4];
        } else if (udh[1] == (byte) 0x08 && udh[2] == (byte) 0x04) {
            return (long) udh[5];
        } else {
            return 0;
        }
    }


    public static long getReferenceNum(byte[] udh) {
        if (udh[1] == 0x00 && udh[2] == 0x03) {
            return (long) udh[3];
        } else if (udh[1] == 0x08 && udh[2] == 0x04) {
            return (long) (udh[3] * 256 + udh[4]);
        } else {
            return 0;
        }
    }


    public static boolean isGmsCharset(String text) {
        try {
            if (text == null) {
                return true;
            }
            //String tmp = Convert.convertGSM2String(Convert.decodeGSM7Bit(Convert.encode7bit(Convert.convert2GSM(text),0),0));
            //return tmp.equals(text);
            return CharMatcher.ASCII.matchesAllOf(text);
        } catch (Exception e) {
        }
        return false;
    }


    /*
     * 是否纯英文拉丁字符
     */
    public static boolean isLatinCharset(String content) {
        try {
			/*
			 * char[] chars = content.toCharArray(); byte[] bytes =
			 * content.getBytes("iso-8859-1"); for (int index = 0; index < bytes.length;
			 * index++) { if (bytes[index] == 63) { if (chars[index] != 63) { return false;
			 * } } }
			 */
        	if(!Charset.forName("iso-8859-1").newEncoder().canEncode(content)) {
        		return Charset.forName("iso-8859-15").newEncoder().canEncode(content);
        	}else {
        		return true;
        	}
            //return true;
        } catch (Exception e) {
        	
        }
        return false;
    }
    
    public static void main(String[] args) {
    	String s = "Hello! LEX is arriving within 30 mins to your location Yoob$[]$#@@!@#$$^&*()`o for pickup. Please ensure\n"
    			+ "all items are ready and packed properly. Thanks!";
    	if(!Charset.forName("iso-8859-1").newEncoder().canEncode(s)) {
    		System.out.println(Charset.forName("iso-8859-15").newEncoder().canEncode(s));
    	}else {
    		System.out.println(1+"true");
    	}
		//System.out.println(Charset.forName("iso-8859-16").newEncoder().canEncode(s));
    	String t1="65:10,12|77:13,14,16|28:12,13";
    	String [] tempSmsOptionSubmitRetryCodeList = t1.split("\\|");
    	Map<String, List<Integer>> map = new HashMap<>();
		for (int i = 0; i < tempSmsOptionSubmitRetryCodeList.length; i++) {
			String [] tempSmsOptionSubmitRetryCodeArray = tempSmsOptionSubmitRetryCodeList[i].split(":");
			String [] lens = tempSmsOptionSubmitRetryCodeArray[1].split(",");
			List<Integer> lenList = new ArrayList();
			for(int t=0; t<lens.length; t++) {
				lenList.add(Integer.parseInt(lens[t].trim()));
			}
			map.put(tempSmsOptionSubmitRetryCodeArray[0], lenList);
		}
		for(Map.Entry<String, List<Integer>> entry: map.entrySet()) {
			String key = entry.getKey();
			List<Integer> value = entry.getValue();
			System.out.println("key:"+key +"--value:"+value);
		}
	}

}
