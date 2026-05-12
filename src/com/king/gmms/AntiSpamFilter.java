package com.king.gmms;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.king.framework.SystemLogger;
import com.king.framework.lifecycle.LifecycleListener;
import com.king.framework.lifecycle.LifecycleSupport;
import com.king.framework.lifecycle.event.Event;
import com.king.gmms.connectionpool.systemmanagement.ConnectionManagementForMGT;
import com.king.gmms.domain.A2PCustomerConfig;
import com.king.message.gmms.GmmsMessage;
public class AntiSpamFilter implements LifecycleListener {
	
	
	private static SystemLogger log = SystemLogger
	.getSystemLogger(AntiSpamFilter.class);
    LifecycleSupport lifecycle = null;//used for notify
    private boolean isLoad = false;
    private String strPath;
    private static final String VALUE_SEP = ",";
    private static final String PATTERN_SEP = "|";
    private static final String ASCII_BEG = ".*(?:";
    private static final String ASCII_END = ").*";
    private static final String UNICODE_BEG = ".*(?:";
    private static final String UNICODE_END = ").*";
    private static final int GROUP_NUM = 10;
    private static String SPECIAL_CHARS="\\\"'^$*+?./(){}|-<>[]";
    private Map<Integer,List<Pattern>> map_InAscii = new HashMap<Integer,List<Pattern>>();
    private Map<Integer,List<Pattern>> map_OutAscii = new HashMap<Integer,List<Pattern>>();
    private Map<Integer,List<Pattern>> map_InUnicode = new HashMap<Integer,List<Pattern>>();
    private Map<Integer,List<Pattern>> map_OutUnicode = new HashMap<Integer,List<Pattern>>();
    
    private static final String HEXES = "0123456789ABCDEF";  
    private static final String HEX_CHARSET = "UTF-16BE";

    
    public AntiSpamFilter(String path){
    	strPath = path;
    	initSpecialCharacters();
    }
    
    private void initSpecialCharacters(){
    	String characters2Escape = GmmsUtility.getInstance().getCommonProperty("AntiSpam.Characters2Escape");
    	if(null==characters2Escape){
    		return;
    	}
    	char[] charr = characters2Escape.toCharArray();
    	for(char ch:charr){
    		if(' '!=ch && SPECIAL_CHARS.indexOf(ch)<0){
    			SPECIAL_CHARS = SPECIAL_CHARS.concat(String.valueOf(ch));
    		}
    	}
    	
        log.debug("SPECIAL_CHARS={}",SPECIAL_CHARS);
    	
    }
	public int OnEvent(Event event) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	public boolean loadAntiSpamInfo() {
		
		
		try {

			ArrayList<A2PCustomerConfig> confBlockList = new ArrayList<A2PCustomerConfig>();
			File conf = new File(strPath);
			
			confBlockList = A2PCustomerConfig.parse(conf,"UTF-8");
			
			if(confBlockList != null){
				for (A2PCustomerConfig block : confBlockList) {
					int ssid = block.getMandatoryInt("CCBSSID");
					
					String inascii = block.getString("IN_ASCII");
					inascii = this.escapeSpecialChars(inascii);
					initASCIIKey(ssid,inascii,true);
					String outascii = block.getString("OUT_ASCII");
					outascii = this.escapeSpecialChars(outascii);
					initASCIIKey(ssid,outascii,false);
					
					String inunicode = block.getString("IN_UNICODE");
					inunicode = this.escapeSpecialChars(inunicode);
					initUnicodeKey(ssid,inunicode,true);
					
					String outunicode = block.getString("OUT_UNICODE");
					outunicode = this.escapeSpecialChars(outunicode);
					initUnicodeKey(ssid,outunicode,false);
					
					
				}
			}
	
		} catch (Exception ex) {
			if(log.isInfoEnabled()){
				log.info("load antiSpam error:{}",ex);
			}
		}

		return isLoad;			
	}
	
	public boolean checkContent(int ssid, GmmsMessage msg, boolean isIn, boolean isAscii){
		Map<Integer,List<Pattern>> map = null;
		map = (isIn == true ? map_InAscii : map_OutAscii);
		if (isAscii) {
			return checkContent(ssid, msg, map, isAscii);
		} else {
			if (checkContent(ssid, msg, map, isAscii))
				return true;
			map = (isIn == true ? map_InUnicode : map_OutUnicode);
			return checkContent(ssid, msg, map, isAscii);
		}

	}
	/**
	 * clear antispam map
	 * @return
	 */
	public void clearAntiSpamInfo(){
		map_InAscii.clear();
		map_OutAscii.clear(); 
		map_InUnicode.clear(); 
		map_OutUnicode.clear(); 
	}
	/**
	 * check content
	 * @param ssid
	 * @param msg
	 * @param map
	 * @param isAscii
	 * @return
	 */
	private boolean checkContent(int ssid, GmmsMessage msg, Map<Integer,List<Pattern>> map, boolean isAscii){
	
		List<Pattern> list = map.get(ssid);
		if(list != null){
			String content = msg.getTextContent();
			for(Pattern pattern : list){
				if(matchContent(pattern,content)){
					// ATOP-145 Benson Chen: log spamed keyword
					String strRawPattern = pattern.pattern();
					String strPatternNoSpecChar = recoverSpecialChars(strRawPattern, isAscii);
					if (isAscii) {
						if(log.isInfoEnabled()){
							log.info("{} Match with ASCII keywords:{};content={}",msg.getMsgID(),strPatternNoSpecChar,content);
						}
					} else {
						if(log.isInfoEnabled()){
							log.info("{} Match with unicode keywords:{};content={}",msg.getMsgID(),convert2HexFormat(strPatternNoSpecChar,true),content);
						}
					}
					return true;
				}
			}
		}
		return false;
	}
	
	/**
	 * RecoverSpecialChars
	 * @param strRaw
	 * @param isAscii
	 * @return comma seperated pattern, without pattern header and tail
	 */
	private String recoverSpecialChars(String strRaw, boolean isAscii) {
		// check param
		if (null == strRaw || strRaw.length() < 1) {
			return null;
		}
		
		// remove pattern header and tail
		String strSub = "";
		if (isAscii) {
			strSub = strRaw.substring(ASCII_BEG.length(), strRaw.length() - ASCII_END.length());
		} else {
			strSub = strRaw.substring(UNICODE_BEG.length(), strRaw.length() - UNICODE_END.length());
		}
		
		//recover special chars
		String strRet = unEscapeSpecialChars(strSub);
		
		// replace '|' with ','
		strRet = strRet.replace(PATTERN_SEP, VALUE_SEP);	
		
		return strRet;
	}

	/**
	 * convert unicode String to Hex format
	 * @param strValue
	 * @return hex String
	 */
	private String convert2HexFormat(String strValue, boolean isPattern) {

		// check param
		if (null == strValue || strValue.length() < 1) {
			return null;
		}

		byte[] raw = null;
		
		try {
			raw = strValue.getBytes(HEX_CHARSET);
		} catch (UnsupportedEncodingException e) {
			log.error("Exception raised in convert2HexFormat: {}" , e);
			return null;
		}
		
		final StringBuilder strHex = new StringBuilder(2 * raw.length);
		for (final byte bItem : raw) {		
			strHex.append(HEXES.charAt((bItem & 0xF0) >> 4)).append(
					HEXES.charAt((bItem & 0x0F)));		
		}

		return strHex.toString();
	}
	
	private boolean matchContent(Pattern pattern, String content) {

		if (pattern == null) {
			return false;
		}
		Matcher matcher = null;
		matcher = pattern.matcher(content);

		if (matcher != null && matcher.matches()) {
			return true;
		} else
			return false;
	}
	
	private void initUnicodeKey(int ssid, String value, boolean isIn){
		
		Map<Integer, List<Pattern>> map = (isIn == true ? map_InUnicode
				: map_OutUnicode);
		
		List<Pattern> list = map.get(ssid);
		if (list != null) {
			log.warn("antiSpam load warn: has same ssid {} in the antiSpam file.",ssid);
			return;
		}
		
		if(value == null || value.trim().equalsIgnoreCase("")){
			log.warn("antiSpam load warn: ssid {} has empty info in the antiSpam file.",ssid);
			return;
		}		
		
		list = new ArrayList<Pattern>();
		StringBuffer buffer = null;

		String[] values = splitKeywords(value);
		if (values == null || values.length < 1) {
			log.warn("antiSpam load warn: ssid {} has empty info in the antiSpam file.",ssid);
			return;
		}
		
		for(int i = 0 ; i < values.length ; i++){
			
			if(i%GROUP_NUM == 0){
				buffer = new StringBuffer();
				buffer.append(UNICODE_BEG);
				buffer.append(values[i]);
			}
			else if ((i + 1) % GROUP_NUM == 0 || (i+1) == values.length) {
				buffer.append("|");
				buffer.append(values[i]);
				buffer.append(UNICODE_END);
				String regular = buffer.toString();
				if (log.isInfoEnabled()) {
					log.info("ssid:{} create Unicode regular:{}",ssid,regular);
				}
				Pattern p = Pattern.compile(regular, Pattern.DOTALL
						| Pattern.CASE_INSENSITIVE);
				list.add(p);
				buffer = null;
				
			}else{
				buffer.append("|");
				buffer.append(values[i]);
			}
			
		}
		if(buffer != null){
			buffer.append(UNICODE_END);
			String regular = buffer.toString();
			if (log.isInfoEnabled()) {
				log.info("ssid:{} create Unicode regular:{}",ssid,regular);
			}
			Pattern p = Pattern.compile(regular, Pattern.DOTALL
					| Pattern.CASE_INSENSITIVE);
			list.add(p);			
		}
		
		map.put(ssid, list);		
		
		
	}
	
	private void initASCIIKey(int ssid, String value, boolean isIn) {
		Map<Integer, List<Pattern>> map = (isIn == true ? map_InAscii
				: map_OutAscii);

		List<Pattern> list = map.get(ssid);
		if (list != null) {
			log.warn("antiSpam load warn: has same ssid {} in the antiSpam file.",ssid);
			return;
		}
				
		if(value == null || value.trim().equalsIgnoreCase("")){
			log.warn("antiSpam load warn: ssid {} has empty info in the antiSpam file.",ssid);
			return;
		}
		
		list = new ArrayList<Pattern>();
		StringBuffer buffer = null;

		String[] values = splitKeywords(value);
		if (values == null || values.length < 1) {
			log.warn("antiSpam load warn: ssid {} has empty info in the antiSpam file.",ssid);
			return;
		}
		
		for(int i = 0 ; i < values.length ; i++){
			
			if(i%GROUP_NUM == 0){
				buffer = new StringBuffer();
				buffer.append(ASCII_BEG);
				buffer.append(values[i]);
			}
			else if ((i + 1) % GROUP_NUM == 0 || (i+1) == values.length) {
				buffer.append("|");
				buffer.append(values[i]);
				buffer.append(ASCII_END);
				String regular = buffer.toString();
				if (log.isInfoEnabled()) {
					log.info("ssid:{} create Ascii regular:{}",ssid,regular);
				}
				Pattern p = Pattern.compile(regular, Pattern.DOTALL
						| Pattern.CASE_INSENSITIVE);
				list.add(p);
				buffer = null;
				
			}else{
				buffer.append("|");
				buffer.append(values[i]);
			}
			
		}
		if(buffer != null){
			buffer.append(ASCII_END);
			String regular = buffer.toString();
			if (log.isInfoEnabled()) {
				log.info("ssid:{} create Ascii regular:{}",ssid,regular);
			}
			Pattern p = Pattern.compile(regular, Pattern.DOTALL
					| Pattern.CASE_INSENSITIVE);
			list.add(p);			
		}
		
		map.put(ssid, list);
	}
	
	/**
	 * splitKeywords, remove wrong ",,"
	 * @param value
	 * @return
	 */
	private String[] splitKeywords(String value) {	
		if (value==null || value.trim().length()<1) {
			return new String[0];
		}
		
		String[] values = value.trim().split(VALUE_SEP);
		List<String> valueList = new ArrayList<String>();
		// filter ",,"
		for(String item : values) {
			if (item != null && item.trim().length()>0) {
				valueList.add(item.trim());
			}
		}
		
		return valueList.toArray(new String[0]);
	}
	/**
	 * escape special characters
	 * @param reg
	 * @return
	 */
	private static String escapeSpecialChars(String reg){
		if(null == reg){
			return null;
		}
		if(null == SPECIAL_CHARS){
			return reg;
		}
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
		if (log.isDebugEnabled()) {
			log.debug("After escapeSpecialChars={}",reg);	
		}
		return reg;
	}
	
	/**
	 * unEscapeSpecialChars
	 * @param reg
	 * @return
	 */
	private static String unEscapeSpecialChars(String reg) {
		if (null == reg || reg.length()<1) {
			return null;
		}
		String strRet = new String(reg);
		for(char ch:SPECIAL_CHARS.toCharArray()){
			if(strRet.indexOf(ch)>=0){
				strRet = strRet.replace("\\"+ch, ""+ch);
			}
		}
		//log.debug("after unEscapeSpecialChars="+strRet);
		return strRet;
	}
}
