/**
 */
package com.king.gmms;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.king.framework.SystemLogger;
import com.king.framework.lifecycle.LifecycleListener;
import com.king.framework.lifecycle.LifecycleSupport;
import com.king.framework.lifecycle.event.Event;
import com.king.message.gmms.GmmsMessage;

/**
 * @author bensonchen
 * @version 1.0.0
 */
public class ContentTemplate implements LifecycleListener {

	private static SystemLogger log = SystemLogger
	.getSystemLogger(ContentTemplate.class);

	/**
	 * Ascii rule of content template
	 * <p>
	 * key: tplID, value: content pattern list
	 * </p>
	 * 
	 */
	private Map<String, List<Pattern>> ruleAscii = new HashMap<String, List<Pattern>>();

	/**
	 * Unicode rule of content template
	 * <p>
	 * key: tplID, value: content pattern list
	 * </p>
	 */
	private Map<String, List<Pattern>> ruleUnicode = new HashMap<String, List<Pattern>>();
	
	/**
	 * key1:tplId, key2:dcTplId, key3:ContentType, value: content
	 */
	private Map<String, Map<String, Map<String, String>>>  templateIdMaps = new HashMap<String, Map<String,Map<String, String>>>();
	private Map<String, Map<String, Map<String, String>>>  signatureMaps = new HashMap<String, Map<String,Map<String, String>>>();

	/**
	 * key1:tplId, key2:ContentType, value: content
	 */
	private Map<String, Map<String, String>>  templateContentMaps = new HashMap<String, Map<String, String>>();

	/**
	 * Attribute separator
	 */
	private static final String ATTR_SEP = ":";

	private LifecycleSupport lifecycle;

	public ContentTemplate() {
		lifecycle = GmmsUtility.getInstance().getLifecycleSupport();
		lifecycle.addListener(Event.TYPE_CONTENT_TEMPLATE_RELOAD, this, 1);
	}

	/**
	 * @param event
	 * @return
	 * @see com.king.framework.lifecycle.LifecycleListener#OnEvent(com.king.framework.lifecycle.event.Event)
	 */
	public int OnEvent(Event event) {

		log.debug("Event Received. Type: {}", event.getEventType());

		if (event.getEventType() == Event.TYPE_CONTENT_TEMPLATE_RELOAD) {
			GmmsUtility util = GmmsUtility.getInstance();
			if (loadContentTemplate(util.getContentTemplateFile())) {

				log.info("Reload Content Template success!");

				return 0;
			}
			log.error("Reload Content Template error!");
			return 1;
		}
		return 0;

	}

	public boolean loadContentTemplate(String confPath) {
		boolean ret = true;
		
			log.debug("Loading content template from file {} ...",confPath);
		
		try {
			Map<String, List<Pattern>> tmpRuleAscii = new HashMap<String, List<Pattern>>();
			Map<String, List<Pattern>> tmpRuleUnicode = new HashMap<String, List<Pattern>>();
			Map<String, Map<String, String>>  tmpTemplateContentMaps = new HashMap<String, Map<String, String>>();
			String contentTempateCfg = confPath+"contentTemplate.cfg";			
			parseConfFile(contentTempateCfg, tmpRuleAscii, tmpRuleUnicode, tmpTemplateContentMaps);
			
			Map<String, List<Pattern>> ruleAsciiBak = this.ruleAscii;
			this.ruleAscii = tmpRuleAscii;
			Map<String, List<Pattern>> ruleUnicodeBak = this.ruleUnicode;
			this.ruleUnicode = tmpRuleUnicode;			
			ruleAsciiBak.clear();
			ruleUnicodeBak.clear();
			
			Map<String, Map<String, String>>  templateContentMapsBak = this.templateContentMaps;
			this.templateContentMaps = tmpTemplateContentMaps;
			templateContentMapsBak.clear();
			
			//load portal portalContentTemplate.cfg
			Map<String, List<Pattern>> tmpRuleAscii1 = new HashMap<String, List<Pattern>>();
			Map<String, List<Pattern>> tmpRuleUnicode1 = new HashMap<String, List<Pattern>>();
			Map<String, Map<String, String>>  tmpTemplateContentMaps1 = new HashMap<String, Map<String, String>>();			
			contentTempateCfg = confPath+"portalContentTemplate.cfg";			
			parseConfFile(contentTempateCfg, tmpRuleAscii1, tmpRuleUnicode1, tmpTemplateContentMaps1);
			if(!tmpRuleAscii1.isEmpty()) {
				this.ruleAscii.putAll(tmpRuleAscii1);
			}
			if(!tmpRuleUnicode1.isEmpty()) {
				this.ruleUnicode.putAll(tmpRuleUnicode1);
			}
			
			if(!tmpTemplateContentMaps1.isEmpty()) {
				this.templateContentMaps.putAll(tmpTemplateContentMaps1);
			}
			
			String tempateIdCfg = confPath+"templateId.cfg";	
			Map<String, Map<String, Map<String, String>>> tempTemplateIdMap = new HashMap<String, Map<String, Map<String, String>>>();
			Map<String, Map<String, Map<String, String>>> tempsignatureMap = new HashMap<String, Map<String, Map<String, String>>>();
			parseTemplateIdConfFile(tempateIdCfg, tempTemplateIdMap,tempsignatureMap);
			Map<String, Map<String, Map<String, String>>> signatureMapBak = this.signatureMaps;
			Map<String, Map<String, Map<String, String>>> templateIdMapBak = this.templateIdMaps;
			this.templateIdMaps = tempTemplateIdMap;
			this.signatureMaps = tempsignatureMap;						
			signatureMapBak.clear();
			templateIdMapBak.clear();

		} catch (Exception e) {
			log.warn("Parse content template exception:{}", e);
			ret = false;
		}

		return ret;
	}

	private void parseConfFile(String confPath,
			Map<String, List<Pattern>> tmpRuleAscii,
			Map<String, List<Pattern>> tmpRuleUnicode,
			Map<String, Map<String, String>>  tmpTemplateContentMaps) throws Exception {
		try {
			File confFile = new File(confPath);
			FileInputStream fin = new FileInputStream(confFile);
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					fin, "utf-8"));
			String line = null;
			int lineNum = 0;
			String ruleKey = null;
			while ((line = reader.readLine()) != null) {

				lineNum++;
				// 1. remove the additional white space
				line = line.trim();
				// 2. Ignore the comment line and empty line.
				if (line.startsWith("#") || line.length() == 0) {
					continue;
				}

				// 3. Create a new block
				if (line.startsWith("[") && line.endsWith("]")) {
					try {
						ruleKey = line.substring(1, line.length() - 1).trim();
						if (ruleKey.length() < 1) {
							throw new Exception("Empty ruleKey!");
						}
					} catch (Exception e) {
						log.warn("Configuration file {} format error. Missing ruleKey. Line: {}"
								,confFile.getName(),lineNum);
						ruleKey = null;
					}
				}
				// 4. Fill the block configuration.
				else {
					if (ruleKey == null) {
						continue;
					}
					int sepIndex = line.indexOf(ATTR_SEP);
					if (sepIndex <= 0) {
						log.warn("Wrong configuration {}. Missing Attribute Seperator. Line: {}"
								,line, lineNum);
						continue;
					}
					try {
						String attribute = line.substring(0, sepIndex).trim();
						String value = line.substring(sepIndex + 1,
								line.length()).trim();
						if (attribute.length() < 1 || value.length() < 1) {
							throw new Exception("Empty attribute or value.");
						}
						if ("English".equalsIgnoreCase(attribute)) {
							this.addTemplateContent(ruleKey, GmmsMessage.AIC_CS_ASCII, value, tmpTemplateContentMaps);
							addRules(ruleKey, value, tmpRuleAscii, true);
						} else if ("UNICODE".equalsIgnoreCase(attribute)) {
							this.addTemplateContent(ruleKey, GmmsMessage.AIC_CS_UCS2, value, tmpTemplateContentMaps);
							addRules(ruleKey, value, tmpRuleUnicode, false);
						} else if ("Sender".equalsIgnoreCase(attribute)) {
							this.addTemplateContent(ruleKey, "sender", value, tmpTemplateContentMaps);
							//addRules(ruleKey, value, tmpRuleUnicode, false);
						}else {
							continue;
						}

					} catch (Exception e) {
						log.warn("{} .Wrong configuration {}. Line: {}",e ,line ,lineNum);
						continue;
					}
				}
			}
		} catch (Exception ex) {
			throw ex;
		}
	}
	
	private void addTemplateContent(String ruleKey, String contentType, String content,
			Map<String, Map<String, String>>  tmpTemplateContentMaps){
		Map<String, String> typeContent = tmpTemplateContentMaps.get(ruleKey);
		if (typeContent == null) {
			typeContent = new HashMap<String, String>();
			tmpTemplateContentMaps.put(ruleKey, typeContent);
		}		
		typeContent.put(contentType, content);
		
	}
	
	private void parseTemplateIdConfFile(String confPath,
			Map<String, Map<String, Map<String, String>>> tempTemplateIdMap,
			Map<String, Map<String, Map<String, String>>> tempSignatureMap) throws Exception {
		try {
			File confFile = new File(confPath);
			FileInputStream fin = new FileInputStream(confFile);
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					fin, "utf-8"));
			String line = null;
			int lineNum = 0;
			String ruleKey = null;
			while ((line = reader.readLine()) != null) {

				lineNum++;
				// 1. remove the additional white space
				line = line.trim();
				// 2. Ignore the comment line and empty line.
				if (line.startsWith("#") || line.length() == 0) {
					continue;
				}

				// 3. Create a new block
				if (line.startsWith("[") && line.endsWith("]")) {
					try {
						ruleKey = line.substring(1, line.length() - 1).trim();
						if (ruleKey.length() < 1) {
							throw new Exception("Empty ruleKey!");
						}
					} catch (Exception e) {
						log.warn("Configuration file {} format error. Missing ruleKey. Line: {}"
								,confFile.getName(),lineNum);
						ruleKey = null;
					}
				}
				// 4. Fill the block configuration.
				else {
					if (ruleKey == null) {
						continue;
					}
					int sepIndex = line.indexOf(ATTR_SEP);
					if (sepIndex <= 0) {
						log.warn("Wrong configuration {}. Missing Attribute Seperator. Line: {}"
								,line, lineNum);
						continue;
					}
					try {
						String attribute = line.substring(0, sepIndex).trim();
						String value = line.substring(sepIndex + 1,
								line.length()).trim();
						int fieldIndex = value.indexOf(ATTR_SEP);
						if (fieldIndex <= 0) {
							log.warn("Wrong configuration {}. Missing Field Seperator. Line: {}"
									,line, lineNum);
							continue;
						}
						String field = value.substring(0, fieldIndex).trim();
						String content = value.substring(fieldIndex + 1,
								value.length()).trim();
						
						if (field.length() < 1 || content.length() < 1) {
							throw new Exception("Empty Field or content.");
						}
						
						int contentIndex = content.indexOf(ATTR_SEP);
						if (contentIndex <= 0) {
							log.warn("Wrong configuration {}. Missing Field Seperator. Line: {}"
									,line, lineNum);
							continue;
						}
						String contentType = content.substring(0, contentIndex).trim();
						String textContent = content.substring(contentIndex + 1,
								content.length()).trim();
						
						if (contentType.length() < 1 || textContent.length() < 1) {
							throw new Exception("Empty Field or content.");
						}
						Map<String, String> contentMap = new HashMap<String, String>(); 
						contentMap.put(contentType, textContent);
						Map<String, Map<String, String>> fieldMap = new HashMap<String, Map<String, String>>();
						fieldMap.put(field, contentMap);
						if ("Template".equalsIgnoreCase(ruleKey)) {							
							tempTemplateIdMap.put(attribute, fieldMap);
						}else {
							tempSignatureMap.put(attribute, fieldMap);
						}
						
					} catch (Exception e) {
						log.warn("{} .Wrong configuration {}. Line: {}",e ,line ,lineNum);
						continue;
					}
				}
			}
		} catch (Exception ex) {
			throw ex;
		}
	}

	private void addRules(String ruleKey, String rawValue,
			Map<String, List<Pattern>> ruleMap, boolean isAscii)
			throws PatternSyntaxException {

		List<Pattern> patternList = null;
		if (ruleMap.containsKey(ruleKey)) {
			patternList = ruleMap.get(ruleKey);
		} else {
			patternList = new ArrayList<Pattern>();
			ruleMap.put(ruleKey, patternList);
		}
		Pattern pattern = null;
		if (isAscii) {
			pattern = Pattern.compile(rawValue, Pattern.DOTALL);
		} else {
			pattern = Pattern.compile(rawValue, Pattern.UNICODE_CASE
					| Pattern.DOTALL);
		}
		patternList.add(pattern);
	}

	/**
	 * check if message content matches the pattern of tplId
	 * 
	 * @param msg
	 * @param tplId
	 * @param isAscii
	 * @return
	 */
	public boolean checkContent(GmmsMessage msg, String tplId) {
		boolean isAscii = false;
		if (GmmsMessage.AIC_CS_ASCII.equalsIgnoreCase(msg.getContentType())
				||msg.getContentType().startsWith("ISO-8859")) {
			isAscii = true;
		}

		List<Pattern> patternList = null;
		patternList = ruleAscii.get(tplId);
		if (isAscii) {
			// ASCII
			return matchContent(patternList, msg, isAscii, tplId);
		} else {
			if (matchContent(patternList, msg, true, tplId)) {
				return true;
			}
			// Unicode
			patternList = ruleUnicode.get(tplId);
			return matchContent(patternList, msg, isAscii, tplId);
		}
	}

	private boolean matchContent(List<Pattern> patternList, GmmsMessage msg,
			Boolean isAscii, String tplId) {
		if (patternList == null || patternList.size() < 1) {
			return false;
		}

		String content = msg.getTextContent();

		for (Pattern pattern : patternList) {
			Matcher matcher = null;
			matcher = pattern.matcher(content);
			if (matcher != null && matcher.find()) {
				String strRawPattern = pattern.pattern();
				if (isAscii) {
					if (log.isInfoEnabled()) {
						/*
						 * //log.info(msg, newStringBuilder(200).append(
						 * "Match ASCII content template, tplName=")
						 * .append(tplId
						 * ).append(", pattern=").append(strRawPattern)
						 * .append(", content=").append(content).toString());
						 */

					}
					if(log.isInfoEnabled()){
						log.info("{} Match ASCII content template, tplName={},pattern={},content={}, match={} ",
									msg.getMsgID(), tplId, strRawPattern,content, matcher.toMatchResult().group());
					}

				} else {
					/*
					 * if (log.isInfoEnabled()) { log.info(msg, new
					 * StringBuilder
					 * (200).append("Match Unicode content template, tplName=")
					 * .append(tplId).append(", pattern=").append(GmmsUtility.
					 * convert2HexFormat(strRawPattern))
					 * .append(", content=").append
					 * (GmmsUtility.convert2HexFormat(content)).toString()); }
					 */
					if(log.isInfoEnabled()){
						log.info("{}Match Unicode content template, tplName={},pattern={},content={}, match={} ",
									msg.getMsgID(), tplId, strRawPattern,content,matcher.toMatchResult().group());
					}
				}
				return true;
			}

		}
		return false;
	}

	public Map<String, List<Pattern>> getRuleAscii() {
		return ruleAscii;
	}

	public Map<String, List<Pattern>> getRuleUnicode() {
		return ruleUnicode;
	}

	public String getTemplateIdMaps(String key) {
		Map<String, Map<String, String>> map = templateIdMaps.get(key);
		if (map!= null && !map.isEmpty()) {
			for (String templateId : map.keySet()) {
				return templateId;
			}
		}
		return "";
	}
	
	public String getTemplateContentMaps(String key) {
		Map<String, Map<String, String>> map = templateIdMaps.get(key);
		if (map!= null && !map.isEmpty()) {
			for (Map<String, String> content : map.values()) {
				if (content !=null && !content.isEmpty()) {
					return content.values().iterator().next();
				}
			}
		}
		return "";
	}
	
	
	public String getTemplateContentTypeMaps(String key) {
		Map<String, Map<String, String>> map = templateIdMaps.get(key);
		if (map!= null && !map.isEmpty()) {
			for (Map<String, String> content : map.values()) {
				if (content !=null && !content.isEmpty()) {
					return content.keySet().iterator().next();
				}
			}
		}
		return "";
	}
	
	
	public Map<String, String> getSignatureContentMaps(String key) {
		Map<String, Map<String, String>> map = signatureMaps.get(key);
		if (map!= null && !map.isEmpty()) {
			for (Map<String, String> content : map.values()) {
				if (content !=null && !content.isEmpty()) {
					return content;
				}
			}
		}
		return null;
	}
	
	
	public String getSignatureContentTypeMaps(String key) {
		Map<String, Map<String, String>> map = signatureMaps.get(key);
		if (map!= null && !map.isEmpty()) {
			for (Map<String, String> content : map.values()) {
				if (content !=null && !content.isEmpty()) {
					return content.keySet().iterator().next();
				}
			}
		}
		return "";
	}
	
	public String getSignatureMaps(String key) {
		Map<String, Map<String, String>> map = signatureMaps.get(key);
		if (map!= null && !map.isEmpty()) {
			for (String signId : map.keySet()) {
				return signId;
			}
		}
		return "";
	}

	public Map<String, Map<String, String>> getTemplateContentMaps() {
		return templateContentMaps;
	}

	public void setTemplateContentMaps(
			Map<String, Map<String, String>> templateContentMaps) {
		this.templateContentMaps = templateContentMaps;
	}

	public static void main(String[] args) {
		String rawValue = "cutt\\.ly/swk4Q0sp|JP|walrnart|Arnazon|AMAZ0NR|AMAZ0N|Apple|Activewear|suspended|IG|Amazon|AMAZON|AMAZOM|HairTuber|sueldo";
		Pattern pattern = Pattern.compile(rawValue);
		
		Matcher matcher = null;
		String content ="cutt.ly/swk4Q0spWelcom you to test the Yunpian Short Message Service, test verification code 888888, if not your own operation, please ignore this message. ";
		matcher = pattern.matcher(content);
		if (matcher != null && matcher.find()) {
			String strRawPattern = pattern.pattern();
			
			System.out.println(matcher.toMatchResult().group());
		}else{
			System.out.println("no");
		}
		
	}
	
	
}
