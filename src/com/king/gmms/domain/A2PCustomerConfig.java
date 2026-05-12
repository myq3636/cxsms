package com.king.gmms.domain;

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;





import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.king.framework.SystemLogger;
import com.king.gmms.Constant;

public class A2PCustomerConfig implements Comparable{

	private static SystemLogger log = SystemLogger
			.getSystemLogger(A2PCustomerConfig.class);
	public static final int INVAILED_INTEGER = 0x80000000;
	public static final String BLOCK_KEY = "ShortName";
	public static final String DEFAULTE_ROUTING_INFO_KEY = "default";
	public static final String ALL_ROUTING_INFO_KEY = "all";
	public static final String ATTR_SEP = ":";
	public static final String VALUE_SEP = ",";
	public static final String GROUP_SEP = ";";
	public static final String SUB_VALUE_SEP = ";";
	public static final String REGEX_VALUE_SEP = "\\|";
	public static final String GROUP_START = "(";
	public static final String GROUP_END = ")";
	private String primaryKey = null;
	private int ssid = -1;
	private String shortName = null;
	private HashMap<String, String> lines = new HashMap<String, String>();

	public A2PCustomerConfig() {
		super();
	}

	public String getShortName() {
		return shortName;
	}

	public int getSSID() {
		return ssid;
	}

	public String getPrimaryKey() {
		return primaryKey;
	}

	public void setShortName(String shortName) {
		this.shortName = shortName;
	}

	public void setSsid(int ssid) {
		this.ssid = ssid;
	}

	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}

	public void addConf(String name, String value) {
		if (name == null || value == null || name.length() == 0
				|| value.length() == 0) {
			return;
		}
		if (isConfigured(name)) {// same name in one block.
			String oriValue = this.getConfValue(name);
			if (oriValue.endsWith(GROUP_END))
				value = oriValue + this.GROUP_SEP + value;
			else
				value = oriValue + this.VALUE_SEP + value;
		}
		this.lines.put(name.trim().toLowerCase(), value);
	}

	/**
	 * Check if the specified item is configured. This method will not check the
	 * value.
	 * 
	 * @param name
	 *            String Item name, case insensitive.
	 * @return boolean true if the item is configured, otherwise false.
	 */
	public boolean isConfigured(String name) {
		return lines.containsKey(name.trim().toLowerCase());
	}

	/**
	 * Get the configured String value.
	 * 
	 * @param name
	 *            String Item name, case insensitive
	 * @param defaultValue
	 *            String Default value. If the value is not configured, this
	 *            value will be returned
	 * @return String Configured value or default value if the item is not
	 *         configured.
	 */
	public String getConfValue(String name, String defaultValue) {
		if (name == null || name.length() == 0)
			return defaultValue;
		String value = lines.get(name.trim().toLowerCase());
		if (value == null)
			return defaultValue;
		return value;
	}

	/**
	 * Same with getConfValue(name, null)
	 * 
	 * @param name
	 *            String Item name, case insensitive
	 * @return String Configured value or null if the item is not configured.
	 */
	public String getConfValue(String name) {
		return this.getConfValue(name, null);
	}

	public String getString(String name) {
		return getConfValue(name);
	}

	public String getString(String name, String defaultValue) {
		return getConfValue(name, defaultValue);
	}

	/**
	 * Get the int value of configuration. If item is not configured, or
	 * configured value can not be parsed into integer, this method will return
	 * then defalut value.
	 * 
	 * @param name
	 *            String Item name, case insensitive.
	 * @param defaultValue
	 *            int Default value
	 * @return int Configured value or defaultValue if the item is not
	 *         configured or error happens when parsing the String into integer.
	 */
	public int getInt(String name, int defaultValue) {
		String strValue = this.getConfValue(name);
		if (strValue == null)
			return defaultValue;
		try {
			int value = Integer.parseInt(strValue);
			return value;
		} catch (NumberFormatException ex) {
			log.warn("Wrong number format for item {}, value: {}", name,
					strValue);
			return defaultValue;
		}
	}

	/**
	 * Get the byte value of configuration. If item is not configured, or
	 * configured value can not be parsed into integer, this method will return
	 * then defalut value.
	 * 
	 * @param name
	 *            String Item name, case insensitive.
	 * @param defaultValue
	 *            byte Default value
	 * @return byte Configured value or defaultValue if the item is not
	 *         configured or error happens when parsing the String into integer.
	 */
	public byte getByteWithHexFormat(String name, byte defaultValue) {
		String strValue = this.getConfValue(name);
		if (strValue == null)
			return defaultValue;
		try {
			byte value = Byte.parseByte(strValue, 16);
			return value;
		} catch (NumberFormatException ex) {
			log.warn("Wrong number format for item {}, value: {}", name,
					strValue);
			return defaultValue;
		}
	}

	/**
	 * Get the int value of specified item. This method is same with
	 * getIntConfValue(name, INVAILED_INTEGER).
	 * 
	 * INVAILED_INTEGER is a huge negative integer.
	 * 
	 * @param name
	 *            String item name. case insensitive.
	 * @return int
	 */
	public int getInt(String name) {
		return this.getInt(name, INVAILED_INTEGER);
	}

	public String getMandatoryConfValue(String name)
			throws CustomerConfigurationException {
		String value = this.getConfValue(name);
		if (value == null)
			throw new CustomerConfigurationException(
					"Missing mandatory config item " + name + ". Customer: "
							+ this.getShortName());
		return value;
	}

	public String getMandatoryString(String name)
			throws CustomerConfigurationException {
		return this.getMandatoryConfValue(name);
	}

	public int getMandatoryInt(String name)
			throws CustomerConfigurationException {
		String strVal = this.getMandatoryConfValue(name);
		try {
			int value = Integer.parseInt(strVal);
			return value;
		} catch (NumberFormatException ex) {
			throw new CustomerConfigurationException(
					"Wrong config value for mandatory item " + name
							+ " of customer " + this.getShortName()
							+ ". Must be a Integer");
		}

	}

	public boolean getMandatoryBool(String name)
			throws CustomerConfigurationException {
		if (!isConfigured(name)) {
			throw new CustomerConfigurationException(
					"Missing mandatory config item " + name + " for cutomer "
							+ this.getShortName() + ".");
		}
		return getBool(name, false);
	}

	public boolean getBool(String name, boolean defaultValue) {
		String strValue = this.getConfValue(name);
		if (strValue == null)
			return defaultValue;
		if (defaultValue == true) {
			if (strValue.equalsIgnoreCase("no")
					|| strValue.equalsIgnoreCase("false")
					|| strValue.equalsIgnoreCase("disable"))
				return false;
			return true;
		} else {
			if (strValue.equalsIgnoreCase("yes")
					|| strValue.equalsIgnoreCase("true")
					|| strValue.equalsIgnoreCase("enable"))
				return true;
			return false;
		}
	}

	public boolean getBool(String name) {
		return this.getBool(name, false);
	}

	public String toString() {
		StringBuffer strBuf = new StringBuffer();
		Iterator attrIt = lines.keySet().iterator();
		while (attrIt.hasNext()) {
			String attr = (String) attrIt.next();
			String value = lines.get(attr);
			strBuf.append(attr).append(":").append(value).append("\n");
		}
		return strBuf.toString();
	}

	public static ArrayList<A2PCustomerConfig> parse(File file, String charset)
			throws CustomerConfigurationException {
		ArrayList<A2PCustomerConfig> blockList = new ArrayList<A2PCustomerConfig>();
		try {
			FileInputStream fin = new FileInputStream(file);
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					fin, charset));
			A2PCustomerConfig oneBlock = null;
			String line = null;
			int lineNum = 0;
			while ((line = reader.readLine()) != null) {
				// 1. remove the addtional write space
				lineNum++;
				line = line.trim();
				// 2. Ignore the comment line and empty line.
				if (line.startsWith("#") || line.length() == 0) {
					continue;
				}

				// 3. [ShortName] is the start of one block
				// Create a new block and add it into the list.
				if (line.startsWith("[") && line.endsWith("]")) {
					oneBlock = new A2PCustomerConfig();
					try {
						String shortName = line.substring(1, line.length() - 1);
						if ("".equals(shortName.trim())) {// added by Jianming
															// in v1.0.1
							throw new Exception("Empty shortName!");
						}
						oneBlock.addConf(BLOCK_KEY, shortName.trim());
						oneBlock.setShortName(shortName.trim());
						blockList.add(oneBlock);
					} catch (Exception e) {// added by Jianming in v1.0.1
						oneBlock = null;
					}
				}
				// 4. Fill the block configuration.
				else {
					if (oneBlock == null) {
						log
								.error(
										"Configuration file {} format error. Missing first block name. Line: {}, {}",
										file.getName(), lineNum, line);
						continue;// modified by Jianming in v1.0.1
					}
					int sepIndex = line.indexOf(ATTR_SEP);
					if (sepIndex <= 0) {
						log
								.warn(
										"Wrong configuration {}. Missing Attribute Seperator. Line: {}",
										line, lineNum);
						continue;
					}
					String attribute = line.substring(0, sepIndex).trim();
					String value = line.substring(sepIndex + 1, line.length())
							.trim();
					oneBlock.addConf(attribute, value);
				}
			}
			return blockList;
		} catch (FileNotFoundException ex) {
			log.error(ex, ex);
			return null;
		} catch (IOException ex) {
			log.error(ex, ex);
			return null;
		}

	}
	
	public static List<RecipientAddressRule>  parseRecipientRuleInfo(File path)
			throws Exception {
		FileReader fileReader = new FileReader(path);
        Reader reader = new InputStreamReader(new FileInputStream(path), "Utf-8");
        int ch = 0;
        StringBuffer sb = new StringBuffer();
        while ((ch = reader.read()) != -1) {
            sb.append((char) ch);
        }
        fileReader.close();
        reader.close();
        String jsonStr = sb.toString();
        //如果是json是非数组类型的情况使用下面的转化
        JSONArray jsona =(JSONArray)JSONArray.parse(jsonStr);
        List<RecipientAddressRule> list = JSONObject.parseArray(jsona.toJSONString(), RecipientAddressRule.class);
        log.info("load recipientRuleFile.cfg info list: {}", list);
        return list;
	}

	public static Map<String,A2PCustomerConfig>  parseRoutingInfo(File path)
			throws CustomerConfigurationException {
		Map<String,A2PCustomerConfig> blockMap = new HashMap<String,A2PCustomerConfig>();
		FileInputStream fin = null;
		BufferedReader reader = null;		
		try {
			File[] files = path.listFiles();
			if(files != null) {
				for(File file: files) {
					if(file.isDirectory()){
						continue;
					}
					fin = new FileInputStream(file);
					reader = new BufferedReader(new InputStreamReader(fin, "utf-8"));
					A2PCustomerConfig oneBlock = new A2PCustomerConfig();;
					String line = null;
					String fileName = file.getName();
					if (!fileName.endsWith(Constant.ROUTING_SUFFER_FEX)) {
						continue;
					}
					int index = fileName.indexOf(Constant.ROUTING_SUFFER_FEX);
					String shortName = fileName.substring(0, index).trim();
					oneBlock.setShortName(shortName);
					int lineNum = 0;
					while ((line = reader.readLine()) != null) {
						// 1. remove the addtional write space
						lineNum++;
						line = line.trim();
						// 2. Ignore the comment line and empty line.
						if (line.startsWith("#") || line.length() == 0) {
							continue;
						}

						int sepIndex = line.indexOf(ATTR_SEP);
						if (sepIndex <= 0) {
							log
									.warn(
											"Wrong confi guration {}. Missing Attribute Seperator. Line: {}",
											line, lineNum);
							continue;
						}
						String attribute = line.substring(0, sepIndex).trim();
						String value = line.substring(sepIndex + 1, line.length())
								.trim();
						oneBlock.addConf(attribute, value);
					} 
					blockMap.put(shortName,oneBlock);
					
			  }				
				return blockMap;
			}
		}catch (Exception e) {
			e.printStackTrace();			
			blockMap.clear();
		}finally{
			try {
				if(reader!=null){
					reader.close();
				}
				if(fin!=null){
					fin.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
		return blockMap;	
	}
	
	public static Map<String, Map<String, List<String>>>  parseSenderActionDir(File path)
			throws Exception {		
		FileInputStream fin = null;
		BufferedReader reader = null;	
		boolean defaultFileExisted = false;
		try {
			Map<String, Map<String, List<String>>> ssidCCSenderBlList = new HashMap<>();
			File[] files = path.listFiles();
			if(files != null) {
				for(File file: files) {
					if(file.isDirectory()){
						continue;
					}
					
					fin = new FileInputStream(file);
					reader = new BufferedReader(new InputStreamReader(fin, "utf-8"));
					String line = null;
					String fileName = file.getName();
					if(("-1"+Constant.SENDER_BLACK_SUFFER_FEX).equalsIgnoreCase(fileName)){
						defaultFileExisted = true;
					}
					if (fileName.endsWith(Constant.SENDER_BLACK_SUFFER_FEX)) {
						int index = fileName.indexOf(Constant.SENDER_BLACK_SUFFER_FEX);
						String ssidName = fileName.substring(0, index).trim();
						Map<String, List<String>> ccBL = ssidCCSenderBlList.get(ssidName);
						if(ccBL == null){
							ccBL = new HashMap<>();
							ssidCCSenderBlList.put(ssidName, ccBL);
						}
						int lineNum = 0;
						while ((line = reader.readLine()) != null) {
							// 1. remove the addtional write space
							lineNum++;
							line = line.trim();
							// 2. Ignore the comment line and empty line.
							if (line.startsWith("#") || line.length() == 0) {
								continue;
							}

							int sepIndex = line.indexOf(ATTR_SEP);
							if (sepIndex <= 0) {
								log.warn("Wrong confi guration {}. Missing Attribute Seperator. Line: {}, filename: {}",
												line, lineNum, fileName);
								continue;
							}
							String attribute = line.substring(0, sepIndex).trim();
							String value = line.substring(sepIndex + 1, line.length())
									.trim();
							if(value == null || value.equalsIgnoreCase("")){
								continue;
							}							
							//value = value.replaceAll(" ", "");
							List<String> bl = ccBL.get(attribute);
							if(bl == null){
								bl = new ArrayList<>();
								ccBL.put(attribute, bl);
							}
							bl.add(value);
						} 						
					}										
			  }											   
			}
			
			if(!defaultFileExisted){
				log.error("no default blacklist file existed, so ignore this load blacklist action");
				return null;
			}
			return ssidCCSenderBlList;
		}catch (Exception e) {
			log.error("do load sender blacklist file error", e);
			e.printStackTrace();			
			throw e;
		}finally{
			try {
				if(reader!=null){
					reader.close();
				}
				if(fin!=null){
					fin.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
			
	}
	
	public static Map<String, Set<String>>  parseRecipientActionDir(File path)
			throws Exception {		
		FileInputStream fin = null;
		BufferedReader reader = null;	
		try {
			Map<String, Set<String>> ssidCCSenderBlList = new HashMap<>();
			File[] files = path.listFiles();
			if(files != null) {
				for(File file: files) {
					if(file.isDirectory()){
						continue;
					}					
					fin = new FileInputStream(file);
					reader = new BufferedReader(new InputStreamReader(fin, "utf-8"));
					String line = null;
					String fileName = file.getName();
					if (fileName.endsWith(Constant.RECIPIENT_BLACK_SUFFER_FEX)) {
						int index = fileName.indexOf(Constant.RECIPIENT_BLACK_SUFFER_FEX);
						String ssidName = fileName.substring(0, index).trim();
						Set<String> ccBL = ssidCCSenderBlList.get(ssidName);
						if(ccBL == null){
							ccBL = new HashSet<>();
							ssidCCSenderBlList.put(ssidName, ccBL);
						}
						while ((line = reader.readLine()) != null) {
							// 1. remove the addtional write space
							line = line.trim();
							// 2. Ignore the comment line and empty line.
							if (line.startsWith("#") || line.length() == 0) {
								continue;
							}
							
							if(line == null || line.equalsIgnoreCase("")){
								continue;
							}							
							line = line.replaceAll(" ", "");							
							ccBL.add(line);
						} 						
					}										
			  }											   
			}
			return ssidCCSenderBlList;
		}catch (Exception e) {
			log.error("do load recipient blacklist file error", e);
			e.printStackTrace();			
			throw e;
		}finally{
			try {
				if(reader!=null){
					reader.close();
				}
				if(fin!=null){
					fin.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
			
	}
	
	public static Map<String, Map<String, Integer>>  parseVendorReplacement(File file)
			throws Exception {		
		FileInputStream fin = null;
		BufferedReader reader = null;	
		
		try {
			//<country,<ossid,rssid>>
			Map<String, Map<String, Integer>> ccVenderReplacementMap = new HashMap<>();			
			if(file.exists()) {
				if(file.isDirectory()){
					return ccVenderReplacementMap;
				}
				fin = new FileInputStream(file);
				reader = new BufferedReader(new InputStreamReader(fin, "utf-8"));
				String line = null;
				String fileName = file.getName();	
				int lineNum = 0;
				if (Constant.ROUTING_VENDOR_REPLACEMENT.equalsIgnoreCase(fileName)) {					
					while ((line = reader.readLine()) != null) {
						// 1. remove the addtional write space
						lineNum++;
						line = line.trim();
						// 2. Ignore the comment line and empty line.
						if (line.startsWith("#") || line.length() == 0) {
							continue;
						}
						String[] lineArr = line.split(ATTR_SEP);
						if (lineArr == null || lineArr.length <= 3) {
							log.warn("Wrong confi guration {}. Missing Attribute Seperator. Line: {}, filename: {}",
											line, lineNum, fileName);
							continue;
						}				
						Map<String, Integer> routingMap = ccVenderReplacementMap.get(lineArr[0].trim());
						if(routingMap == null){
							routingMap = new HashMap<>();
							ccVenderReplacementMap.put(lineArr[0].trim(), routingMap);
						}
						routingMap.put(lineArr[1].trim()+":"+lineArr[2].trim(), Integer.parseInt(lineArr[3].trim()));
					} 						
				}										
		  											   
			}
			return ccVenderReplacementMap;
		}catch (Exception e) {
			log.error("do load vendor replacement file error", e);
			e.printStackTrace();			
			throw e;
		}finally{
			try {
				if(reader!=null){
					reader.close();
				}
				if(fin!=null){
					fin.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
			
	}
	
	public static Map<String, Map<String, List<String>>>  parseContentActionDir(File path, String suffer)
			throws Exception {		
		FileInputStream fin = null;
		BufferedReader reader = null;	
		
		try {
			Map<String, Map<String, List<String>>> ssidCCSenderBlList = new HashMap<>();
			File[] files = path.listFiles();
			if(files != null) {
				for(File file: files) {
					if(file.isDirectory()){
						continue;
					}
					fin = new FileInputStream(file);
					reader = new BufferedReader(new InputStreamReader(fin, "utf-8"));
					String line = null;
					String fileName = file.getName();
					if (fileName.endsWith(suffer)) {
						int index = fileName.indexOf(suffer);
						String ssidName = fileName.substring(0, index).trim();
						Map<String, List<String>> ccBL = ssidCCSenderBlList.get(ssidName);
						if(ccBL == null){
							ccBL = new HashMap<>();
							ssidCCSenderBlList.put(ssidName, ccBL);
						}
						int lineNum = 0;
						while ((line = reader.readLine()) != null) {
							// 1. remove the addtional write space
							lineNum++;
							line = line.trim();
							// 2. Ignore the comment line and empty line.
							if (line.startsWith("#") || line.length() == 0) {
								continue;
							}

							int sepIndex = line.indexOf(ATTR_SEP);
							if (sepIndex <= 0) {
								log.warn("Wrong confi guration {}. Missing Attribute Seperator. Line: {}, filename: {}",
												line, lineNum, fileName);
								continue;
							}
							String attribute = line.substring(0, sepIndex).trim();
							String value = line.substring(sepIndex + 1, line.length())
									.trim();
							if(value == null || value.equalsIgnoreCase("")){
								continue;
							}							
							//value = value.replaceAll(" ", "");
							List<String> bl = ccBL.get(attribute);
							if(bl == null){
								bl = new ArrayList<>();
								ccBL.put(attribute, bl);
							}
							bl.add(value);
						} 						
					}										
			  }											   
			}
			return ssidCCSenderBlList;
		}catch (Exception e) {
			e.printStackTrace();			
			throw e;
		}finally{
			try {
				if(reader!=null){
					reader.close();
				}
				if(fin!=null){
					fin.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
			
	}
	
	public static Map<String, Map<String, List<String>>>  parseWhiteSenderActionDir(File path)
			throws Exception {		
		FileInputStream fin = null;
		BufferedReader reader = null;	
		
		try {
			//<ssid,<wl_cc,<sender:wl:cc:rpsender>>>
			Map<String, Map<String, List<String>>> ssidCCSenderBlList = new HashMap<>();
			File[] files = path.listFiles();
			boolean defaultFileExisted = false;
			if(files != null) {
				for(File file: files) {
					if(file.isDirectory()){
						continue;
					}
					fin = new FileInputStream(file);
					reader = new BufferedReader(new InputStreamReader(fin, "utf-8"));
					String line = null;
					String fileName = file.getName();
					if(("-1"+Constant.SENDER_BLACK_SUFFER_FEX).equalsIgnoreCase(fileName)){
						defaultFileExisted = true;
					}
					if (fileName.endsWith(Constant.SENDER_WHITLE_SUFFER_FEX)) {
						int index = fileName.indexOf(Constant.SENDER_WHITLE_SUFFER_FEX);
						String ssidName = fileName.substring(0, index).trim();
						Map<String, List<String>> ccBL = ssidCCSenderBlList.get(ssidName);
						if(ccBL == null){
							ccBL = new HashMap<>();
							ssidCCSenderBlList.put(ssidName, ccBL);
						}
						int lineNum = 0;
						while ((line = reader.readLine()) != null) {
							// 1. remove the addtional write space
							lineNum++;
							line = line.trim();
							// 2. Ignore the comment line and empty line.
							if (line.startsWith("#") || line.length() == 0) {
								continue;
							}

							String[] lineArr = line.split(ATTR_SEP);
							if (lineArr == null || lineArr.length <= 3) {
								log.warn("Wrong confi guration {}. Missing Attribute Seperator. Line: {}, filename: {}",
												line, lineNum, fileName);
								continue;
							}														
							List<String> bl = ccBL.get(lineArr[1].trim()+"_"+lineArr[0].trim());
							if(bl == null){
								bl = new ArrayList<>();
								ccBL.put(lineArr[1].trim()+"_"+lineArr[0].trim(), bl);
							}
							bl.add(lineArr[2].trim()+":"+lineArr[1].trim()+":"+lineArr[0].trim()+":"+lineArr[3].trim());
						} 						
					}										
			  }											   
			}
			if(!defaultFileExisted){
				log.error("no default whitelist file existed, so ignore this load whitelist action");
				return null;
			}
			return ssidCCSenderBlList;
		}catch (Exception e) {
			log.error("do load sender whitelist file error", e);
			e.printStackTrace();			
			throw e;
		}finally{
			try {
				if(reader!=null){
					reader.close();
				}
				if(fin!=null){
					fin.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
			
	}
	
	public static Map<String,A2PCustomerConfig>  parse(File file)
			throws CustomerConfigurationException {
		Map<String,A2PCustomerConfig> blockMap = new HashMap<String,A2PCustomerConfig>();
		FileInputStream fin = null;
		BufferedReader reader = null;
		try {
			fin = new FileInputStream(file);
			reader = new BufferedReader(new InputStreamReader(fin, "utf-8"));
			A2PCustomerConfig oneBlock = null;
			String line = null;
			int lineNum = 0;
			while ((line = reader.readLine()) != null) {
				// 1. remove the addtional write space
				lineNum++;
				line = line.trim();
				// 2. Ignore the comment line and empty line.
				if (line.startsWith("#") || line.length() == 0) {
					continue;
				}

				// 3. [ShortName] is the start of one block
				// Create a new block and add it into the list.
				if (line.startsWith("[") && line.endsWith("]")) {
					oneBlock = new A2PCustomerConfig();
					try {
						String shortName = line.substring(1, line.length() - 1);
						if ("".equals(shortName.trim())) {// added by Jianming
															// in v1.0.1
							throw new Exception("Empty shortName!");
						}
						oneBlock.addConf(BLOCK_KEY, shortName.trim());
						oneBlock.setShortName(shortName.trim());
						blockMap.put(shortName,oneBlock);
						continue;
					} catch (Exception e) {// added by Jianming in v1.0.1
						oneBlock = null;
					}
				}
				// 4. Fill the block configuration.
				else {
					if (oneBlock == null) {
						log
								.error(
										"Configuration file {} format error. Missing first block name. Line: {}",
										file.getName(), lineNum);
						continue;// modified by Jianming in v1.0.1
					}
					
				}
				int sepIndex = line.indexOf(ATTR_SEP);
				if (sepIndex <= 0) {
					log
							.warn(
									"Wrong configuration {}. Missing Attribute Seperator. Line: {}",
									line, lineNum);
					continue;
				}
				String attribute = line.substring(0, sepIndex).trim();
				String value = line.substring(sepIndex + 1, line.length())
						.trim();
				oneBlock.addConf(attribute, value);
			}
			reader.close();
			fin.close();
			
			Map<String,A2PCustomerConfig> temp = new HashMap<String,A2PCustomerConfig>();
			temp.putAll(blockMap);
			
			for (Map.Entry<String, A2PCustomerConfig> entry: temp.entrySet()) {
				A2PCustomerConfig a2pConfig = entry.getValue();
				if (a2pConfig.getBool("DisableCustomer", false)) {
					log.info("the DisableCustomer key is:{}", entry.getKey());
					blockMap.remove(entry.getKey());
				}				
			}
			return blockMap;
		} catch (Exception e) {
			e.printStackTrace();			
			return null;
		}finally{
			try {
				if(reader!=null){
					reader.close();
				}
				if(fin!=null){
					fin.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
	}

	// Find the last "_" and cut off service_type sufix
	public String getShortNameWithoutService() {
		if (this.shortName == null)
			return null;

		int c = shortName.lastIndexOf("_");
		if (c > 0)
			return shortName.substring(0, c);
		else
			return shortName;
	}

	public A2PCustomerMultiValue parseMultiValue(String name) {
		A2PCustomerMultiValue value = new A2PCustomerMultiValue();
		if (value.parse(this.getConfValue(name)))
			return value;
		return null;

	}
	
	public A2PCustomerMultiValue parseRoutingMultiValue(String name) {
		A2PCustomerMultiValue value = new A2PCustomerMultiValue();
		if (value.parseRouting(this.getConfValue(name)))
			return value;
		return null;

	}
	
	/**
	 * 0: no changed
	 * 1:items changed
	 * 2:shortName changed
	 */
	public int compareTo(Object o) {
		A2PCustomerConfig cust = (A2PCustomerConfig)o;
		if(this.shortName.equals(cust.shortName)){
			HashMap<String, String>  olines = cust.getLines();
			if(olines!=null && this.lines.size()!=olines.size()){
				return 1;
			}else{
				for(String key:lines.keySet()){
					String value = lines.get(key);
					String ovalue = olines.get(key);
					if(!value.equals(ovalue)){
						return 1;
					}
				}
			}
			return 0;
		}else{
			return 2;
		}
	}
	@Override
	public boolean equals(Object obj) {
		A2PCustomerConfig cust = (A2PCustomerConfig)obj;
		return this.shortName.equals(cust.getShortName());
	}

	@Override
	public int hashCode() {
		return shortName.hashCode()*31;
	}

	public HashMap<String, String> getLines() {
		return lines;
	}
}
