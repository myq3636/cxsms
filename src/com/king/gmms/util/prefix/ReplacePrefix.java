package com.king.gmms.util.prefix;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.message.gmms.GmmsMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Random;

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * <p>Copyright: Copyright (c) 2006</p>
 *
 * <p>Company: </p>
 *
 * @author not attributable
 * @version 1.0
 */
public class ReplacePrefix
    extends Prefix {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(A2PCustomerInfo.class);
    private static final String sSperater = ",";
    private static final String sGroup = "\\|";
    private static final String sRandomPrefix = "&";
    private static final String sRandomSpe = "%";
    private Map<String, String> mReplaceValue = new HashMap<String, String> ();
    private Random random = new Random();
    //sender: List<>(1:前缀开始*后缀，2：前缀结束)
    public Map<String, List<Long>> mValue = new HashMap<String, List<Long>> ();
    public ReplacePrefix(int aim) {
        super(aim);
    }

    /**
     * handl
     *
     * @param gmmsMsg GmmsMessage
     * @todo Implement this prefix.Prefix method
     */
    public void handle(String[] address) {

//        if(gmmsMsg == null)
//            return;

        Iterator iterator = this.mReplaceValue.keySet().iterator();
        boolean isAllKey = false;
        boolean isReplace = false;
        while(iterator.hasNext()){
            String str = (String)iterator.next();
            if("All".equalsIgnoreCase(str)){
            	isAllKey = true;
            	continue;
            }
            String value = this.mReplaceValue.get(str);
            if(replacePrefix(address,str,value)){
            	isReplace=true;
                break;
            }
        }
        if(isReplace){
        	return;
        }
        if(isAllKey){
        	String value = this.mReplaceValue.get("all");
        	replacePrefix(address,"all",value);
        }
    }

    /**
     * parseValue
     *
     * @param value String
     * @return boolean
     * @todo Implement this prefix.Prefix method
     */
    public boolean parseValue(String value) {

        if(value == null || value.equals(""))
            return false;

        boolean bResult  = false;

        String[] group = value.split(this.sGroup);
        for(int i = 0 ; i < group.length ; i++){
            String[] temp = group[i].split(this.sSperater);
            if(temp.length != 2){
                continue;
            }
            String replace = temp[1].trim();
            String key = temp[0].trim();
            this.mReplaceValue.put(key,replace);
            if (replace.contains(sRandomPrefix)) {
            	//1586666-1586669%4 & 1586651-1586665%4
            	List<Long> replaceList = mValue.get(key);
            	if (replaceList == null) {
            		replaceList = new ArrayList<Long>();
					mValue.put(key, replaceList);
				}
            	String[] replaces  = replace.split(sRandomPrefix);
            	for (int j = 0; j < replaces.length; j++) {
            		//1586666-1586669%4
    				if (replaces[j].contains(sRandomSpe)) {
    					String suffer = replaces[j].split(sRandomSpe)[1];
                    	String prefix = replaces[j].split(sRandomSpe)[0];
                    	//1586666-1586669
                    	String start = prefix;
                    	String end = prefix;
                    	if (prefix.contains("-")) {
                    		start = prefix.split("-")[0];
        					end = prefix.split("-")[1];
						}                    	
                    	for (int k = 0; k < Integer.parseInt(suffer); k++) {
                    		start+="0";
                    		end+="9";
						}
                    	replaceList.add(Long.parseLong(start));
                    	replaceList.add(Long.parseLong(end));
    				}
				}            									
			}
        }

        if(!this.mReplaceValue.isEmpty()){
        	for(Map.Entry<String, String> entry: mReplaceValue.entrySet()){
        		log.info("SMSOptionReplaceOriPrefix4NewMT: key:{},value:{}",entry.getKey(), entry.getValue());
        	}
        	
        	for(Map.Entry<String, List<Long>> entry: mValue.entrySet()){
        		log.info("SMSOptionReplaceOriPrefix4NewMT: key:{},value:{}",entry.getKey(), entry.getValue());
        	}
            bResult = true;
        }

        return bResult;

    }

    private boolean replacePrefix(String[] address, String prefix, String replace){
        String addr = this.getAddress(address);
        if("All".equalsIgnoreCase(prefix)){ //added in A2P v1.1.2 to replace all,Jianming
        	//and random replace
        	if(!mValue.isEmpty() && mValue.get(prefix) != null 
            		&& !mValue.get(prefix).isEmpty()){
            	String random = doRandomReplaceNumber(prefix);
            	if (random != null) {
            		this.setAddress(address,random);
                    return true;
				}
            }
        	
            this.setAddress(address,replace);
            return true;
        }        
        if (addr != null && prefix != null) {
        	//and random replace
            if(addr.startsWith(prefix) && !mValue.isEmpty() && mValue.get(prefix) != null 
            		&& !mValue.get(prefix).isEmpty()){
            	String random = doRandomReplaceNumber(prefix);
            	if (random != null) {
            		this.setAddress(address,random);
                    return true;
				}
            }
        	if(prefix.endsWith("*") && prefix.length()>1 && addr.startsWith(prefix.substring(0, prefix.length()-1))){
        		this.setAddress(address,replace);
                return true;
        	}
        	if("".equalsIgnoreCase(addr)&& "".equalsIgnoreCase(prefix)) {
        		this.setAddress(address,replace);
                return true;
        	}
        	if(addr.startsWith(prefix)){
        		addr = replace + addr.substring(prefix.length());
                this.setAddress(address,addr);
                return true;
        	}
            
        }
        if(addr == null && "".equalsIgnoreCase(prefix)) {
        	this.setAddress(address,replace);
            return true;
        }

        return false;
    }

	private String doRandomReplaceNumber(String prefix) {
		List<Long> perfixList = mValue.get(prefix);
		String replace = null;
		if (perfixList != null && !perfixList.isEmpty()) {
			long start = 0L;
			long end = 0L;
			if (perfixList.size() == 2) {
				start = perfixList.get(0);
				end = perfixList.get(1);
			}else {
				int listIndex = random.nextInt(perfixList.size());
				if (listIndex%2 != 0) {
					listIndex--;
				}
				start = perfixList.get(listIndex);
				end = perfixList.get(listIndex+1);
			}
			long suffer= 0;
			if (end-start>0) {
				if (end-start>=Integer.MAX_VALUE) {
					long inteval = end-start;
					String inteval1 = String.valueOf(inteval).substring(0, 8);					
					String inteval2 = String.valueOf(inteval).substring(8);
					int suffer1 = random.nextInt(Integer.parseInt(inteval1));
					int suffer2 = random.nextInt(Integer.parseInt(inteval2));
					suffer = Long.parseLong(suffer1+""+suffer2);
				}else {
					suffer=random.nextInt(Integer.parseInt((end-start)+""));
				}
				
			}
			 
			replace = String.valueOf(start+suffer);
		}
		return replace;
	}

    public String toString() {
    	String prefix = ";";
        StringBuffer buf = new StringBuffer();
        buf.append("{iOperation: replace");
        buf.append(prefix);
        buf.append("iAim:" + this.iAim);
        buf.append(prefix);
        buf.append("replace value:");
        Iterator iterator = this.mReplaceValue.keySet().iterator();
        while (iterator.hasNext()) {
            String str = (String) iterator.next();
            String value = this.mReplaceValue.get(str);
            buf.append(str + "," + value + "|");
        }
        buf.append("}");
        return buf.toString();
    }
    
    public static void main(String[] args) {
		ReplacePrefix prefix = new ReplacePrefix(1);
		prefix.parseValue("all,628110-628119%10&628121-628129%10&628160-628169%10&62817-62829%10");
		Map<String, List<Long>> map = prefix.mValue;
		for (Map.Entry<String, List<Long>> entry : map.entrySet()) {
			System.out.println(entry.getKey());
			System.out.println(entry.getValue());
		}		
		String[] address = {"12142","134444"};
		
		prefix.replacePrefix(address, "all", "567");
		System.out.println(address[0]);
		Random random = new Random();
		for (int i = 1000; i < 1010; i++) {
			System.out.println("first:"+random.nextInt(i));
			int j = i-1;
			System.out.println("second:"+random.nextInt(j));
		}
		
		long inteval = 89999999999L;
		String inteval1 = String.valueOf(inteval).substring(0, 8);					
		String inteval2 = String.valueOf(inteval).substring(8);
		System.out.println(inteval1+":"+inteval2);
		int suffer1 = random.nextInt(Integer.parseInt(inteval1));
		int suffer2 = random.nextInt(Integer.parseInt(inteval2));
		long suffer = Long.parseLong(suffer1+""+suffer2);
		System.out.println(suffer);
		String t="abc||tcd";
		System.out.println(t.split("\\|").length);
		String s = "*";
		String ad = "abdd";
		System.out.println(ad.startsWith(s.substring(0, s.length()-1)));
	}
}
