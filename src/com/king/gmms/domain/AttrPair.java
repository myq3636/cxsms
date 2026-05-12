package com.king.gmms.domain;

import com.king.framework.SystemLogger;

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
public class AttrPair {
    private static SystemLogger log = SystemLogger.getSystemLogger(AttrPair.class);
    String item = null;
    String value = null;
    public AttrPair(String item, String value) {
        this.item = item;
        this.value = value;
    }

    public AttrPair() {

    }

    public String getItem() {
        return item;
    }

    public String getStringValue() {
        return getStringValue(null);
    }

    public String getStringValue(String defaultValue) {
        if(value == null)
            return defaultValue;

        return value;
    }


    public void setValue(String value){
        this.value = value;
    }

    public boolean getBoolValue() {
        return "yes".equalsIgnoreCase(value) ||
            "true".equalsIgnoreCase(value) ||
            "enable".equalsIgnoreCase(value);
    }

    public int getIntValue() {
        if (value == null)
            return A2PCustomerConfig.INVAILED_INTEGER;
        try {
            int intValue = Integer.parseInt(value);
            return intValue;
        }
        catch (NumberFormatException ex) {
            log.warn(ex, ex);
            return A2PCustomerConfig.INVAILED_INTEGER;
        }
    }

    public boolean parse(String conf) {
        int idx = conf.indexOf("=");
        if (idx < 0) {
            item="";
            value=conf.trim();
            return true;
        }
        item = conf.substring(0, idx);
        if(item != null) item = item.trim();
        value = conf.substring(idx + 1, conf.length());
        if(value != null) value = value.trim();

        if (item != null && item.length() > 0 && value != null) {
            return true;
        }
        return false;
    }

	@Override
	public String toString() {
		return "AttrPair [item=" + item + ", value=" + value + "]";
	}
    
    
}
