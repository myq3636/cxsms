package com.king.gmms.util.prefix;

import com.king.message.gmms.GmmsMessage;

import java.util.LinkedList;

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
public class RemovePrefix
    extends Prefix {
    private static final String sSperater = ",";
    private LinkedList<String> lRemoveValue = new LinkedList<String> ();

    public RemovePrefix(int aim) {
        super(aim);
    }

    /**
     * handl
     *
     * @param gmmsMsg GmmsMessage
     * @todo Implement this prefix.Prefix method
     */
    public void handle(String[] address) {
//
//        if(gmmsMsg == null)
//            return;
        for (int i = 0; i < this.lRemoveValue.size(); i++) {
            String str = (String) lRemoveValue.get(i);
            if (this.removePrefix(address, str)) {
                break;
            }
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
        boolean bResult = false;


        String[] temp = value.split(sSperater);

        for(int i = 0 ; i < temp.length ; i++){
            this.lRemoveValue.add(temp[i].trim());
        }

        bResult = true;
        return bResult;
    }

    private boolean removePrefix(String[] address,String prefix) {

        String addr = this.getAddress(address);
        if (addr != null && !prefix.equals("") && addr.startsWith(prefix)) {
            addr = addr.substring(prefix.length());
            this.setAddress(address, addr);
            return true;
        }

        return false;
    }

    public String toString() {

    	String prefix = ";";
        StringBuffer buf = new StringBuffer();
        buf.append("{iOperation: remove");
        buf.append(prefix);
        buf.append("iAim:" + this.iAim);
        buf.append(prefix);

        buf.append("remove value:");
        for (int i = 0; i < this.lRemoveValue.size(); i++) {
            buf.append((String)this.lRemoveValue.get(i) + ",");
        }
        buf.append("}");

        return buf.toString();

    }
}
