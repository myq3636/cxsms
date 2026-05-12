/**
 * Copyright 2000-2013 King Inc. All rights reserved.
 */
package com.king.gmms.protocol.smpp.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import com.king.framework.SystemLogger;


/**
 * SMPP validityPeriod time formatter
 * refer to SMPP Protocol Specification v3.4 point 7.1.1.
 * Refer to jsmpp.
 * 
 */
public class TimeFormatter {
	
	private static SystemLogger log = SystemLogger.getSystemLogger(TimeFormatter.class);

    /**
     * Format GMT/UTC date to SMPP AbsoluteTimeFormat
     * @param gmtDate
     * @return
     * @see com.king.gmms.protocol.smpp.util.TimeFormatter#toAbsoluteFormat(java.util.Date)
     */
    public static String toAbsoluteFormat(Date gmtDate) {
        if (gmtDate == null) {
            return null;
        }
        
        try {
        	SimpleDateFormat formatter = new SimpleDateFormat("yyMMddHHmmss");
    		// since already GMT time, can hard-coded for nnp as "00+"
    		return (formatter.format(gmtDate) + "000+");
        } catch (Exception e) {
        	log.warn(e, e);
        	return null;
        }
    }
    
    /**
     * @param timeStr
     * @param a2pTz
     * @return
     * @throws Exception 
     */
    public static Date parse(String timeStr, TimeZone a2pTz) throws Exception {
    	if (timeStr == null || timeStr.trim().length()<1) {
            return null;
        }

    	try {
    		int year = Integer.parseInt(timeStr.substring(0, 2));
            int month = Integer.parseInt(timeStr.substring(2, 4));
            int day = Integer.parseInt(timeStr.substring(4, 6));
            int hour = Integer.parseInt(timeStr.substring(6, 8));
            int minute = Integer.parseInt(timeStr.substring(8, 10));
            int second = Integer.parseInt(timeStr.substring(10, 12));
            int tenthsOfSecond = Integer.parseInt(timeStr.substring(12, 13));
            int  nn = Integer.parseInt(timeStr.substring(13, 15));
            String p = timeStr.substring(15, 16);
            
            if ("+".equals(p) || "-".equals(p)) { // absolute format 
            	// parse as customer local time
            	String tzStr = "GMT" + p + nn / 4;
            	if ((nn %4)!=0) {
            		tzStr = tzStr + ":" + (nn %4) * 15;
            	}
            	TimeZone custTz = TimeZone.getTimeZone(tzStr);
            	
            	Calendar custCalendar = Calendar.getInstance(custTz);
                custCalendar.set(year + 2000, month - 1, day, hour, minute, second);
                custCalendar.set(Calendar.MILLISECOND, tenthsOfSecond * 100);
                
                return custCalendar.getTime();
            } else if ("R".equals(p)) { // relative format
            	Calendar a2pCalendar = Calendar.getInstance(a2pTz);
            	a2pCalendar.add(Calendar.YEAR, year);
            	a2pCalendar.add(Calendar.MONTH, month);
            	a2pCalendar.add(Calendar.DAY_OF_MONTH, day);
            	a2pCalendar.add(Calendar.HOUR_OF_DAY, hour);
            	a2pCalendar.add(Calendar.MINUTE, minute);
            	a2pCalendar.add(Calendar.SECOND, second);
            	
                return a2pCalendar.getTime();
            } 
            
    	} catch (Exception e) {
    		throw new Exception("parse SMPP time string error");
    		
    	}
    	return null;
        
    }
    
    public static Date parse(String timeStr) throws Exception {
    	if (timeStr == null || timeStr.trim().length()<1) {
            return null;
        }

    	try {
    		int year = Integer.parseInt(timeStr.substring(0, 4));
            int month = Integer.parseInt(timeStr.substring(5, 7));
            int day = Integer.parseInt(timeStr.substring(8, 10));
            int hour = Integer.parseInt(timeStr.substring(11, 13));
            int minute = Integer.parseInt(timeStr.substring(14, 16));
            int second = Integer.parseInt(timeStr.substring(17, 19));
            String tzString = timeStr.substring(19, timeStr.length());
            if(!"GMT".equals(tzString.trim())) {
            	int  nn = Integer.parseInt(tzString);
                String p = timeStr.substring(19, 20);                
                if ("+".equals(p) || "-".equals(p)) { // absolute format 
                	// parse as customer local time
                	String tzStr = "GMT" + nn;
                	TimeZone custTz = TimeZone.getTimeZone(tzStr);
                	
                	Calendar custCalendar = Calendar.getInstance(custTz);
                    custCalendar.set(year, month, day, hour, minute, second);                
                    return custCalendar.getTime();
                }
            }else{
            	TimeZone custTz = TimeZone.getTimeZone("GMT");            	
            	Calendar custCalendar = Calendar.getInstance(custTz);
                custCalendar.set(year, month, day, hour, minute, second);                
                return custCalendar.getTime();
			}
            
    	} catch (Exception e) {
    		throw new Exception("parse Http time string error");
    		
    	}
    	return null;
        
    }

}
