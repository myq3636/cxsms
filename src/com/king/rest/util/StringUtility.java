package com.king.rest.util;

import java.text.SimpleDateFormat;
import java.util.Date;

public class StringUtility {
	
	
	public static boolean stringIsNotEmpty(String str)
	{
		if(str==null||str.trim().length()==0)
			return false;
		else
			return true;
	}
	
	public static String formatDate(Date date)
	{
		if(date==null)
		{
			return null;
		}
		SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMddHHmmss");
		String str=sdf.format(date);
		return str;
	}

}
