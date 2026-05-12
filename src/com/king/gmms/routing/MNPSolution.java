package com.king.gmms.routing;

import java.util.HashMap;
import java.util.List;

import org.xbill.DNS.Record;
import org.xbill.DNS.Resolver;
import org.xbill.DNS.Type;
import org.xbill.DNS.Lookup;

import com.king.message.gmms.GmmsMessage;

public class MNPSolution {

	public void getAsyQueryRes(List msgs, List results, Resolver resolver) {
		// TODO Auto-generated method stub
		
	}

	public Object getErrorMessage() {
		// TODO Auto-generated method stub
		return null;
	}

	public HashMap asyQuery(String name, String type, GmmsMessage obj, Resolver resolver) throws Exception{
		int qtype = verifyQueryType(type);
	    HashMap<Object, Object> map = null;
	    Lookup lookup = new Lookup(name, qtype);
	    lookup.setResolver(resolver);
	    Record[] answers = lookup.asyRun(obj);	    
	    map = new HashMap<Object, Object>();
	    map.put(new Integer(lookup.getResult()), answers);
	    return map;
	}
	
	private int verifyQueryType(String type) throws IllegalArgumentException {
	    int qtype = Type.value(type);
	    if (qtype < 0)
	      throw new IllegalArgumentException("\ninvalid type: " + type + 
	          "\n-----------------------------\n" + 
	          "Available types include: A NS MD MF CNAME SOA MB MG MR NULL WKS PTR HINFO" + 
	          "\nMINFO MX TXT NAPTR ... Please check the API document" + 
	          "\n-----------------------------"); 
	    return qtype;
	  }

	public HashMap query(String string, String string2) {
		// TODO Auto-generated method stub
		return null;
	}

}
