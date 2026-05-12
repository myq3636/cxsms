package com.king.gmms.routing;


import org.xbill.DNS.*;

import java.net.URL;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Observable;
import java.util.Observer;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.session.HttpSession;
import com.king.message.gmms.GmmsMessage;
import com.king.gmms.domain.EnumberDTO;

/**
 * <p>
 * Title: DNSClient
 * </p>
 * <p>
 * Description: This class manage to query operator's information by querying
 * DNS with a number
 * </p>
 * <p>
 * Copyright: Copyright (c) 2001-2010
 * </p>
 * <p>
 * Company: King.Inc
 * </p>
 * 
 * @version 6.1
 * @author: Sam Hao
 */
public class DNSClient extends HttpSession{
	// MNPSolution to operate querying
	private MNPSolution mnp;
	// Logger ot write log
	private static SystemLogger log = SystemLogger.getSystemLogger(DNSClient.class);
	private Resolver resolver;
    private boolean isASY;
    private int dnsTimeout = 20;
    private ADSServerMonitor dnsMonitor = null;
	/**
	 * Constructor
	 */
	public DNSClient(boolean isASY) {
		GmmsUtility gmmsUtility = GmmsUtility.getInstance();
        this.isASY=isASY;
        try {
        	dnsMonitor = ADSServerMonitor.getInstance();
        	String dnsAddress = gmmsUtility.getCommonProperty("DNSAddress", "172.31.216.49");
        	int port = Integer.parseInt(gmmsUtility.getCommonProperty("DNSPort", "53"));
            if(dnsAddress == null || port <= 0 )
            {
                throw new UnknownHostException("No definition of DNS in property file");
            }
            
            dnsTimeout = gmmsUtility.getDNSTimeout();
            
            initResolver(dnsAddress,port);
        }
        catch(Exception ex) {
            log.error("Exception in initialize() of DNSClient", ex);
        }
	}
    
    /**
     * init resolver
     */
    private void initResolver(String dnsServerAddress, int port){
    	try {
			resolver = new SimpleResolver(dnsServerAddress,isASY);
            resolver.setPort(port);
            resolver.setTimeout(dnsTimeout);
            Lookup.setDefaultResolver(resolver);
            mnp = new MNPSolution();
    	}
    	catch(Exception e){
    		log.info("initResolver error:",e);
    	}
    }
    
	/**
	 * Translate a number (12345) to a string 5.4.3.2.1.e164.gprs. or
	 * 5.4.3.2.1.e164.local.
	 * 
	 * @param number
	 *            String
	 * @param suffix
	 *            String
	 * @return StringBuffer
	 * @throws Exception
	 */
	private StringBuilder numberToName(String number, String suffix)
			throws Exception {
		StringBuilder e164 = new StringBuilder(100);
		for (int i = number.length() - 1; i >= 0; i--) {
			e164.append(number.charAt(i)).append('.');
		}
		e164.append("e164.");
		e164.append(suffix);
		e164.append(".");
		return e164;
	}

	public List<String[]> getAsyQueryResult(List msgs) {
		List results = new ArrayList();
		List<String[]> res = null;
		try {
			mnp.getAsyQueryRes(msgs, results, resolver);

			res = handleAsyQueryResult(results);
		} catch (Exception ex) {
			StringWriter writer = new StringWriter();
			ex.printStackTrace(new PrintWriter(writer, true));
			log.warn(writer.toString());
		}
		results.clear();

		return res;
	}

	private List<String[]> handleAsyQueryResult(List results) {

		ArrayList<String[]> list = new ArrayList<String[]>();

		for (Object obj : results) {
			HashMap map = (HashMap) obj;
			String[] mncmcc = null;
			Iterator it = map.keySet().iterator();
			int resultCode = (Integer) it.next();
			/*
			 * 1: lookup failed due to a data or server error. Repeating the
			 * lookup would not be helpful 2: lookup failed due to a network
			 * error 3: The host does not exist 4: The host exists, but has no
			 * records associated with the queried type. 5: doing asy query next
			 * time try to get result.
			 */
			if (resultCode == 0) {
				Record[] answers = null;
				answers = (Record[]) map.get(resultCode);

				String mnc = null; // mnc info
				String mcc = null; // mcc info
				String mno = null; // mno info
				for (Record answer : answers) {
					if (answer.rdataToString().indexOf("mno") > -1) {
						mno = answer.rdataToString();
					}
				}

				for (int i = 0; i < answers.length; i++) {
					answers[i] = null;
				}
				answers = null;
				map.clear();

				if (mno == null) {
					mncmcc = new String[] { "-5", "-5" };
				} else { // have mno info
					int mncIndex = mno.indexOf("mnc");
					int mccIndex = mno.indexOf("mcc");
					mnc = mno.substring(mncIndex + 3, mno
							.indexOf(".", mncIndex));
					mcc = mno.substring(mccIndex + 3, mno
							.indexOf("!", mccIndex));
					if(log.isDebugEnabled()){
						log.debug("mnc:{};{}", mnc, mcc);
					}
					mncmcc = new String[] { mnc, mcc };
				}
			} else {
				if(log.isInfoEnabled()){
					log.info("Asyn query result code is {} and error message is {}",
						resultCode, mnp.getErrorMessage());
				}
				mncmcc = new String[] { String.valueOf(-resultCode),
						String.valueOf(-resultCode) };
			}
			list.add(mncmcc);
		}// end of loop for

		return list;
	}

	public String[] asyQueryMncMcc(String phoneNumber, String suffix,
			GmmsMessage msg) {
		try {
			HashMap dnsResp = mnp.asyQuery(numberToName(phoneNumber, suffix)
					.toString(), "NAPTR", msg, resolver);
			Iterator it = dnsResp.keySet().iterator();
			int resultCode = (Integer) it.next();
			/*
			 * 1: lookup failed due to a data or server error. Repeating the
			 * lookup would not be helpful 2: lookup failed due to a network
			 * error 3: The host does not exist 4: The host exists, but has no
			 * records associated with the queried type. 5: doing asy query next
			 * time try to get result.
			 */
			if (resultCode == 0) {
				Record[] answers = null;
				answers = (Record[]) dnsResp.get(resultCode);

				String mnc = null; // mnc info
				String mcc = null; // mcc info
				String mno = null; // mno info
				for (Record answer : answers) {
					if (answer.rdataToString().indexOf("mno") > -1) {
						mno = answer.rdataToString();
					}
				}

				for (int i = 0; i < answers.length; i++) {
					answers[i] = null;
				}
				answers = null;
				dnsResp.clear();

				if (mno == null) {
					return new String[] { "-5", "-5" };
				} else { // have mno info
					int mncIndex = mno.indexOf("mnc");
					int mccIndex = mno.indexOf("mcc");
					mnc = mno.substring(mncIndex + 3, mno
							.indexOf(".", mncIndex));
					mcc = mno.substring(mccIndex + 3, mno
							.indexOf("!", mccIndex));
					return new String[] { mnc, mcc };
				}
			} else {
				log.info(new StringBuilder(msg.getMsgID()).append(" ").append(
						mnp.getErrorMessage()).append(" for the number: ")
						.append(phoneNumber).toString());
				return new String[] { String.valueOf(-resultCode),
						String.valueOf(-resultCode) };
			}
		} catch (Exception e) {
			log.error(e, e);
			return new String[] { "-1", "-1" };
		}
	}
	
	public String[] queryMncMcc(String phoneNumber, String suffix) {
		try {
			HashMap dnsResp = mnp.query(numberToName(phoneNumber, suffix)
					.toString(), "NAPTR");
			Iterator it = dnsResp.keySet().iterator();
			int resultCode = (Integer) it.next();
			/*
			 * 1: lookup failed due to a data or server error. Repeating the
			 * lookup would not be helpful 2: lookup failed due to a network
			 * error 3: The host does not exist 4: The host exists, but has no
			 * records associated with the queried type.
			 */
			if (resultCode == 0) {
				Record[] answers = null;
				answers = (Record[]) dnsResp.get(resultCode);

				String mnc = null; // mnc info
				String mcc = null; // mcc info
				String mno = null; // mno info
				for (Record answer : answers) {
					if (answer.rdataToString().indexOf("mno") > -1) {
						mno = answer.rdataToString();
					}
				}
				if (mno == null) {
					return new String[] { "-5", "-5" };
				} else { // have mno info
					int mncIndex = mno.indexOf("mnc");
					int mccIndex = mno.indexOf("mcc");
					mnc = mno.substring(mncIndex + 3, mno
							.indexOf(".", mncIndex));
					mcc = mno.substring(mccIndex + 3, mno
							.indexOf("!", mccIndex));
					return new String[] { mnc, mcc };
				}
			} else {
				if(log.isInfoEnabled()){
					log.info("{} for the number: {}", mnp.getErrorMessage(),
						phoneNumber);
				}
				return new String[] { String.valueOf(-resultCode),
						String.valueOf(-resultCode) };
			}
		} catch (Exception e) {
			log.error(e, e);
			return new String[] { "-1", "-1" };
		}
	}

	/**
	 * 
	 * @param phoneNumber
	 *            String
	 * @param suffix
	 *            String
	 * @return [mnc, mcc]
	 */
	public EnumberDTO queryIR21(GmmsMessage msg,String phoneNumber, String suffix) {
		try {
			HashMap dnsResp = mnp.asyQuery(numberToName(phoneNumber, suffix)
					.toString(), "NAPTR", msg, resolver);
			Iterator it = dnsResp.keySet().iterator();
			int resultCode = (Integer) it.next();
			/*
			 * 1: lookup failed due to a data or server error. Repeating the
			 * lookup would not be helpful 2: lookup failed due to a network
			 * error 3: The host does not exist 4: The host exists, but has no
			 * records associated with the queried type.
			 * 0.1.9.6.3.1.6.8.e164.arpa. 
			 * 3600 IN NAPTR 100 100 "u" "E2U+a2p" "!^.*$!a2p:86136@cc86.opid106.mnc000.mcc460.gprs!"
			 */
			log.debug(msg, " IR21 query enum response {}", resultCode);
			if (resultCode == 0) {
				Record[] answers = null;
				answers = (Record[]) dnsResp.get(resultCode);

				String cc = null; // mnc info
				String opid = null; // mcc info
				String mno = null; // mno info
				for (Record answer : answers) {
					if (answer.rdataToString().indexOf("opid") > -1) {
						mno = answer.rdataToString();
					}
				}
				if (mno == null) {
					return null;
				} else { // have mno info
					int mncIndex = mno.indexOf("@cc");
					int mccIndex = mno.indexOf("opid");
					cc = mno.substring(mncIndex + 3, mno
							.indexOf("opid", mncIndex));
					opid = mno.substring(mccIndex + 4, mno
							.indexOf("mnc", mccIndex));
					EnumberDTO dto = new EnumberDTO();               
                 	dto.setNumber(phoneNumber);                 	
  					dto.setOpId(Integer.parseInt(opid));
  					dto.setPrefix(Integer.parseInt(cc));
  					dto.setQueryMethod("IR21");
  					return dto;
				}
			} else {
				if(log.isInfoEnabled()){
					log.info("{} for the number: {}", mnp.getErrorMessage(),
						phoneNumber);
				}
				return null;
			}
		} catch (Exception e) {
			log.error(e, e);
			return null;
		}
	}
	
	public EnumberDTO queryIR21ByHost(GmmsMessage msg,String phoneNumber, String suffix, String serverHost) {
		try {			
			/*
			 * 1: lookup failed due to a data or server error. Repeating the
			 * lookup would not be helpful 2: lookup failed due to a network
			 * error 3: The host does not exist 4: The host exists, but has no
			 * records associated with the queried type.
			 * 0.1.9.6.3.1.6.8.e164.arpa. 
			 * 3600 IN NAPTR 100 100 "u" "E2U+a2p" "!^.*$!a2p:86136@cc86.opid106.mnc000.mcc460.gprs!"
			 */
			
			String mno = this.digRecord(serverHost, "NAPTR", numberToName(phoneNumber, suffix)
					.toString());
			    String cc = null;
			    String opid= null;							
				if (mno == null) {
					return null;
				} else { // have mno info
					int mncIndex = mno.indexOf("@cc");
					int mccIndex = mno.indexOf("opid");
					cc = mno.substring(mncIndex + 3, mno
							.indexOf("opid", mncIndex)-1);
					opid = mno.substring(mccIndex + 4, mno
							.indexOf("mnc", mccIndex)-1);					
					EnumberDTO dto = new EnumberDTO();               
                 	dto.setNumber(phoneNumber);                 	
  					dto.setOpId(Integer.parseInt(opid));
  					dto.setPrefix(Integer.parseInt(cc));
  					dto.setQueryMethod("IR21");
  					return dto;
				}
			
		} catch (Exception e) {
			log.error(e, e);
			return null;
		}
	}
	
	public static String digRecord(String nameServer, String recordType, String record) throws Exception {

        String cmd;
        //dig @172.31.216.49 -t naptr  2.3.3.0.7.1.9.e164.arpa
        cmd = "dig @" + nameServer + " -t " + recordType + " "+record;
        log.debug("dig ir21 msg request: {}", cmd);
        StringBuilder builder = new StringBuilder();
        try {
            Process process = Runtime.getRuntime().exec(cmd);
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));            
            String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line).append("\n");
            }
            
        } catch (IOException e) {
            log.error("dig ir21 msg throw exception", e);           
        }
        log.debug("dig ir21 msg response: {}", builder.toString());
        String result = builder.toString();        
        String[] records = result.split(";");
        //去除两端的空白
        for (int i = 0; i < records.length; i++) {
            if(records[i].contains("ANSWER SECTION")) {
            	return records[i];
            }
        }
        return null;
    }
	
	
	public EnumberDTO queryMNC(GmmsMessage msg, String enumAddress) throws Exception{		
		URL url = new URL(enumAddress);
		JSONObject jsonParam = new JSONObject();  
    	jsonParam.put("number", msg.getRecipientAddress());
    	String post = jsonParam.toJSONString();
		log.debug("http post mothed send url: {}", url.toString());
		String resp = super.doMNPquery(url, post, null,"application/json",false);
		JSONObject json = JSON.parseObject(resp);      
		log.debug(msg, " query enum response {}", resp);
        if (json.get("data")!=null &&"SUCCESS".equalsIgnoreCase(String.valueOf(json.get("errmsg")))) {
        	JSONArray data = (JSONArray)json.get("data");
        	for(int i=0; i<data.size(); i++){
        		Map enumMap = (Map)data.get(i);                	
             	for( Object key: enumMap.keySet()){
             		EnumberDTO dto = new EnumberDTO();               
                 	dto.setNumber(String.valueOf(key));                	                 
  					dto.setOperator(String.valueOf(((Map)enumMap.get(key)).get("operator")));
  					dto.setType(String.valueOf(((Map)enumMap.get(key)).get("type")));
  					dto.setOpId(Integer.parseInt(String.valueOf(((Map)enumMap.get(key)).get("operator_id"))));
  					dto.setPrefix(Integer.parseInt(String.valueOf(((Map)enumMap.get(key)).get("country_code"))));
  					dto.setQueryMethod("realEnumquery");
  					dto.setMccmnc(String.valueOf(((Map)enumMap.get(key)).get("mcc"))+""+String.valueOf(((Map)enumMap.get(key)).get("mnc")));
  					return dto;
             	}   
        	}
         }
        return null;
	}
	

	@Override
	public boolean submit(GmmsMessage msg) throws IOException {
		// TODO Auto-generated method stub
		
		return false;
	}

	@Override
	public ByteBuffer submitAndRec(GmmsMessage msg) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected StringBuffer appendData(GmmsMessage message) {
		// TODO Auto-generated method stub
		return null;
	}
}
