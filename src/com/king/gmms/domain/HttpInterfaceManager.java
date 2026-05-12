package com.king.gmms.domain;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.http.HttpConstants;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpParam;
import com.king.gmms.domain.http.HttpPdu;
import com.king.gmms.protocol.commonhttp.CommonDeliveryReportHttpHandler;
import com.king.gmms.protocol.commonhttp.CommonMessageHttpHandler;
import com.king.gmms.protocol.commonhttp.HttpCharset;
import com.king.gmms.protocol.commonhttp.HttpStatus;
import com.king.gmms.protocol.commonhttp.HttpUtils;
import com.king.message.gmms.GmmsStatus;

public class HttpInterfaceManager {
	private static SystemLogger log = SystemLogger.getSystemLogger(HttpInterfaceManager.class);
	private Map<String, HttpInterface> httpInterfaceMap = new ConcurrentHashMap<String, HttpInterface>();
	/**
	 * init httpInterfaceMap
	 * @param interfaceName
	 */
	public void initHttpInterfaceMap(String interfaceName) {
		if (httpInterfaceMap.get(interfaceName) != null) {
			return;
		}
		synchronized (httpInterfaceMap) {
			if (httpInterfaceMap.get(interfaceName) != null) {
				return;
			}
			try {
				String interffaceConfig = System.getProperty("a2p_home") + "conf/" + interfaceName + ".xml";
//				 String interffaceConfig ="E:\\Workspaces\\A2PNewGeneration\\conf\\" + interfaceName + ".xml";
				SAXReader saxReader = new SAXReader();
				Document doc = saxReader.read(interffaceConfig);
				Element interfaceEl = doc.getRootElement();
				HttpInterface hi = new HttpInterface();
				hi.setInterfaceName(interfaceName);
				Element username = this.getElementByName(interfaceEl, HttpConstants.ELEM_TYPE_USERNAME);
				if(username != null){
					hi.setUsername(username.getTextTrim());
				}
				
				Element password = this.getElementByName(interfaceEl, HttpConstants.ELEM_TYPE_PASSWORD);
				if(password != null){
					hi.setPassword(password.getTextTrim());
				}
				//init charset mapping
				Element charsetEl = this.getElementByName(interfaceEl, HttpConstants.ELEM_TYPE_CHARSETMAPPING);
				initCharsetMap(charsetEl, hi);
				
				//init status mapping
				Element statusEl = this.getElementByName(interfaceEl, HttpConstants.ELEM_TYPE_STATUSMAPPING);
				initStatusMap(statusEl, hi);
				
				Element moEl = this.getElementByName(interfaceEl, HttpConstants.ELEM_TYPE_MO);
				initElement4MO(moEl,hi);
				
				Element mtEl = this.getElementByName(interfaceEl, HttpConstants.ELEM_TYPE_MT);
				initElement4MT(mtEl,hi);
				//init handler class last, for parameter hi ready
				this.initHandlerClass(hi);
				httpInterfaceMap.put(interfaceName, hi);
			} catch (DocumentException e) {
				log.error("init httpinterface " + interfaceName + " conf file error!!!",e);
				e.printStackTrace();
			}
		}

	}
	/**
	 * init for MO tag
	 * @param me
	 * @param hi
	 */
	private void initElement4MO(Element me, HttpInterface hi){
		if(me == null){
			return;
		}
		Element submitE =  this.getElementByName(me,HttpConstants.ELEM_TYPE_SUBMITREQUEST);
		initSubElement(submitE,hi,HttpConstants.MO_SUBMIT_REQUEST);
		
		Element submitrspE =  this.getElementByName(me,HttpConstants.ELEM_TYPE_SUBMITRESPONSE);
		initSubElement(submitrspE,hi,HttpConstants.MO_SUBMIT_RESPONSE);
		
		Element drE =  this.getElementByName(me,HttpConstants.ELEM_TYPE_DRREQUEST);
		initSubElement(drE,hi,HttpConstants.MO_DR_REQUEST);
		
		Element drrespE =  this.getElementByName(me,HttpConstants.ELEM_TYPE_DRRESPONSE);
		initSubElement(drrespE,hi,HttpConstants.MO_DR_RESPONSE);
	}
	
	/**
	 * init element for MT tag
	 * @param me
	 * @param hi
	 */
	private void initElement4MT(Element me, HttpInterface hi){
		Element submitE =  this.getElementByName(me,HttpConstants.ELEM_TYPE_SUBMITREQUEST);
		initSubElement(submitE,hi,HttpConstants.MT_SUBMIT_REQUEST);
		
		Element submitrspE =  this.getElementByName(me,HttpConstants.ELEM_TYPE_SUBMITRESPONSE);
		initSubElement(submitrspE,hi,HttpConstants.MT_SUBMIT_RESPONSE);
		
		Element drE =  this.getElementByName(me,HttpConstants.ELEM_TYPE_DRREQUEST);
		initSubElement(drE,hi,HttpConstants.MT_DR_REQUEST);
		
		Element drrespE =  this.getElementByName(me,HttpConstants.ELEM_TYPE_DRRESPONSE);
		initSubElement(drrespE,hi,HttpConstants.MT_DR_RESPONSE);
	}
	/**
	 * init sub element of MT/MO
	 * @param submitEl
	 * @param hi
	 * @param type
	 */
	private void initSubElement(Element submitEl, HttpInterface hi, String type) {
		if(submitEl == null){
			return;
		}
		
		Iterator itInner = submitEl.elementIterator();
		
		while (itInner.hasNext()) {
			HttpParam hp = new HttpParam();
			Element sub = (Element) itInner.next();
			try{
				String param = sub.getName();
				String oparam = sub.getTextTrim();
				if(HttpConstants.ELEM_TYPE_HANDlERCLASS.equalsIgnoreCase(param)){//init HandlerClass Map
					this.setHandlerClass(hi, type, oparam);
					continue;
				}else if(HttpConstants.ELEM_TYPE_RESPONSEDELIMITER.equalsIgnoreCase(param)){//init response delimiter
					oparam = HttpUtils.unEscapeSpecialChars(oparam);
					if(!"".equals(oparam) && !" ".equals(oparam)){
						this.setResponseDelimiter(hi, type, oparam);
					}
					continue;
				}else if(HttpConstants.ELEM_TYPE_PARAMETERDELIMITER.equalsIgnoreCase(param)){//init response parameter delimiter
					oparam = HttpUtils.unEscapeSpecialChars(oparam);
					if(!"".equals(oparam) && !" ".equals(oparam)){
						this.setParameterDelimiter(hi, type, oparam);
					}
					continue;
				}
				hp.setParam(param);
				hp.setOppsiteParam(oparam);
				hp.setDefaultValue(this.getStringAttrValue(sub, HttpConstants.ELEM_ATTR_VALUE));
				hp.setEncoding(this.getStringAttrValue(sub, HttpConstants.ELEM_ATTR_ENCODING));
				hp.setFormat(this.getStringAttrValue(sub, HttpConstants.ELEM_ATTR_FORMAT));
				hp.setType(this.getStringAttrValue(sub, HttpConstants.ELEM_ATTR_TYPE));
				
				//add by kevin 
				hp.setCcbName(this.getStringAttrValue(sub, HttpConstants.ELEM_ATTR_TAYPE_CCB_NAME));
				
				if (type.equalsIgnoreCase(HttpConstants.MO_SUBMIT_REQUEST)) {
					hi.getMoSubmitRequest().add(hp);
				} else if (type.equalsIgnoreCase(HttpConstants.MO_SUBMIT_RESPONSE)) {
					hi.getMoSubmitResponse().add(hp);
				} else if (type.equalsIgnoreCase(HttpConstants.MO_DR_REQUEST)) {
					hi.getMoDRRequest().add(hp);
				} else if (type.equalsIgnoreCase(HttpConstants.MO_DR_RESPONSE)) {
					hi.getMoDRResponse().add(hp);
				} else if (type.equalsIgnoreCase(HttpConstants.MT_SUBMIT_REQUEST)) {
					hi.getMtSubmitRequest().add(hp);
				} else if (type.equalsIgnoreCase(HttpConstants.MT_SUBMIT_RESPONSE)) {
					hi.getMtSubmitResponse().add(hp);
				} else if (type.equalsIgnoreCase(HttpConstants.MT_DR_REQUEST)) {
					hi.getMtDRRequest().add(hp);
				} else if (type.equalsIgnoreCase(HttpConstants.MT_DR_RESPONSE)) {
					hi.getMtDRResponse().add(hp);
				}
			}catch(Exception e){
				log.error("Init element "+sub.getPath()+" failed!",e);
				e.printStackTrace();
				continue;
			}
		}
	}
	
	/**
	 * init data coding map
	 * @param submitEl
	 * @param hi
	 * @param type
	 */
	private void initCharsetMap(Element el, HttpInterface hi) {
		if(el == null||hi == null){
			return;
		}
		
		Iterator itInner = el.elementIterator();
		Map map = hi.getCharsetMap();
		while (itInner.hasNext()) {
			Element sub = (Element) itInner.next();
			try{
				String mtype = sub.getName(); //<mtype value="charset">gmmsCharset</mtype>
				String gmmsCharset = sub.getTextTrim();
				String charset = this.getStringAttrValue(sub, HttpConstants.ELEM_ATTR_VALUE);
				if(null == charset){
					charset = mtype;
				}
				HttpCharset httpCharset = new HttpCharset(charset,mtype);
				map.put(httpCharset, gmmsCharset);
			}catch(Exception e){
				log.error("Init element "+sub.getPath()+" failed!",e);
				continue;
			}
		}
	}
	/**
	 * init handler class map
	 * @param hi
	 * @param type
	 * @param className
	 */
	private void initHandlerClass(HttpInterface hi){
		Object handler = null;
		Map<String,Object> handlerClassMap = hi.getHandlerClassMap();

		Set<String> handlerClassSet = getHandlerClassSet(hi);
		//add configured handler classes
		for(String className : handlerClassSet){
			try{
				Class clazz = Class.forName(className);
				if(!handlerClassMap.containsKey(className)){
					Constructor c = clazz.getConstructor(new Class[]{HttpInterface.class});
					Object[] constructors = {hi};
					handler = c.newInstance(constructors);
					handlerClassMap.put(className, handler);
				}
			}catch(Exception e){
				log.fatal("initHandlerClass exception with className:"+className,e);
				e.printStackTrace();
				continue;
			}
		}
		//add common handler classes
		if(!handlerClassMap.containsKey(HttpConstants.COMMON_MSG_HANDLERCLASS)){
			CommonMessageHttpHandler commonMessageHttpHandler = new CommonMessageHttpHandler(hi);
			handlerClassMap.put(HttpConstants.COMMON_MSG_HANDLERCLASS, commonMessageHttpHandler);
		}
		
		if(!handlerClassMap.containsKey(HttpConstants.COMMON_DR_HANDLERCLASS)){
			CommonDeliveryReportHttpHandler commonDeliveryReportHttpHandler = new CommonDeliveryReportHttpHandler(hi);
			handlerClassMap.put(HttpConstants.COMMON_DR_HANDLERCLASS, commonDeliveryReportHttpHandler);
		}
	}
	/**
	 * set value for HandlerClass
	 * @param hi
	 * @param type
	 * @param oparam
	 */
	private void setHandlerClass(HttpInterface hi, String type, String oparam){
		if (type.equalsIgnoreCase(HttpConstants.MO_SUBMIT_REQUEST)) {
			hi.getMoSubmitRequest().setHandlerClass(oparam);
		} else if (type.equalsIgnoreCase(HttpConstants.MO_SUBMIT_RESPONSE)) {
			hi.getMoSubmitResponse().setHandlerClass(oparam);
		} else if (type.equalsIgnoreCase(HttpConstants.MO_DR_REQUEST)) {
			hi.getMoDRRequest().setHandlerClass(oparam);
		} else if (type.equalsIgnoreCase(HttpConstants.MO_DR_RESPONSE)) {
			hi.getMoDRResponse().setHandlerClass(oparam);
		} else if (type.equalsIgnoreCase(HttpConstants.MT_SUBMIT_REQUEST)) {
			hi.getMtSubmitRequest().setHandlerClass(oparam);
		} else if (type.equalsIgnoreCase(HttpConstants.MT_SUBMIT_RESPONSE)) {
			hi.getMtSubmitResponse().setHandlerClass(oparam);
		} else if (type.equalsIgnoreCase(HttpConstants.MT_DR_REQUEST)) {
			hi.getMtDRRequest().setHandlerClass(oparam);
		} else if (type.equalsIgnoreCase(HttpConstants.MT_DR_RESPONSE)) {
			hi.getMtDRResponse().setHandlerClass(oparam);
		}
	}
	/**
	 * set value for ResponseDelimiter
	 * @param hi
	 * @param type
	 * @param oparam
	 */
	private void setResponseDelimiter(HttpInterface hi, String type, String oparam){
		if (type.equalsIgnoreCase(HttpConstants.MO_SUBMIT_REQUEST)) {
			hi.getMoSubmitRequest().setResponseDelimiter(oparam);
		} else if (type.equalsIgnoreCase(HttpConstants.MO_SUBMIT_RESPONSE)) {
			hi.getMoSubmitResponse().setResponseDelimiter(oparam);
		} else if (type.equalsIgnoreCase(HttpConstants.MO_DR_REQUEST)) {
			hi.getMoDRRequest().setResponseDelimiter(oparam);
		} else if (type.equalsIgnoreCase(HttpConstants.MO_DR_RESPONSE)) {
			hi.getMoDRResponse().setResponseDelimiter(oparam);
		} else if (type.equalsIgnoreCase(HttpConstants.MT_SUBMIT_REQUEST)) {
			hi.getMtSubmitRequest().setResponseDelimiter(oparam);
		} else if (type.equalsIgnoreCase(HttpConstants.MT_SUBMIT_RESPONSE)) {
			hi.getMtSubmitResponse().setResponseDelimiter(oparam);
		} else if (type.equalsIgnoreCase(HttpConstants.MT_DR_REQUEST)) {
			hi.getMtDRRequest().setResponseDelimiter(oparam);
		} else if (type.equalsIgnoreCase(HttpConstants.MT_DR_RESPONSE)) {
			hi.getMtDRResponse().setResponseDelimiter(oparam);
		}
	}
	/**
	 * set value for parameterDelimiter
	 * @param hi
	 * @param type
	 * @param oparam
	 */
	private void setParameterDelimiter(HttpInterface hi, String type, String oparam){
		if (type.equalsIgnoreCase(HttpConstants.MO_SUBMIT_REQUEST)) {
			hi.getMoSubmitRequest().setParameterDelimiter(oparam);
		} else if (type.equalsIgnoreCase(HttpConstants.MO_SUBMIT_RESPONSE)) {
			hi.getMoSubmitResponse().setParameterDelimiter(oparam);
		} else if (type.equalsIgnoreCase(HttpConstants.MO_DR_REQUEST)) {
			hi.getMoDRRequest().setParameterDelimiter(oparam);
		} else if (type.equalsIgnoreCase(HttpConstants.MO_DR_RESPONSE)) {
			hi.getMoDRResponse().setParameterDelimiter(oparam);
		} else if (type.equalsIgnoreCase(HttpConstants.MT_SUBMIT_REQUEST)) {
			hi.getMtSubmitRequest().setParameterDelimiter(oparam);
		} else if (type.equalsIgnoreCase(HttpConstants.MT_SUBMIT_RESPONSE)) {
			hi.getMtSubmitResponse().setParameterDelimiter(oparam);
		} else if (type.equalsIgnoreCase(HttpConstants.MT_DR_REQUEST)) {
			hi.getMtDRRequest().setParameterDelimiter(oparam);
		} else if (type.equalsIgnoreCase(HttpConstants.MT_DR_RESPONSE)) {
			hi.getMtDRResponse().setParameterDelimiter(oparam);
		}
	}
	/**
	 * get  HandlerClasses
	 * @param hi
	 * @param type
	 * @param oparam
	 */
	private Set getHandlerClassSet(HttpInterface hi){
		Set<String> handlerClassSet = new HashSet<String>();
		List<HttpPdu> pdus = new ArrayList<HttpPdu>();
		pdus.add(hi.getMoSubmitRequest());
		pdus.add(hi.getMoSubmitResponse());
		pdus.add(hi.getMoDRRequest());
		pdus.add(hi.getMoDRResponse());
		pdus.add(hi.getMtSubmitRequest());
		pdus.add(hi.getMtSubmitResponse());
		pdus.add(hi.getMtDRRequest());
		pdus.add(hi.getMtDRResponse());
		for(HttpPdu pdu:pdus){
			if (pdu.hasHandlerClass()){
				String className = pdu.getHandlerClass();
				if(!handlerClassSet.contains(className)){
					handlerClassSet.add(className);
				}
			}
		}
		return handlerClassSet;
	}
	/**
	 * getElementByName
	 * @param el
	 * @param name
	 * @return
	 */
	private Element getElementByName(Element el, String name) {
		try {
			return el.element(name);
		} catch (Exception e) {
			log.error("No such element in config file:"+name,e);
			return null;
		}
	}
	/**
	 * get attribute of xml tag
	 * @param el
	 * @param name
	 * @return
	 */
	private String getStringAttrValue(Element el, String name) {
		try {
			return el.attribute(name).getValue().trim();
		} catch (Exception e) {
//			log.error("Ignore, no such attribute in config file:"+name);
			return null;
		}
	}
	
	/**
	 * init error code map
	 * 
	 * <SUCCESS value="0">
				<code>200</code>
				<text>OK</text>
		</SUCCESS>
	 * @return
	 */
	private void initStatusMap(Element el,HttpInterface hi){
		if(el == null||hi == null){
			return;
		}
		Map<HttpStatus,GmmsStatus> sbMap = hi.getStatusSubmitMap();
		Map<HttpStatus,GmmsStatus> drMap = hi.getStatusDRMap();
		Map<HttpStatus,GmmsStatus> drRespMap = hi.getStatusDRRespMap();
		List list = el.elements();
		if(list!=null){
			for(int i = 0; i<list.size();i++) {			
				Element sub1 = (Element) list.get(i);
			try{				
				String msgTypeTag = sub1.getName();
				List list1 = sub1.elements();
				for(int j = 0; j<list1.size();j++){
					Element sub = (Element) list1.get(j);				
					//log.debug("gmmsStatusText: "+gmmsStatusText+": "+sub.getName());
					String gmmsStatusCode = this.getStringAttrValue(sub, HttpConstants.ELEM_ATTR_VALUE);
					//log.debug("gmmsStatusCode:"+gmmsStatusCode);
					int code = Integer.valueOf(gmmsStatusCode);
					GmmsStatus gmmsStatus = GmmsStatus.getStatus(code);
					HttpStatus httpStatus = getHttpStatus(sub);
					//log.debug("init httpstatus: "+httpStatus+" gmmsstatus: "+gmmsStatus);
					if("SUBMITRESP".equalsIgnoreCase(msgTypeTag)){
						sbMap.put(httpStatus, gmmsStatus);
					}else if("DR".equalsIgnoreCase(msgTypeTag)){
						drMap.put(httpStatus, gmmsStatus);
					}else if("DRRESP".equalsIgnoreCase(msgTypeTag)){
						drRespMap.put(httpStatus, gmmsStatus);
					}
				}
			}catch(Exception e){
				log.error("Init element "+sub1.getPath()+" failed!",e);
				continue;
			}			 
		 }
		}
	}
	/**
	 * get httpstatus by status xml element
	 * @param el
	 * @return
	 */
	private HttpStatus getHttpStatus(Element el){
		Iterator itInner = el.elementIterator();
		String code = null;
		String text = null;
		while (itInner.hasNext()) {
			Element sub = (Element) itInner.next();
			String name = sub.getName();
			if(HttpConstants.ELEM_TYPE_CODE.equalsIgnoreCase(name)){
				String httpStatusCodes  = sub.getTextTrim();
				try{
					code = httpStatusCodes;
				}catch(NumberFormatException e){
					log.error("Invalid code:",e);
					continue;
				}
			}else if(HttpConstants.ELEM_TYPE_TEXT.equalsIgnoreCase(name)){
				text = sub.getTextTrim();
			}
		}	
		if((code!=null)||(null!=text)){
			return new HttpStatus(code,text);
		}
		return null;
	}
	
	public Map<String, HttpInterface> getHttpInterfaceMap() {
		return httpInterfaceMap;
	}

	public void setHttpInterfaceMap(Map<String, HttpInterface> httpInterfaceMap) {
		this.httpInterfaceMap = httpInterfaceMap;
	}
}
