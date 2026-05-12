package com.king.gmms.protocol.commonhttp.xml;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;





import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.domain.HttpInterfaceManager;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpParam;
import com.king.gmms.protocol.commonhttp.HttpCharset;
import com.king.gmms.protocol.commonhttp.HttpHandler;
import com.king.gmms.protocol.commonhttp.HttpStatus;
import com.king.gmms.protocol.commonhttp.HttpUtils;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageBase;
import com.king.message.gmms.MessageIdGenerator;

public abstract class XmlMessageConverter extends HttpHandler{
	 private static SystemLogger log = SystemLogger.getSystemLogger(XmlMessageConverter.class);
	 private static JAXBContext jaxbContext = null;  
	 private static Marshaller marshaller = null;  
	 private static HttpInterfaceManager him = null;
	 protected static List<String> eleList = null;
	 private static HashMap<String,String> revMap = new HashMap<String,String>();
	 private static String a2phome = "/usr/local/a2p/";
	 private static String charset = "utf8";
	 private static String a2pLibPath = "/usr/local/a2p/Gmms/WEB-INF/lib/";
	 public XmlMessageConverter(HttpInterface hie){
		 super(hie);
         a2phome = System.getProperty("a2p_home", "/usr/local/a2p/");
         if(!a2phome.endsWith("/")) {
         	a2phome = a2phome + "/";
             System.setProperty("a2p_home",a2phome);
         }
         a2pLibPath =  getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
         if(a2pLibPath.contains(":")){//windows
        	 a2pLibPath=a2pLibPath.substring(1);
        	 a2pLibPath=a2pLibPath.replace("/", "\\");
        	 a2pLibPath=a2pLibPath+"..\\xsdlib\\";
         }else{//unix
        	 a2pLibPath = "/xsdlib/";
         }
         him = gmmsUtility.getHttpInterfaceManager();
	 }
	 /**
	  * init xsd file
	  * @param xsdFileName
	  */
	 public void init(String xsdFileName){
		 eleList = new ArrayList<String>();
		 try{
			 parserXSD(xsdFileName);
		 }catch(Exception e){
			 e.printStackTrace();
		 }
	 }
	 /** 
	  * bind object to xml 
	  * @param  Object
	  */  
	 public static String buildXml(Object javaObj) {  
		  try {  
			  jaxbContext = JAXBContext.newInstance(javaObj.getClass());  
			  marshaller = jaxbContext.createMarshaller();  
			  ByteArrayOutputStream baos = new ByteArrayOutputStream();
			  marshaller.marshal(javaObj, baos);
			  return baos.toString(charset);
		  }catch (JAXBException e) {  
			  e.printStackTrace();  
		  }catch(Exception e){
			  e.printStackTrace();  
		  }  
		  return null;
	 }  
	 /** 
	  * bind xml to Object 
	  * @param class,xml file  
	  * @return 
	  */   
	 public static Object buildObject(Class objclass, String xmlContent) {  
	  try {  
		    jaxbContext = JAXBContext.newInstance(objclass);  
			Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();    
			InputStream is =new ByteArrayInputStream(xmlContent.getBytes());  
			return unmarshaller.unmarshal(is);    
	  } catch (Exception e) {  
		  e.printStackTrace();  
	  }  
	  return null;
	 }  
   /**  
    * parse XSD to generate class relations  
    * @param xsdfile  
    * @return  
    * @throws Exception  
    */   
    public  void parserXSD(String xsdfile) throws Exception {   
	 	String xsdFilePath = a2pLibPath+xsdfile;
	    SAXReader saxReader = new SAXReader();   
	    InputStream ins = getClass().getResourceAsStream(xsdFilePath);
	    Document doc = saxReader.read(ins);  
	    Element element = doc.getRootElement();   
	    String basePath = null;  
	    Element dataElement = null;  
        basePath = "/xs:schema";   
        dataElement = (Element) element.selectSingleNode(basePath);   
	    List<Element> subs =  dataElement.elements();
	    for(Element e:subs){
	    	String qname = e.attributeValue("name");
	    	List<String> subList = new ArrayList<String>();
	    	List<Element>  subes = (List<Element>) e.selectNodes("xs:complexType");   
	    	for(Element ee:subes){
	    		String parentName = ee.getParent().attributeValue("name");
		    	System.out.print(parentName+"{");
	    		List<Element>  eles = (List<Element>) ee.selectNodes(".//xs:element");
	    		for(Element eee:eles){
	    			String eeename = eee.attributeValue("name");
	    			if(eeename==null||"".equals(eeename)){
	    				eeename = eee.attributeValue("ref");
	    			}
	    			subList.add(eeename);
	    			revMap.put(eeename, parentName);
			    	System.out.print(eeename+",");
	    		}
		    	System.out.println("}");
		    	eleList.add(parentName);
	    	}
	    }
    }   
    /**
     * convert gmmsmessage to object
     * @param pkgpath
     * @param msg
     */
    public String gmms2xml(String pkgpath,GmmsMessage msg,List<HttpParam> params,A2PCustomerInfo cst){
    	String parentClassName = (String)eleList.get(0);
    	parentClassName = convertName(parentClassName);
    	try{
    		Class parentClass = Class.forName(pkgpath+"."+parentClassName);
        	Object instance = parentClass.newInstance();
			HttpStatus hs = hi.mapGmmsStatus2HttpStatus(msg.getStatus(),msg.getMessageType());
        	for(HttpParam pm:params){
        		String par = pm.getParam();
        		String pval = null;
        		if("outTransID".equalsIgnoreCase(par)){
        			pval = MessageIdGenerator.generateCommonStringID();
    			}else if("outMsgID".equalsIgnoreCase(par)){
    				pval = MessageIdGenerator.generateCommonStringID();
    			}else if("msgID".equalsIgnoreCase(par)){
    				pval = MessageIdGenerator.generateCommonStringID();
    			}else if("deliveryReport".equalsIgnoreCase(par)){
    				pval = parseHttpDeliveryReport(pm,msg);
    			}else if("expiryDate".equalsIgnoreCase(par)){
    				pval = parseGmmsExpiredDate(pm,msg);
    			}else if("dateIn".equalsIgnoreCase(par)){
    				pval = parseDate(pm,msg.getDateIn());
    			}else if("contentType".equalsIgnoreCase(par)){
    				HttpCharset httpCharset = hi.mapGmmsCharset2HttpCharset(msg.getContentType());
    				pval  = httpCharset.getMessageType();
    			}else  if ("StatusCode".equalsIgnoreCase(par)) {
    				pval = "" + hs.getCode();
    			} else if ("StatusText".equalsIgnoreCase(par)) {
    				pval = hs.getText();
    			}else  if ("chlPassword".equalsIgnoreCase(par)) {
    				A2PSingleConnectionInfo sinfo = (A2PSingleConnectionInfo)cst;
    				String method2encrypt = sinfo.getPasswdEncryptMethod();
    				if(method2encrypt!=null && !"".equals(method2encrypt.trim())){
    					pval = HttpUtils.encrypt(sinfo.getChlPassword(), method2encrypt);
    				}else{
    					pval = cst.getChlPasswordr();
    				}
    			}else  if ("chlPasswordr".equalsIgnoreCase(par)) {
    				String method2encrypt = cst.getPasswdEncryptMethod();
    				if(method2encrypt!=null && !"".equals(method2encrypt.trim())){
    					pval = HttpUtils.encrypt(cst.getChlPasswordr(), method2encrypt);
    				}else{
    					pval = cst.getChlPasswordr();
    				}
    			}else{
    				pval = HttpUtils.getParameter(pm, msg, cst);
    			}
        		String oproperty = convertName(pm.getOppsiteParam());
        		setProperty(parentClass,instance,oproperty,pval);
        	}
        	String xml = buildXml(instance);
        	return xml;
    	}catch(Exception e){
    		log.error(e.getMessage(),e);
    	}
    	return null;
    }
    /**
     *convert object to gmmsmessage
     * @param pkgpath
     * @return
     */
    public GmmsMessage xml2gmms(String pkgpath,String xmlcontent,List<HttpParam> params,A2PCustomerInfo cst,String msgType){
    	GmmsMessage msg = new GmmsMessage();
    	String parentClassName = (String)eleList.get(0);
    	parentClassName = convertName(parentClassName);
    	try{
    		Class parentClass = Class.forName(pkgpath+"."+parentClassName);
        	Object reqst = buildObject(parentClass, xmlcontent);
        	String encoding = null; 
			String messageContent = null;
			String statusCode = null;
			String statusText = null;
        	for(HttpParam pm:params){
        		String par = pm.getParam();
        		String oproperty = convertName(pm.getOppsiteParam());
        		String oval = (String)getProperty(parentClass,reqst,oproperty);
        		if ("expiryDate".equalsIgnoreCase(par)) {
					Date expireDate = parseHttpExpiryDate(cst,pm, oval);
					if (expireDate != null) {
						expireDate = gmmsUtility.getGMTTime(expireDate);
					}
					msg.setProperty(par, expireDate);
				} else if ("textContent".equalsIgnoreCase(par)) {//textContent
					messageContent=oval;
				} else if ("contentType".equalsIgnoreCase(par)) {//contentType
					encoding = oval;
				}else if("StatusCode".equalsIgnoreCase(par)){
					statusCode = oval;
				}else if("StatusText".equalsIgnoreCase(par)){
					statusText =oval;
				} else {
					HttpUtils.setParameter(pm, msg, cst,oval);
				}
        	}
        	if(statusCode!=null && statusText!=null){
        		HttpStatus status = new HttpStatus(statusCode,statusText);
    			GmmsStatus gs = hi.mapHttpStatus2GmmsStatus(status,msgType);
    			msg.setStatus(gs);
    			msg.setMsgID(msg.getOutMsgID());
    			return msg;//parse response
        	}
        	//parse request
        	String gmmsCharset = hi.mapHttpCharset2GmmsCharset(encoding);
			msg.setContentType(gmmsCharset);
			
			if (MessageBase.AIC_MSG_TYPE_BINARY.equals(gmmsCharset)) {
				msg.setGmmsMsgType(MessageBase.AIC_MSG_TYPE_BINARY);
			}
		
			if (messageContent == null || "".equalsIgnoreCase(messageContent)) {
				msg.setStatus(GmmsStatus.INVALID_MSG_FIELD);
			} else {
				parseHttpContent(msg, encoding, messageContent, null);
			}

			msg.setOSsID(cst.getSSID());
			String  commonMsgID = MessageIdGenerator.generateCommonMsgID(cst.getSSID());
			msg.setMsgID(commonMsgID);

    	}catch(Exception e){
    		log.error(e.getMessage(),e);
    	}
    	return msg;
    }
    /**
     * 
     * @param clazz
     * @param instance
     * @param propertyName
     * @return
     */
    public Object getProperty(Class clazz,Object instance,String propertyName){
		Method[] methods = clazz.getMethods();
		Object obj = null;
		try {
			for (Method f : methods) {
				if (f.getName().equalsIgnoreCase("get" + propertyName)) {
					obj =f.invoke(instance);
				}
			}
		} catch (Exception e) {
			System.out.println("get properyt " + propertyName + " error!");
		}
		return obj;
	}
    /**
     * 
     * @param clazz
     * @param instance
     * @param propertyName
     * @param value
     */
    public void setProperty(Class clazz,Object instance,String propertyName,Object value){
		Method[] methods = clazz.getMethods();
		try {
			for (Method f : methods) {
				if (f.getName().equalsIgnoreCase("set" + propertyName)) {
					f.invoke(instance,value);
				}
			}
		} catch (Exception e) {
			System.out.println("set properyt " + propertyName + " error!");
		}
	}
    /**
     * convert qname
     * @param qname
     * @return
     */
    public static String convertName(String qname){
    	String[] names = qname.split("_");
    	StringBuilder sb = new StringBuilder();
    	for(String name:names){
    		char firstC = Character.toUpperCase(name.charAt(0));
        	String cname = firstC + name.substring(1);
        	sb.append(cname);
    	}
    	return sb.toString();
    }
//    public  static void main(String[] args){
//		if(args.length<4){
//			System.out.println("XmlMessageConverter -g|-x file.xsd pkgpath interface");      
//			System.exit(0);
//		}
//		String cmdOpt = args[0];
//		String xsdFileName = args[1]; 
//		String pkgpath = args[2]; 
//		String interfaceName = args[3];  
////		if(cmdOpt.equalsIgnoreCase("-x")){
////			try{
////				XmlMessageConverter tools = new XmlMessageConverter();
////				tools.init(xsdFileName);
////				GmmsMessage msg = new GmmsMessage();
//////				tools.gmms2xml(pkgpath, interfaceName, msg);
////			}catch(Exception e){
////				e.printStackTrace();
////			}
////		}else if(cmdOpt.equalsIgnoreCase("-g")){
////			try{
////				XmlMessageConverter tools = new XmlMessageConverter();
////				tools.init(xsdFileName);
//////				tools.xml2gmms(pkgpath, interfaceName, xmlcontent);
////			}catch(Exception e){
////				e.printStackTrace();
////			}
////		}
//	}
}
