package com.king.gmms.protocol.commonhttp.xml;

import java.io.File;
import java.io.FileWriter;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.Namespace;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;
import org.dom4j.tree.DefaultElement;

import com.sun.tools.internal.xjc.XJCFacade;

public class XmlTools {
	 private static List<String> eleList = new ArrayList<String>();
	 private static HashMap<String,String> revMap = new HashMap<String,String>();
	 private static String a2phome = "/usr/local/a2p/";
	 private static String a2pLibPath = "/usr/local/a2p/Gmms/WEB-INF/lib/";
	 private static String xmlStorePath = "/usr/local/a2p/tempFile/";
	 public XmlTools(){
         a2phome = System.getProperty("a2p_home", "/usr/local/a2p/");
         if(!a2phome.endsWith("/")) {
         	a2phome = a2phome + "/";
             System.setProperty("a2p_home",a2phome);
         }
         a2pLibPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
         if(a2pLibPath.contains(":")){
        	 a2pLibPath=a2pLibPath.substring(1);
        	 a2pLibPath=a2pLibPath.replace("/", "\\");
        	 a2pLibPath=a2pLibPath+"..\\xsdlib\\";
        	 xmlStorePath = a2pLibPath+"..\\temp\\";
         }else{
        	 xmlStorePath = a2pLibPath+"../../../temp/";
         }
	 }
	 /**
	  * modify xml & xsd
	  * @param xmlfile
	  * @param xsdfile
	  * @throws Exception
	  */
	 public static void generateXSD(String xmlfile,String xsdfile) throws Exception {  
		 	SAXReader saxReader = new SAXReader();   
		 	String xmlFilePath = xmlStorePath+xmlfile;
		 	String xsdFilePath = a2pLibPath+xsdfile;
		    Document doc = saxReader.read(xmlFilePath);   
		    Element element = doc.getRootElement();
		    String nsPrefix = element.getNamespacePrefix();
		    Namespace ns = element.getNamespaceForPrefix(nsPrefix);
		    if(ns!=null){
		    	element.remove(ns);
		    	((DefaultElement) element).setNamespace(Namespace.NO_NAMESPACE);
		    }else{
		    	return;
		    }
            OutputFormat format = OutputFormat.createPrettyPrint();
            String newxmlFilePath = a2pLibPath+xmlfile;
		    XMLWriter writer = new XMLWriter(new FileWriter(newxmlFilePath), format);
		    writer.write(doc);
		    writer.close();
		    try{
		    	String command = "java -jar "+a2pLibPath+"trang.jar "+newxmlFilePath+" "+xsdFilePath;
				Runtime.getRuntime().exec(command);
				Thread.sleep(1000);
			}catch(Exception e){
				e.printStackTrace();
			}
			
			//then modify xsd for XmlMessageConverter
//			SAXReader saxReader2 = new SAXReader();   
//		    Document doc2 = saxReader2.read(xsdFilePath);   
//		    Element xsdEle = doc2.getRootElement();
//		    if(ns!=null && ns.getPrefix()!=null && ns.getPrefix().length()!=0){
//			    xsdEle.addAttribute("targetNamespace", ns.getURI());
////			    xsdEle.remove(attr);
//			    XMLWriter xsdwriter = new XMLWriter(new FileWriter(xsdFilePath), format);
//			    xsdwriter.write(doc2);
//			    xsdwriter.close();
//		    }
	 }
	 /**  
    * parse XSD to generate class relations  
    * @param xsdfile  
    * @return  
    * @throws Exception  
    */   
    public static List parserXSD(String xsdfile) throws Exception {   
	 	String xsdFilePath = a2pLibPath+xsdfile;
	    SAXReader saxReader = new SAXReader();   
	    Document doc = saxReader.read(xsdFilePath);   
	    Element element = doc.getRootElement();   
	    String basePath = null;  
	    Element dataElement = null;  
        basePath = "/xs:schema";   
        dataElement = (Element) element.selectSingleNode(basePath);   
	    List<Element> subs =  dataElement.elements();
	    for(Element e:subs){
	    	String qname = e.attributeValue("name");
//		    	System.out.println(qname);
//	    	eleMap.put(qname, e);
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
	    return eleList;
    }   
    /**
     * 
     * @param pkgpath
     * @param xsdfile
     */
    public static void generateClass(String xsdfile,String pkgpath){
    	String projectPath = new File("").getAbsolutePath();  
    	String destDir  =projectPath+"\\src\\";
	 	String xsdFilePath = a2pLibPath+xsdfile;
		//JAXB描述文件   
		List<String> argsList = new ArrayList<String>();   
		//一次性可以添加多个XSD File   
		argsList.add(xsdFilePath);   
		//添加JAXB描述文件   
		argsList.add("-p");     
		argsList.add(pkgpath);   
		//添加生成的Java文件路径 
		argsList.add("-d");   
		argsList.add(destDir); 
		//生成代码   
		String[] args = new String[argsList.size()];   
		try{
			XJCFacade.main(argsList.toArray(args));
		}catch(Throwable t){
			t.printStackTrace();
		}
    }
    /**
     * modify java class
     * @param pkgpath
     */
    public static void addMethod(String pkgpath){
		String projectPath = new File("").getAbsolutePath();    
    	String pkgfilepath = pkgpath.replace(".", "\\");
    	pkgfilepath =projectPath+"\\src\\"+pkgfilepath+"\\";
    	try{
        	ArrayList<String> clist = new ArrayList<String>(eleList);
        	Collections.reverse(clist);
        	for(String classname:clist){
        		if(revMap.containsKey(classname)){
        			String cname = convertName(classname);
        	    	Class firstClass = Class.forName(pkgpath+"."+cname);
        	    	String parentClassName = convertName(revMap.get(classname));
        	    	Class parentClass = Class.forName(pkgpath+"."+parentClassName);
        	    	Field[] fields = parentClass.getDeclaredFields();
        	    	StringBuffer sb = new StringBuffer();
        	    	for(Field f:fields){
                		if(f.getType().equals(firstClass)){
                			Method[] methods = firstClass.getDeclaredMethods();
                			for(Method method:methods){
                				try{
                					if(parentClass.getDeclaredMethod(method.getName())!=null){
                    					return;
                    				}
                				}catch(Exception e){
                				}
                		    	String methodDecl = genMethod(method);
                				sb.append("\t").append(methodDecl).append("{\r\n")
                				.append(genMethodCode(f,method,firstClass))
                				.append("\r\n\t}\r\n");
                			}
                		}
                	}
        	    	if(sb.length()>0){
        	    		RandomAccessFile randomFile = new RandomAccessFile(pkgfilepath+parentClassName+".java", "rw");  
                        long fileLength = randomFile.length(); 
                        randomFile.seek(fileLength-3); 
//                        System.out.println( (char)randomFile.readByte());
                        sb.append("\r\n}\r\n");
                        randomFile.writeBytes(sb.toString());  
                        randomFile.close(); 
//        	    		System.out.println("Add to "+parentClassName+":"+sb.toString());
        	    	}
        		}
        	}
    	}catch(Exception e){
    		e.printStackTrace();
    	}
    }
    public static String genMethod(Method method){
    	String returnName = method.getReturnType().getSimpleName();
		Class[] parameterTypes = method.getParameterTypes();
    	if(parameterTypes.length>0){
    		return "public "+returnName+" "+method.getName()+"("+parameterTypes[0].getSimpleName()+" value)";
    	}else{
    		return "public "+returnName+" "+method.getName()+"()";
    	}
    }
    public static String genMethodCode(Field f,Method method,Class firstClass){
		Class[] parameterTypes = method.getParameterTypes();
    	StringBuffer sb = new StringBuffer();
    	sb.append("\t\tif(")
		.append(f.getName()).append(" == null){\r\n")
		.append("\t\t\t").append(f.getName()).append(" = new ").append(firstClass.getSimpleName())
		.append("();").append("\r\n\t\t}\r\n\t\t");
    	if(parameterTypes.length>0){
    		return sb.append(f.getName()).append(".").append(method.getName()).append("(value);").toString();
    	}else{
    		return sb.append("return ").append(f.getName()).append(".").append(method.getName()).append("();").toString();
    	}
    }
    /**
     * 
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
	public  static void main(String[] args){
		if(args.length<4){
			System.out.println("XmlTools -g|-m file.xml file.xsd pkgpath");      
			System.exit(0);
		}
		String cmdOpt = args[0];
		String xmlFileName = args[1]; 
		String xsdFileName = args[2]; 
		String pkgpath = args[3];  
		if(cmdOpt.equalsIgnoreCase("-g")){
			try{
		        XmlTools tools = new XmlTools();
		        tools.generateXSD(xmlFileName, xsdFileName);
		        tools.generateClass(xsdFileName,pkgpath);
			}catch(Exception e){
				e.printStackTrace();
			}
		}else if(cmdOpt.equalsIgnoreCase("-m")){
			try{
		        XmlTools tools = new XmlTools();
		        tools.parserXSD(xsdFileName);
		        tools.addMethod(pkgpath);
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}
}
