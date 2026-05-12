/**
 * Author: frank.xue@King.com
 * Date: 2006-8-28
 * Time: 13:45:20
 * Document Version: 0.1
 */

package com.king.gmms.ha;

import java.io.Serializable;
import java.util.UUID;

import com.king.gmms.util.SystemConstants;

public class TransactionURI implements Serializable,Comparable {
    private ModuleURI module;
    private UUID id;
    private String connectionName="";

    public TransactionURI() {
        module = ModuleURI.self();
        id = UUID.randomUUID();
    }

    public TransactionURI(String conn) {
        this();
        if(conn!=null){
        	connectionName = conn;
        }
    }
    /**
     * 
     * @param module
     * @param conn
     */
    public TransactionURI(ModuleURI module,String conn) {
    	this.module = module;
    	if(conn!=null){
    		this.connectionName = conn;
        }
    	this.id = UUID.randomUUID();
    }
    public TransactionURI(byte[] ipBytes){
        module = ModuleURI.self();
        if(ipBytes != null && ipBytes.length > 0){
            id = UUID.nameUUIDFromBytes(ipBytes);
        }else{
            id = UUID.randomUUID();
        }
    }

    public ModuleURI getModule() {
        return module;
    }

    public UUID getId() {
        return id;
    }

    public String getConnectionName(){
        return connectionName;
    }

    public void setConnectionName(String str){
        connectionName = str;
    }

    public int hashCode() {
    		return id.hashCode()*31 + module.hashCode();
    }

    public boolean equals(Object obj) {
        if (obj instanceof TransactionURI) {
            TransactionURI uri = (TransactionURI) obj;
            return id.equals(uri.id) && module.equals(uri.module) && connectionName.equals(uri.connectionName);
        }
        else {
            return false;
        }
    }

    public String toString() {
        return new StringBuilder()
            .append(module.toString())
            .append("/")
            .append(id.toString())
            .append("/")
            .append(connectionName).toString();
    }
    
	public void setModule(ModuleURI module) {
		this.module = module;
	}

	public void setId(UUID id) {
		this.id = id;
	}
	/**
	 * generate TransactionURI from toString 
	 * @param uriString
	 * @return
	 */
	public static TransactionURI fromString(String uriString){
	    	TransactionURI transaction = new TransactionURI();
	    	if(uriString == null){
	    		return transaction; 
	    	}
	    	String[] uriArray = uriString.split(SystemConstants.SLASH_SEPARATOR);
	    	if(uriArray.length<2){
	    		System.out.println("TransactionURI convert with invalid uriString:"+uriString);
	    		return transaction;
	    	}
	    	try{
		    	ModuleURI  moduleUri = ModuleURI.fromString(uriArray[0]);
		    	UUID uuid = UUID.fromString(uriArray[1]);
		    	transaction.setModule(moduleUri);
		    	transaction.setId(uuid);
		    	if(uriArray.length==3){
			    	String connectionName = uriArray[2];
			    	transaction.setConnectionName(connectionName);
		    	}
	    	}catch(IllegalArgumentException e){
	    		e.printStackTrace();
	    	}catch(Exception e){
	    		e.printStackTrace();
	    	}
	    	return transaction;
	}

	
	public int compareTo(Object o) {
		if(this.equals(o)){
			return 0;
		}else{
			return 1;
		}
	}
}
