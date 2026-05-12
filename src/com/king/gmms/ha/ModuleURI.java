/**
 * Author: frank.xue@King.com
 * Date: 2006-7-6
 * Time: 11:24:02
 * Document Version: 0.1
 */

package com.king.gmms.ha;

import com.king.gmms.GmmsUtility;

import java.io.Serializable;
import java.util.UUID;

public class ModuleURI implements Serializable {
    private String address;
    private String module;
    private UUID id;
    
    public ModuleURI(String address, String module) {
        if(address == null || module == null) {
            throw new NullPointerException("The two parameters of ModuleURI can't be null. Address="+address+";Module="+module);
        }
        this.address = address;
        this.module = module;
        this.id = UUID.randomUUID();
    }

    public static ModuleURI all() {
        return new All();
    }

    public static ModuleURI self() {
        return createLocalURI(System.getProperty("module"));
    }

    public static ModuleURI createLocalURI(String module) {
        return new ModuleURI(GmmsUtility.getInstance().getServerIP(), module);
    }

    public boolean isLocal() {
        return address.equalsIgnoreCase(GmmsUtility.getInstance().getServerIP());
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getModule() {
        return module;
    }

    public void setModule(String module) {
        this.module = module;
    }

    public int hashCode() {
        return address.hashCode() * 9 + module.hashCode()+id.hashCode();
    }

    public boolean equals(Object obj) {
        if(obj instanceof ModuleURI) {
            ModuleURI entry = (ModuleURI) obj;
            return entry.address.equalsIgnoreCase(this.address)
                   && entry.module.equalsIgnoreCase(this.module)
                   && entry.id.equals(this.id);
        }
        else {
            return false;
        }
    }

    public String toString() {
        return new StringBuilder(50).append(address).append(":").append(module).append(":").append(id).toString();
    }
    /**
     * 
     * @param uriStr
     * @return
     */
    public static ModuleURI fromString(String uriStr){
    	ModuleURI moduleURI = ModuleURI.self();
    	String[] uriArray = uriStr.split(":");
    	if(uriArray.length<3){
    		System.out.println("ModuleURI convert with invalid uriString:"+uriStr);
    		return moduleURI;
    	}
    	moduleURI.setAddress(uriArray[0]);
    	moduleURI.setModule(uriArray[1]);
    	UUID uuid = UUID.fromString(uriArray[2]);
    	moduleURI.setId(uuid);
    	return moduleURI;
    }
    
    static class All extends ModuleURI {
        public All() {
            super("both", "all");
        }
    }
	public UUID getId() {
		return id;
	}

	public void setId(UUID id) {
		this.id = id;
	}
}
