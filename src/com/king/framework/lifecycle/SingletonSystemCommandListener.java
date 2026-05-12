package com.king.framework.lifecycle;
/**
 * Singleton SystemCommandListener
 * @author jianmingyang
 *
 */
public class SingletonSystemCommandListener extends SystemCommandListener {
	private static SingletonSystemCommandListener instance=null;  
	  
    public static synchronized SingletonSystemCommandListener startCommandListener() {  
	    if (instance == null) {  
	        instance = new SingletonSystemCommandListener();  
	        instance.service();
	    }  
	    return instance;  
    }  
    private SingletonSystemCommandListener(){
	   super(port);
    }
}
