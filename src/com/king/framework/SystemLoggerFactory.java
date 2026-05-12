package com.king.framework;

import org.apache.logging.log4j.core.LoggerContext;

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * <p>Copyright: Copyright (c) 2006</p>
 *
 */
public class SystemLoggerFactory
     extends LoggerContext{

	//private static LoggerContext logContext = (LoggerContext)LogManager.getContext();
	public SystemLoggerFactory(String name,LoggerContext logContext) {
		
		super(name,logContext);
		// TODO Auto-generated constructor stub
	}
   

    /**
     * makeNewLoggerInstance
     *
     * @param string String
     * @return Logger
     * @todo Implement this org.apache.log4j.spi.LoggerFactory method
     */
   /* public Logger makeNewLoggerInstance(String name) {
        return new SystemLogger(name);
    }*/
	/* public Logger getLogger(String name) {

	      return  new SystemLogger(this,name);
	    }
*/
	
}
