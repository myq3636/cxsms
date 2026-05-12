package com.king.framework;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.king.message.gmms.GmmsMessage;

public class SystemLogger{

	private int size = 1024;
	private Logger logger = null;

	
	public static SystemLogger getSystemLogger(String name){
		return new SystemLogger(name);
	}
	
	public static SystemLogger getSystemLogger(Class clazz) {
		return new SystemLogger(clazz.getName());
	}
	
	private SystemLogger(String name) {		
		logger = LogManager.getLogger(name);
	}

	public void trace(String message) {
		logger.trace(message);
	}

	public void trace(Object e, Throwable t) {
		logger.trace(e, t);
	}
	
	public void trace(GmmsMessage gmmsMsg, Object message) {
		logger.trace(getFormatMessage(gmmsMsg, message));
	}
	
	public void trace(String message, Object... params) {
		logger.trace(message, params);
	}
	
	public void trace(GmmsMessage gmmsMsg, Object message, Throwable t) {
		logger.trace(getFormatMessage(gmmsMsg, message), t);
	}

	public void trace(GmmsMessage gmmsMsg, Object message, Object... params) {
		logger.trace(getFormatMessage(gmmsMsg, message).toString(), params);
	}

	public void debug(String message) {
		logger.debug(message);
	}
	
	public void debug(Object e, Throwable t) {
		logger.debug(e, t);
	}

	public void debug(GmmsMessage gmmsMsg, Object message) {
		logger.debug(getFormatMessage(gmmsMsg, message));
	}
	
	public void debug(String message, Object... params) {
		logger.debug(message, params);
	}

	public void debug(GmmsMessage gmmsMsg, Object message, Throwable t) {
		logger.debug(getFormatMessage(gmmsMsg, message), t);
	}
	
	public void debug(GmmsMessage gmmsMsg, Object message, Object... params) {
		logger.debug(getFormatMessage(gmmsMsg, message).toString(), params);
	}

	public void info(String message) {
		logger.info(message);
	}

	public void info(Object e, Throwable t) {
		logger.info(e, t);
	}
	
	public void info(GmmsMessage gmmsMsg, Object message) {
		logger.info(getFormatMessage(gmmsMsg, message));
	}
	
	public void info(String message, Object... params) {
		logger.info(message, params);
	}
	
	public void info(GmmsMessage gmmsMsg, Object message, Throwable t) {
		logger.info(getFormatMessage(gmmsMsg, message), t);
	}

	public void info(GmmsMessage gmmsMsg, Object message, Object... params) {
		logger.info(getFormatMessage(gmmsMsg, message).toString(), params);
	}

	public void warn(String message) {
		logger.warn(message);
	}

	public void warn(Object e, Throwable t) {
		logger.warn(e, t);
	}
	
	public void warn(GmmsMessage gmmsMsg, Object message) {
		logger.warn(getFormatMessage(gmmsMsg, message));
	}
	
	public void warn(String message, Object... params) {
		logger.warn(message, params);
	}

	public void warn(GmmsMessage gmmsMsg, Object message, Throwable t) {
		logger.warn(getFormatMessage(gmmsMsg, message), t);
	}
	
	public void warn(GmmsMessage gmmsMsg, Object message, Object... params) {
		logger.warn(getFormatMessage(gmmsMsg, message).toString(), params);
	}

	public void error(String message) {
		logger.error(message);
	}

	public void error(Object e, Throwable t) {
		logger.error(e, t);
	}

	public void error(GmmsMessage gmmsMsg, Object message) {
		logger.error(getFormatMessage(gmmsMsg, message));
	}

	public void error(String message, Object... params) {
		logger.error(message, params);
	}
	
	public void error(GmmsMessage gmmsMsg, Object message, Throwable t) {
		logger.error(getFormatMessage(gmmsMsg, message), t);
	}

	public void error(GmmsMessage gmmsMsg, Object message, Object... params) {
		logger.error(getFormatMessage(gmmsMsg, message).toString(), params);
	}

	public void fatal(String message) {
		logger.fatal(message);
	}
	
	public void fatal(Object e, Throwable t) {
		logger.fatal(e, t);
	}

	public void fatal(GmmsMessage gmmsMsg, Object message) {
		logger.fatal(getFormatMessage(gmmsMsg, message));
	}
	
	public void fatal(String message, Object... params) {
		logger.fatal(message, params);
	}

	public void fatal(GmmsMessage gmmsMsg, Object message, Throwable t) {
		logger.fatal(getFormatMessage(gmmsMsg, message), t);
	}
	
	public void fatal(GmmsMessage gmmsMsg, Object message, Object... params){
		logger.fatal(getFormatMessage(gmmsMsg, message).toString(), params);
	}

	public boolean isTraceEnabled() {
		return logger.isTraceEnabled();
	}

	public boolean isDebugEnabled() {
		return logger.isDebugEnabled();
	}

	public boolean isInfoEnabled() {
		return logger.isInfoEnabled();
	}

	public boolean isWarnEnabled() {
		return logger.isWarnEnabled();
	}

	public boolean isErrorEnabled() {
		return logger.isErrorEnabled();
	}

	public boolean isFatalEnabled() {
		return logger.isFatalEnabled();
	}

	private Object getFormatMessage(GmmsMessage gmmsMsg, Object msg) {
		if (gmmsMsg != null) {
			if  (gmmsMsg.getMsgID()!=null) {
				return new StringBuffer(size).append(gmmsMsg.getMsgID())
				 .append(" ").append(msg);
			} else if (gmmsMsg.getOutMsgID()!=null) {
				return new StringBuffer(size).append(gmmsMsg.getOutMsgID())
				 .append(" ").append(msg);
			} 
		}
		return msg;
	}
	
	public static void main(String[] args) {
		SystemLogger log = SystemLogger.getSystemLogger(A2PService.class);
		if (log instanceof SystemLogger) {
			System.out.println("SystemLogger");
		} else if (log instanceof Logger) {
			System.out.println("Logger");
		} else {
			System.out.println("Error");
		}
	}
}
