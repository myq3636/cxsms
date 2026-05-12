package com.king.gmms.ha.systemmanagement;


import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.systemmanagement.ConnectionManagementForCore;
import com.king.gmms.connectionpool.systemmanagement.ConnectionManagementForFunction;
import com.king.gmms.connectionpool.systemmanagement.ConnectionManagementForMGT;
import com.king.gmms.connectionpool.systemmanagement.SystemManagementInterface;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.ha.systemmanagement.pdu.KeepAliveAck;
import com.king.gmms.ha.systemmanagement.pdu.SystemPdu;
import com.king.gmms.util.SystemConstants;

public class SystemMessageHandler implements MessageHandler{
    private static SystemLogger log = SystemLogger.getSystemLogger(SystemMessageHandler.class);
	private SystemManagementInterface connectionManager = null;
    private ModuleManager moduleManager = null;
	
	public SystemMessageHandler() {
        moduleManager = ModuleManager.getInstance();
        String moduleName = System.getProperty("module");
        String moduleType = moduleManager.getModuleType(moduleName);
        if(SystemConstants.MGT_MODULE_TYPE.equalsIgnoreCase(moduleType)){
        	connectionManager = ConnectionManagementForMGT.getInstance();
        }else if(SystemConstants.ROUTER_MODULE_TYPE.equalsIgnoreCase(moduleType)){
        	connectionManager = ConnectionManagementForCore.getInstance();
        }else if(SystemConstants.SERVER_MODULE_TYPE.equalsIgnoreCase(moduleType)
    			||SystemConstants.CLIENT_MODULE_TYPE.equalsIgnoreCase(moduleType)
    			||SystemConstants.MQM_MODULE_TYPE.equalsIgnoreCase(moduleType)){
        	connectionManager = ConnectionManagementForFunction.getInstance();
        }else{
        	log.fatal("Invalid module type:{} for {}",moduleType,moduleName);
        	System.exit(-1);
        }
    }
	
	/**
	 * process SystemMessage
	 */
	public  SystemPdu process(SystemPdu message) {
		if (message == null) {
            return null;
        }
		SystemPdu response = null;
        switch (message.getCommandId()) {
            case SystemPdu.COMMAND_KEEP_ALIVE:
            	response = new KeepAliveAck(message);
                break;
            case SystemPdu.COMMAND_IN_BIND_REQUEST:
            	response = connectionManager.handleInBindRequest(message);
                break;
            case SystemPdu.COMMAND_OUT_BIND_REQUEST:
            	response = connectionManager.handleOutBindRequest(message);
                break;
            case SystemPdu.COMMAND_CONNECTION_STATUS_NOTIFICATION:
            	response = connectionManager.handleStatusNotification(message);
                break;
            case SystemPdu.COMMAND_CONNECTION_CONFIRM:
            	response = connectionManager.handleConnectionConfirm(message);
                break;
            case SystemPdu.COMMAND_MODULE_REGISTER_REQUEST:
            	response = connectionManager.handleModuleRegister(message);
                break;
            case SystemPdu.COMMAND_MODULE_STOP_REQUEST:
            	response = connectionManager.handleModuleStop(message);
                break;
            case SystemPdu.COMMAND_DB_OPERATION_REQUEST:
            	response = connectionManager.handleDBRequest(message);
                break;
            case SystemPdu.COMMAND_CHANGEDB:
            	response = connectionManager.handleDBChanged(message);
                break;
            case SystemPdu.COMMAND_CHANGEREDIS:
            	response = connectionManager.handleRedisChanged(message);
                break;
            case SystemPdu.COMMAND_SHUTDOWN_SESSION:
            	response = connectionManager.handleShutdownSession(message);
                break;
            case SystemPdu.COMMAND_SHUTDOWN_SESSION_ACK:
            	response = connectionManager.handleShutdownSessionAck(message);
                break;
            case SystemPdu.COMMAND_REPORT_IN_MSG_COUNT:
            	response = connectionManager.handleReportInMsgCount(message);
                break;
            case SystemPdu.COMMAND_APPLY_IN_THROTTLE_QUOTA:
            	response = connectionManager.handleApplyInThrottleQuota(message);
                break;
            case SystemPdu.COMMAND_QUERY_HTTP_ACK:
            	response = connectionManager.handleQueryHttpAck(message);
                break;
            case SystemPdu.COMMAND_QUERY_HTTP_REQUEST:
            	response = connectionManager.handleQueryHttpRequest(message);
                break;
            case SystemPdu.COMMAND_CONNECTION_HTTP_CONFIRM:
            	connectionManager.handleConnectionHttpConfirm(message);
                break;
            default:
            	response = new KeepAliveAck(message);
                break;
        }
		return response;
	}
}
