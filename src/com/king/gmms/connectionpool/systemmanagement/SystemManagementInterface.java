package com.king.gmms.connectionpool.systemmanagement;

import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.ha.systemmanagement.pdu.SystemPdu;

public interface SystemManagementInterface {
	public boolean insertSession(A2PCustomerInfo custInfo, TransactionURI transaction, boolean isServer);
	public SystemPdu handleInBindRequest(SystemPdu message);
	public SystemPdu handleOutBindRequest(SystemPdu message);
	public SystemPdu handleConnectionConfirm(SystemPdu message);
	public SystemPdu handleStatusNotification(SystemPdu message);
	public SystemPdu handleModuleRegister(SystemPdu message);
	public SystemPdu handleModuleStop(SystemPdu message);
	public SystemPdu handleShutdownSession(SystemPdu message);
	public SystemPdu handleShutdownSessionAck(SystemPdu message);
	public SystemPdu handleDBRequest(SystemPdu message);
	public SystemPdu handleDBChanged(SystemPdu message);
	public SystemPdu handleRedisChanged(SystemPdu message);
	public SystemPdu handleReportInMsgCount(SystemPdu message);
	public SystemPdu handleApplyInThrottleQuota(SystemPdu message);
	public SystemPdu handleQueryHttpRequest(SystemPdu message);
	public SystemPdu handleQueryHttpAck(SystemPdu message);
	public void handleConnectionHttpConfirm(SystemPdu message);
	public void handleApplyInThrottleQuotaAck(SystemPdu message);
}
