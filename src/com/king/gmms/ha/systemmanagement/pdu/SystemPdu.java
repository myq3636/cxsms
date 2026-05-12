package com.king.gmms.ha.systemmanagement.pdu;

import java.io.UnsupportedEncodingException;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.protocol.tcp.internaltcp.Parameter;
import com.king.gmms.protocol.tcp.internaltcp.TcpByteBuffer;
import com.king.gmms.protocol.tcp.internaltcp.exception.HeaderIncompleteException;
import com.king.gmms.protocol.tcp.internaltcp.exception.InvalidPduException;
import com.king.gmms.protocol.tcp.internaltcp.exception.MessageIncompleteException;
import com.king.gmms.protocol.tcp.internaltcp.exception.NotEnoughDataInByteBufferException;
import com.king.gmms.protocol.tcp.internaltcp.exception.UnknownCommandIdException;
import com.king.gmms.protocol.tcp.internaltcp.exception.UnknownParameterIdException;
import com.king.gmms.protocol.tcp.internaltcp.exception.UnsupportedCommandIdException;

public class SystemPdu {
	private static SystemLogger log = SystemLogger.getSystemLogger(SystemPdu.class);

	protected SystemPduHeader header = null;
	protected String uuid = null;
	protected String timestamp = null;
	public static int PDU_HEADER_SIZE = 41;
    private static int sequenceNumber = 0;
    private static Object sequemutex = new Object();
	////////didn't tranfer in pdu////////
	private static ModuleManager moduleManager = null;
	private String moduleName = null;
    private String moduleType = null;

	// Command Type
	public static final int COMMAND_KEEP_ALIVE = 0x01;
	public static final int COMMAND_ACK = 0x02;
	public static final int COMMAND_IN_BIND_REQUEST = 0x03;
	public static final int COMMAND_IN_BIND_ACK = 0x04;
	public static final int COMMAND_OUT_BIND_REQUEST = 0x05;
	public static final int COMMAND_OUT_BIND_ACK = 0x06;
	public static final int COMMAND_CONNECTION_STATUS_NOTIFICATION = 0x07;
	public static final int COMMAND_CONNECTION_STATUS_NOTIFICATION_ACK = 0x08;
	public static final int COMMAND_CONNECTION_CONFIRM = 0x09;
	public static final int COMMAND_MODULE_REGISTER_REQUEST = 0x0a;
	public static final int COMMAND_MODULE_REGISTER_ACK = 0x0b;
	public static final int COMMAND_MODULE_STOP_REQUEST = 0x0c;
	public static final int COMMAND_MODULE_STOP_ACK = 0x0d;
	public static final int COMMAND_DB_OPERATION_REQUEST = 0x0e;
	public static final int COMMAND_DB_OPERATION_ACK = 0x0f;
	public static final int COMMAND_CHANGEDB = 0x10;
	public static final int COMMAND_CHANGEDB_ACK = 0x11;
	public static final int COMMAND_CHANGEREDIS = 0x12;
	public static final int COMMAND_CHANGEREDIS_ACK = 0x13;
	public static final int COMMAND_SHUTDOWN_SESSION = 0x14;
	public static final int COMMAND_SHUTDOWN_SESSION_ACK = 0x15;
	public static final int COMMAND_REPORT_IN_MSG_COUNT = 0x16;
	public static final int COMMAND_APPLY_IN_THROTTLE_QUOTA = 0x17;
	public static final int COMMAND_APPLY_IN_THROTTLE_QUOTA_ACK = 0x18;
	public static final int COMMAND_QUERY_HTTP_REQUEST = 0x19;
	public static final int COMMAND_QUERY_HTTP_ACK = 0x1A;
	public static final int COMMAND_CONNECTION_HTTP_CONFIRM = 0x1B;
	public static final int COMMAND_CONNECTION_CONFIRM_ACK = 0x1C;
	
	// Parameter Tag Identifier
	public static final int FIELD_UUID = 0x01;
	public static final int FIELD_TIMESTAMP = 0x02;
	public static final int FIELD_SSID = 0x03;
	public static final int FIELD_RESPONSECODE = 0x04;
	public static final int FIELD_BINDTYPE = 0x05;
	public static final int FIELD_ACTION = 0x06;
	public static final int FIELD_SESSIONNUM = 0x07;
	public static final int FIELD_DB_STATUS = 0x08;
	public static final int FIELD_REDIS_STATUS = 0x09;
	public static final int FIELD_CONNECTIONNAME = 0x0a;
	public static final int FIELD_MODULE_INCOMING_MSG_COUNT = 0x0b;
	public static final int FIELD_SYS_LOAD_PERCENT = 0x0c;
	public static final int FIELD_VALUE = 0x0d;
	public static final int FIELD_QUERYFLAG = 0x0e;
	public static final int FIELD_CUSTOMER_SSID = 0x0f;


	public static int[] commandIdList = { COMMAND_KEEP_ALIVE, COMMAND_ACK,
			COMMAND_IN_BIND_REQUEST, COMMAND_IN_BIND_ACK,
			COMMAND_OUT_BIND_REQUEST, COMMAND_OUT_BIND_ACK,
			COMMAND_CONNECTION_STATUS_NOTIFICATION,
			COMMAND_CONNECTION_STATUS_NOTIFICATION_ACK,
			COMMAND_CONNECTION_CONFIRM, COMMAND_MODULE_REGISTER_REQUEST,
			COMMAND_MODULE_REGISTER_ACK, COMMAND_MODULE_STOP_REQUEST,
			COMMAND_MODULE_STOP_ACK, COMMAND_DB_OPERATION_REQUEST,
			COMMAND_DB_OPERATION_ACK, COMMAND_CHANGEDB, COMMAND_CHANGEDB_ACK, 
			COMMAND_CHANGEREDIS, COMMAND_CHANGEREDIS_ACK,COMMAND_SHUTDOWN_SESSION,
			COMMAND_SHUTDOWN_SESSION_ACK, COMMAND_REPORT_IN_MSG_COUNT, 
			COMMAND_APPLY_IN_THROTTLE_QUOTA, COMMAND_APPLY_IN_THROTTLE_QUOTA_ACK,
			COMMAND_QUERY_HTTP_REQUEST, COMMAND_QUERY_HTTP_ACK, COMMAND_CONNECTION_HTTP_CONFIRM,
			COMMAND_CONNECTION_CONFIRM_ACK};
	
	public static String[] commandTypeList = { "KEEP_ALIVE", "ACK",
		"IN_BIND", "IN_BIND_ACK",
		"OUT_BIND", "OUT_BIND_ACK",
		"CONNECTION_STATUS_NOTIFICATION",
		"CONNECTION_STATUS_NOTIFICATION_ACK",
		"CONNECTION_CONFIRM", "MODULE_REGISTER",
		"MODULE_REGISTER_ACK", "MODULE_STOP",
		"MODULE_STOP_ACK", "DB_OPERATION",
		"DB_OPERATION_ACK", "CHANGEDB", "CHANGEDB_ACK", 
		"CHANGEREDIS", "CHANGEREDIS_ACK","SHUTDOWN_SESSION",
		"SHUTDOWN_SESSION_ACK", "REPORT_IN_MSG_COUNT", 
		"APPLY_IN_THROTTLE_QUOTA", "APPLY_IN_THROTTLE_QUOTA_ACK",
		"COMMAND_QUERY_HTTP_REQUEST", "COMMAND_QUERY_HTTP_ACK", "COMMAND_CONNECTION_HTTP_CONFIRM",
		"COMMAND_CONNECTION_CONFIRM_ACK"};

	private int retryTimes = 0;//needn't send in buffer 
	
	public SystemPdu() {
		TransactionURI uri = new TransactionURI();
		uuid = uri.toString();
		timestamp = Long.toString(System.currentTimeMillis());
		moduleManager = ModuleManager.getInstance();
	}
	
	public SystemPdu(SystemPdu pdu){
//		this.header = pdu.getHeader();
		this.header = new SystemPduHeader(pdu.getHeader());
		this.uuid = pdu.getUuid();
		this.timestamp = pdu.getTimestamp();
		this.moduleName = pdu.getModuleName();
		this.moduleType = pdu.getModuleType();
		assignSequenceNumber();
		moduleManager = ModuleManager.getInstance();
	}

	public boolean isRequest(){
		return true;
	}
	
	protected void appendParameterToBuffer(TcpByteBuffer buffer, int id,
			String value) throws NotEnoughDataInByteBufferException {
		if (value == null) {
			return;
		}

		buffer.appendBytes(new Parameter(id, value).toByteBuffer());
	}

	protected void appendParameterToBuffer(TcpByteBuffer buffer, int id,
			int value) throws NotEnoughDataInByteBufferException {
		appendParameterToBuffer(buffer, id, value, 2);
	}

	protected void appendParameterToBuffer(TcpByteBuffer buffer, int id,
			int value, int length) throws NotEnoughDataInByteBufferException {
		if (value == -1) {
			return;
		}

		buffer.appendBytes(new Parameter(id, value, length).toByteBuffer());
	}

	protected void appendParameterToBuffer(TcpByteBuffer buffer, int id,
			byte[] byteValue, int length)
			throws NotEnoughDataInByteBufferException {
		if (byteValue == null) {
			return;
		}

		buffer.appendBytes(new Parameter(id, byteValue, length).toByteBuffer());
	}

	protected void appendParameterToBuffer(TcpByteBuffer buffer, int id,
			byte[] byteValue) throws NotEnoughDataInByteBufferException {
		appendParameterToBuffer(buffer, id, byteValue, byteValue.length);
	}

	public void parsePduCommand(TcpByteBuffer buffer)
			throws NotEnoughDataInByteBufferException,
			UnsupportedEncodingException, UnknownParameterIdException {
		if (buffer == null) {
			return;
		}
		while (buffer.length() > 0) {
			int tag = buffer.removeBytesAsInt(1);
			int length = buffer.removeBytesAsInt(2);
			switch (tag) {
			case FIELD_UUID:
				uuid = buffer.removeString(length);
				break;
			case FIELD_TIMESTAMP:
				timestamp = buffer.removeString(length);
				break;
			default:
				log.warn("Cant find field with tag: {},len:{}",tag, length);
				buffer.removeBytes(length);
				break;
			}
		}
	}

	public TcpByteBuffer pduCommandToByteBuffer()
			throws NotEnoughDataInByteBufferException,
			UnsupportedEncodingException {

		TcpByteBuffer buffer = new TcpByteBuffer();

		appendParameterToBuffer(buffer, FIELD_UUID, uuid);
		appendParameterToBuffer(buffer, FIELD_TIMESTAMP, timestamp);
		return buffer;
	}

	private void parsePduBody(TcpByteBuffer buffer) throws InvalidPduException,
			NotEnoughDataInByteBufferException, MessageIncompleteException,
			UnknownCommandIdException, UnknownParameterIdException,
			UnsupportedEncodingException {

		if (header == null) {
			log.error("Header is null in parsePduBody!");
			return;
		}

		long bodyLength = header.getTotalLength() - PDU_HEADER_SIZE;
		if (buffer.length() < bodyLength) {
			throw new MessageIncompleteException();
		}
		TcpByteBuffer bodyBuffer = buffer.removeBytes((int) bodyLength);
		parsePduCommand(bodyBuffer);
	}
	
	 public void assignSequenceNumber() {
	    	int sequeceNO=0;
        	synchronized(sequemutex){//added by Jianming
        		if(sequenceNumber==Integer.MAX_VALUE){
        			sequenceNumber = 0;
        		}
        		sequeceNO = ++sequenceNumber;
        	}
            setSequenceNumber(sequeceNO);
    }
	 /**
     * Checks if the header field is null and if not, creates it.
     */
    private void checkHeader() {
        if(header == null) {
            header = new SystemPduHeader();
        }
    }
    
    public void setSequenceNumber(int seqNr) {
	        checkHeader();
	        header.setSequenceNumber(seqNr);
    }
    
    public int getSequenceNumber() {
        checkHeader();
        return header.getSequenceNumber();
    }
    
    public String getConfFileVersion() {
    	 checkHeader();
         return header.getConfFileVersion();
    }
    
	public static final SystemPdu createPdu(SystemPduHeader header) {
		if (header == null) {
			return null;
		}
		SystemPdu newInstance = null;

		int commId = header.getCommandId();
		switch (commId) {
			case COMMAND_KEEP_ALIVE:
				newInstance = new KeepAlive();
				break;
			case COMMAND_ACK:
				newInstance = new KeepAliveAck();
				break;
			case COMMAND_IN_BIND_REQUEST:
				newInstance = new InBindRequest();
				break;
			case COMMAND_IN_BIND_ACK:
				newInstance = new InBindAck();
				break;
			case COMMAND_OUT_BIND_REQUEST:
				newInstance = new OutBindRequest();
				break;
			case COMMAND_OUT_BIND_ACK:
				newInstance = new OutBindAck();
				break;
			case COMMAND_CONNECTION_STATUS_NOTIFICATION:
				newInstance = new ConnectionStatusNotification();
				break;
			case COMMAND_CONNECTION_STATUS_NOTIFICATION_ACK:
				newInstance = new ConnectionStatusNotificationAck();
				break;
			case COMMAND_CONNECTION_CONFIRM:
				newInstance = new ConnectionConfirm();
				break;
			case COMMAND_CONNECTION_CONFIRM_ACK:
				newInstance = new ConnectionConfirmAck();
				break;
			case COMMAND_MODULE_REGISTER_REQUEST:
				newInstance = new ModuleRegisterRequest();
				break;
			case COMMAND_MODULE_REGISTER_ACK:
				newInstance = new ModuleRegisterAck();
				break;
			case COMMAND_MODULE_STOP_REQUEST:
				newInstance = new ModuleStopRequest();
				break;
			case COMMAND_MODULE_STOP_ACK:
				newInstance = new ModuleStopAck();
				break;
			case COMMAND_DB_OPERATION_REQUEST:
				newInstance = new DBOperationRequest();
				break;
			case COMMAND_DB_OPERATION_ACK:
				newInstance = new DBOperationAck();
				break;
			case COMMAND_CHANGEDB:
				newInstance = new ChangeDB();
				break;
			case COMMAND_CHANGEDB_ACK:
				newInstance = new ChangeDBAck();
				break;	
			case COMMAND_CHANGEREDIS:
				newInstance = new ChangeRedis();
				break;		
			case COMMAND_CHANGEREDIS_ACK:
				newInstance = new ChangeRedisAck();
				break;	
			case COMMAND_SHUTDOWN_SESSION:
				newInstance = new ShutdownSession();
				break;
			case COMMAND_SHUTDOWN_SESSION_ACK:
				newInstance = new ShutdownSessionAck();
				break;
			case COMMAND_REPORT_IN_MSG_COUNT:
				newInstance = new ReportInMsgCount();
				break;
			case COMMAND_APPLY_IN_THROTTLE_QUOTA:
				newInstance = new ApplyInThrottleQuota();
				break;
			case COMMAND_APPLY_IN_THROTTLE_QUOTA_ACK:
				newInstance = new ApplyInThrottleQuotaAck();
				break;
			case COMMAND_QUERY_HTTP_REQUEST:
				newInstance = new QueryHttpRequest();
				break;
			case COMMAND_QUERY_HTTP_ACK:
				newInstance = new QueryHttpAck();
				break;
			case COMMAND_CONNECTION_HTTP_CONFIRM:
				newInstance = new ConnectionHttpConfirm();
				break;			
			default:
				log.warn("Error Command Type found in createPdu() method:{}"
								,commId);
		}
		if (newInstance != null) {
			newInstance.setHeader(header);
		}
		return newInstance;
	}

	public static final SystemPdu createPdu(int commandId) {
		SystemPdu newInstance = null;
		SystemPduHeader header = new SystemPduHeader();
		header.setCommandId(commandId);
		switch (commandId) {
			case COMMAND_KEEP_ALIVE:
				newInstance = new KeepAlive();
				break;
			case COMMAND_ACK:
				newInstance = new KeepAliveAck();
				break;
			case COMMAND_IN_BIND_REQUEST:
				newInstance = new InBindRequest();
				break;
			case COMMAND_IN_BIND_ACK:
				newInstance = new InBindAck();
				break;
			case COMMAND_OUT_BIND_REQUEST:
				newInstance = new OutBindRequest();
				break;
			case COMMAND_OUT_BIND_ACK:
				newInstance = new OutBindAck();
				break;
			case COMMAND_CONNECTION_STATUS_NOTIFICATION:
				newInstance = new ConnectionStatusNotification();
				break;
			case COMMAND_CONNECTION_STATUS_NOTIFICATION_ACK:
				newInstance = new ConnectionStatusNotificationAck();
				break;
			case COMMAND_CONNECTION_CONFIRM:
				newInstance = new ConnectionConfirm();
				break;
			case COMMAND_CONNECTION_CONFIRM_ACK:
				newInstance = new ConnectionConfirmAck();
				break;
			case COMMAND_MODULE_REGISTER_REQUEST:
				newInstance = new ModuleRegisterRequest();
				break;
			case COMMAND_MODULE_REGISTER_ACK:
				newInstance = new ModuleRegisterAck();
				break;
			case COMMAND_MODULE_STOP_REQUEST:
				newInstance = new ModuleStopRequest();
				break;
			case COMMAND_MODULE_STOP_ACK:
				newInstance = new ModuleStopAck();
				break;
			case COMMAND_DB_OPERATION_REQUEST:
				newInstance = new DBOperationRequest();
				break;
			case COMMAND_DB_OPERATION_ACK:
				newInstance = new DBOperationAck();
				break;
			case COMMAND_CHANGEDB:
				newInstance = new ChangeDB();
				break;
			case COMMAND_CHANGEDB_ACK:
				newInstance = new ChangeDBAck();
				break;	
			case COMMAND_CHANGEREDIS:
				newInstance = new ChangeRedis();
				break;		
			case COMMAND_CHANGEREDIS_ACK:
				newInstance = new ChangeRedisAck();
				break;	
			case COMMAND_SHUTDOWN_SESSION:
				newInstance = new ShutdownSession();
				break;
			case COMMAND_SHUTDOWN_SESSION_ACK:
				newInstance = new ShutdownSessionAck();
				break;
			case COMMAND_REPORT_IN_MSG_COUNT:
				newInstance = new ReportInMsgCount();
				break;
			case COMMAND_APPLY_IN_THROTTLE_QUOTA:
				newInstance = new ApplyInThrottleQuota();
				break;
			case COMMAND_APPLY_IN_THROTTLE_QUOTA_ACK:
				newInstance = new ApplyInThrottleQuotaAck();
				break;	
			case COMMAND_QUERY_HTTP_REQUEST:
				newInstance = new QueryHttpRequest();
				break;
			case COMMAND_QUERY_HTTP_ACK:
				newInstance = new QueryHttpAck();
				break;
			case COMMAND_CONNECTION_HTTP_CONFIRM:
				newInstance = new ConnectionHttpConfirm();
				break;		
			default:
				log.warn("Error Command Type found in createPdu() method:{}"
								,commandId);
		}
		if (newInstance != null) {
			newInstance.setHeader(header);
		}
		return newInstance;
	}

	public TcpByteBuffer toByteBuffer()
			throws NotEnoughDataInByteBufferException,
			UnsupportedEncodingException {

		TcpByteBuffer body = pduCommandToByteBuffer();
		if (body != null) {
			header.setTotalLength(body.length() + SystemPdu.PDU_HEADER_SIZE);
		} else {
			header.setTotalLength(SystemPdu.PDU_HEADER_SIZE);
		}

		TcpByteBuffer buffer = header.toByteBuffer();

		if (body != null) {
			buffer.appendBytes(body);
		}
		
//		log.trace("Send {}", this);
		return buffer;
	}

	public int getCommandId() {
		if (header == null) {
			return -1;
		}

		return header.getCommandId();
	}
	/**
	 * for debug
	 * @return
	 */
	public String getCommandType() {
		if (header == null) {
			return null;
		}
		return SystemPdu.commandTypeList[header.getCommandId()-1];
	}
	/**
	 * Insert the method's description here. Creation date: (1/8/2013 10:29:32
	 * AM)
	 * 
	 * @param totalLength
	 *            long
	 */
	public void setTotalLength(long totalLength) {
		header.setTotalLength(totalLength);
	}

	public static final SystemPdu createPdu(TcpByteBuffer buffer)
			throws MessageIncompleteException, UnknownCommandIdException,
			NotEnoughDataInByteBufferException, HeaderIncompleteException,
			UnsupportedCommandIdException, UnknownParameterIdException,
			InvalidPduException, UnsupportedEncodingException {
		TcpByteBuffer headerBuf = null;
		try {
			headerBuf = buffer.readBytes(PDU_HEADER_SIZE);
		} catch (NotEnoughDataInByteBufferException e) {
			log.warn(e, e);
			throw new HeaderIncompleteException();
		}

		SystemPduHeader header = null;
		try {
			header = SystemPduHeader.parseHeader(headerBuf);
		} catch (NotEnoughDataInByteBufferException e) {
			log.warn(e, e);
			throw new NotEnoughDataInByteBufferException(e.getMessage());
		}

		if (buffer.length() < header.getTotalLength()) {
			throw new MessageIncompleteException();
		}

		buffer.removeBytes(PDU_HEADER_SIZE);
		SystemPdu pdu = createPdu(header);
		if (pdu != null) {
			pdu.parsePduBody(buffer);
//			log.trace("parse PDU:{}", pdu);
			return pdu;
		} else {
			throw new UnknownCommandIdException();
		}
	}

	public static final TcpByteBuffer getPduByteBuffer(TcpByteBuffer buffer)
			throws MessageIncompleteException, UnknownCommandIdException,
			NotEnoughDataInByteBufferException, HeaderIncompleteException,
			UnsupportedCommandIdException, UnknownParameterIdException,
			InvalidPduException, UnsupportedEncodingException {
		TcpByteBuffer headerBuf = null;
		try {
			headerBuf = buffer.readBytes(PDU_HEADER_SIZE);
		} catch (NotEnoughDataInByteBufferException e) {
			log.warn(e, e);
			throw new HeaderIncompleteException();
		}

		SystemPduHeader header = null;
		try {
			header = SystemPduHeader.parseHeader(headerBuf);
		} catch (NotEnoughDataInByteBufferException e) {
			log.warn(e, e);
			throw new MessageIncompleteException(e.getMessage());
		}

		int totalLength = (int) header.getTotalLength();

		if (buffer.length() < totalLength) {
			throw new MessageIncompleteException();
		} else {
			return buffer.removeBytes(totalLength);
		}
	}

	public SystemPduHeader getHeader() {
		return header;
	}

	public void setHeader(SystemPduHeader header) {
		this.header = header;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}
	public String toString() {
        return new StringBuffer("SequenceNumber:").append(header.getSequenceNumber()).append(",")
            .append("uuid:").append(uuid).append(",")
            .append("timestamp:").append(timestamp)
            .toString();
    }

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	/**
	 * retry 
	 */
	public void nextRetry() {
        retryTimes++;
    }

	public int getRetryTimes() {
		return retryTimes;
	}

	public void setRetryTimes(int retryTimes) {
		this.retryTimes = retryTimes;
	}
	public String getModuleName() {
		if(moduleName!=null){
    		return moduleName;
    	}
		//generate TransactionURI
		TransactionURI transURI = TransactionURI.fromString(uuid);
		//get moduleName
		moduleName = transURI.getModule().getModule();
		return moduleName;
	}
	/**
     * getModuleType
     */
    public String getModuleType(){
    	if(moduleType!=null){
    		return moduleType;
    	}
		//generate TransactionURI
		TransactionURI transURI = TransactionURI.fromString(uuid);
		//get moduleName
		moduleName = transURI.getModule().getModule();
		//get moduleType
		moduleType =  moduleManager.getModuleType(moduleName);
		return moduleType;
    }
}
