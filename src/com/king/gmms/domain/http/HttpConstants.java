package com.king.gmms.domain.http;

public class HttpConstants {
	public static final String ELEM_TYPE_MO = "MO";
	public static final String ELEM_TYPE_MT = "MT";
	public static final String ELEM_TYPE_SUBMITREQUEST = "SubmitRequest";
	public static final String ELEM_TYPE_SUBMITRESPONSE = "SubmitResponse";
	public static final String ELEM_TYPE_DRREQUEST = "DRRequest";
	public static final String ELEM_TYPE_DRRESPONSE = "DRResponse";
	public static final String ELEM_TYPE_CHARSETMAPPING = "CharsetMapping";
	public static final String ELEM_TYPE_HANDlERCLASS = "HandlerClass";
	public static final String ELEM_TYPE_USERNAME = "username";
	public static final String ELEM_TYPE_PASSWORD = "password";
	public static final String ELEM_TYPE_STATUSMAPPING = "StatusMapping";
	public static final String ELEM_TYPE_UDH = "udh";
	public static final String ELEM_TYPE_CODE = "code";
	public static final String ELEM_TYPE_TEXT = "text";
	public static final String ELEM_TYPE_RESPONSEDELIMITER = "ResponseDelimiter";
	public static final String ELEM_TYPE_PARAMETERDELIMITER = "ParameterDelimiter";

	public static final String ELEM_ATTR_FORMAT = "format";
	public static final String ELEM_ATTR_TYPE = "type";
	public static final String ELEM_ATTR_TYPE_CCB = "ccb";
	public static final String ELEM_ATTR_VALUE = "value";
	public static final String ELEM_ATTR_ENCODING = "encoding";
	public static final String ELEM_ATTR_TEXT = "text";

	public static final String MO_SUBMIT_REQUEST = "moSubmitReqeust";
	public static final String MO_SUBMIT_RESPONSE = "moSubmitResponse";
	public static final String MO_DR_REQUEST = "moDRReqeust";
	public static final String MO_DR_RESPONSE = "moDRResponse";
	public static final String MT_SUBMIT_REQUEST = "mtSubmitReqeust";
	public static final String MT_SUBMIT_RESPONSE = "mtSubmitResponse";
	public static final String MT_DR_REQUEST = "mtDRReqeust";
	public static final String MT_DR_RESPONSE = "mtDRResponse";
	 
	public static final String HANDLER_METHOD_MAKEREQUEST = "makeRequest";
	public static final String HANDLER_METHOD_PARSEREQUEST = "parseRequest";
	public static final String HANDLER_METHOD_MAKERESPONSE = "makeResponse";
	public static final String HANDLER_METHOD_PARSERESPONSE = "parseResponse";
	public static final String HANDLER_METHOD_PARSELISTRESPONSE = "parseListResponse";
	public static final String HANDLER_METHOD_PARSELISTREQUEST = "parseListRequest";
	public static final String WS_HANDLER_METHOD_NEWREQUEST = "newRequest";
	public static final String WS_HANDLER_METHOD_DRREQUEST = "drRequest";
	public static final String WS_HANDLER_METHOD_PARSEMOLISTREQUEST = "parseMORequestList";
	public static final String WS_HANDLER_METHOD_PARSMTELISTREQUEST = "parseMTDRRequestList";
	public static final String HTTP_METHOD_WEBSERVICE = "webservice";
	public static final String HTTP_SEC_SESSION = "secSession";

	public static final String COMMON_MSG_HANDLERCLASS = "com.king.gmms.protocol.commonhttp.CommonMessageHttpHandler";
	public static final String COMMON_DR_HANDLERCLASS = "com.king.gmms.protocol.commonhttp.CommonDeliveryReportHttpHandler";
	
	public static final String EXPIRYDATE_TYPE_RELATIVE = "RELATIVE";
	public static final String EXPIRYDATE_TYPE_ABSOLUTE = "ABSOLUTE";
	
	//add by kevin for REST
	
	public static final String MEDIA_TYPE_XML="application/xml";
	
	public static final String MEDIA_TYPE_JSON="application/json";
	
	public static final String MEDIA_TYPE_URLENCODE="application/x-www-form-urlencoded";
	
	public static final String COMMON_MSG_REST_XML_HANDLERCLASS="com.king.gmms.protocol.commonhttp.CommonMessageRESTXmlHttpHandler";
	public static final String COMMON_MSG_REST_JSON_HANDLERCLASS="com.king.gmms.protocol.commonhttp.CommonMessageRESTJsonHttpHandler";
	public static final String COMMON_DR_REST_XML_HANDLERCLASS = "com.king.gmms.protocol.commonhttp.CommonDeliveryReportRESTXmlHttpHandler";
	public static final String COMMON_DR_REST_JSON_HANDLERCLASS = "com.king.gmms.protocol.commonhttp.CommonDeliveryReportRESTJsonHttpHandler";
	
	public static final String COMMON_DR_QUERY_REST_XML_HANDLERCLASS="com.king.gmms.protocol.commonhttp.CommonDeliveryReportQueryRESTXmlHttpHandler";
	public static final String COMMON_DR_QUERY_REST_JSON_HANDLERCLASS="com.king.gmms.protocol.commonhttp.CommonDeliveryReportQueryRESTJsonHttpHandler";
	
	public static final String SEC_COMMON_MSG_HANDLERCLASS="com.king.gmms.protocol.commonhttp.SecCommonMessageHttpHandler";
	public static final String SEC_COMMON_DELIVERY_REPORT_HANDLERCLASS="com.king.gmms.protocol.commonhttp.SecCommonDeliveryHttpHandler";
	
    public static final String HTTP_METHOD_REST_JSON="REST/JSON";
	
	public static final String HTTP_METHOD_REST_XML="REST/XML";
	
	public static final String HTTP_METHOD_REST_URLENCODE="REST/URLENCODE";
	
	public static final String ELEM_ATTR_TAYPE_CCB_NAME="ccbname";
	
	public static final String MO_DR_QUERY_REQUEST="moDRQueryReqeust";
	
	public static final String MO_DR_QUERY_RESPONSE="moDRQueryResponse";
}
