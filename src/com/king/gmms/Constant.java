package com.king.gmms;

/**
 * <p>Title: Constants</p>
 * <p/>
 * <p>Description: provide some common constants.</p>
 * <p/>
 * <p>Copyright: Copyright (c) 2004</p>
 * <p/>
 */
public class Constant {
    public static final String IOSMS_SERVICE = "IO-SMS";
    public static final String GMMS_SERVICE = "GMMS";
    public static final String GMD_IOSMS_SYSTEM = "IO-SMS";
    public static final String ROUTING_SUFFER_FEX = "_routingInfo.cfg";
    public static final String SENDER_BLACK_SUFFER_FEX = "_sender_blacklist.cfg";
    public static final String SENDER_WHITLE_SUFFER_FEX = "_sender_whitelist.cfg";
    public static final String CONTENT_BLACK_SUFFER_FEX = "_content_blacklist.cfg";
    public static final String CONTENT_WHITLE_SUFFER_FEX = "_content_whitelist.cfg";
    public static final String RECIPIENT_BLACK_SUFFER_FEX = "_recipient_blacklist.cfg";
    public static final String ROUTING_VENDOR_REPLACEMENT = "vendorRoutingReplacement.cfg";
    public static final String DEFAULT_ROUTING_SUFFER = "_default";
    public static final String GMD_GMMS_SYSTEM = "GMMS";
    public static final String GMD_IOSMS_GMMS_SYSTEM = "GMMSANDIO-SMS";
    public static final String A2P = "A2P";
    public static final String P2P = "P2P";
    public static final String CONTENT_TEMPLATE_KEYWORD = "$TEMPALTE MESSAGE$:";
    public static final String CONTENT_SIGNATURE_KEYWORD = "$SIGNATURE ID$=";
    public static final String AMR_DELIVERYCHANNEL =
            "com.king.gmms.channel.AmrDeliveryChannel";
    
    
    public static void main(String[] args) {
		String textContent = "$TEMPALTE MESSAGE$:1=456$SIGNATURE ID$=1";
		int signatureIndex = textContent.indexOf(Constant.CONTENT_SIGNATURE_KEYWORD);
		int templateIndex = textContent.indexOf(Constant.CONTENT_TEMPLATE_KEYWORD);
		String template = "";
		String signature = "";
		if (templateIndex>-1) {
			if (signatureIndex>0) {
				template = textContent.substring(templateIndex+Constant.CONTENT_TEMPLATE_KEYWORD.length(), signatureIndex);
				signature = textContent.substring(signatureIndex+Constant.CONTENT_SIGNATURE_KEYWORD.length(), textContent.length());						
			}else {
				template = textContent.substring(templateIndex+Constant.CONTENT_TEMPLATE_KEYWORD.length(), textContent.length());
			}
	    	
		}
		
		String templateId = template.trim();
		String paramters = "";
		int templateIdIndex = template.indexOf("=");
    	if (templateIdIndex>-1) {
			templateId = template.substring(0, templateIdIndex).trim();
			paramters = template.substring(templateIdIndex+1).trim();
		}
		
		
		System.out.println(templateId);
		System.out.println(paramters);
		System.out.println(signature);
		System.out.println(template);
	}
}
