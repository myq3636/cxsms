package com.king.rest.util;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SimpleTimeZone;
import java.util.TreeMap;
import java.util.UUID;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.DigestUtils;

import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.session.CommonHttpSession;

import sun.misc.BASE64Encoder;


/**
 * 
 * 阿里云短信签名机制
 * 
 * @author yuj
 *
 * 
 * 
 */

public class SMSSignatureUtil {

    private static final String ACTION = "SendMessageToGlobe";
    private static final String JSON = "JSON";
    private static final String REGIONID = "ap-southeast-1";
    private static final String SIGNATUREMETHOD = "HMAC-SHA1";
    private static final String SIGNATUREVERSION = "1.0";
    private static final String VERSION = "2018-05-01";
    private static SystemLogger log = SystemLogger
			.getSystemLogger(SMSSignatureUtil.class);
    /**
     * 
     * 签名结果串-阿里云短信
     * 
     * @param accessKeyId    - testid
     * @param jsonParams     - {\"name\":\"d\",\"name1\":\"d\"}
     * @param recNum         - 13098765432
     * @param signName       - 标签测试
     * @param signatureNonce - 9e030f6b-03a2-40f0-a6ba-157d44532fd0
     * @param templateCode   - SMS_1650053
     * @param timestamp      - 2016-10-20T05:37:52Z
     * @return signature - ka8PDlV7S9sYqxEMRnmlBv%2FDoAE%3D
     * 
     */

    public static String getSignature(String accessKeyId, String from,

            String msg, String to, String accessSercurt, String signatureNonce, String timestamp, String customValue){

        String signature = "";
        try {
            Map<String,String> parameters = new HashMap<>();
            parameters.put("AccessKeyId", accessKeyId);
            parameters.put("Action", ACTION);
            parameters.put("Format", "JSON");
            // parameters.put("RecNum", recNum);
            //parameters.put("RegionId", REGIONID);
            // parameters.put("SignName", signName);
            parameters.put("SignatureMethod", SIGNATUREMETHOD);
            parameters.put("SignatureNonce", signatureNonce);
            //parameters.put("SignatureNonce", "57acef20-c1d8-11eb-8c08-db81fda24dcc");
            parameters.put("SignatureVersion", SIGNATUREVERSION);
            parameters.put("From", from);
            parameters.put("Message", msg);
            parameters.put("To", to);
            parameters.put("Type", customValue);
            
            // parameters.put("TemplateCode", templateCode);
            Calendar calendar=Calendar.getInstance();
			calendar.clear();
			
			calendar.set(2021, 04, 31, 14, 20, 49);
			
            parameters.put("Timestamp", timestamp);
			//parameters.put("Timestamp", formatIso8601Date(calendar.getTime()));
            parameters.put("Version", VERSION);
            // parameters.put("ParamString", jsonParams);
            // 对参数进行排序
            String[] sortedKeys = (String[]) parameters.keySet().toArray(new String[] {});
            Arrays.sort(sortedKeys);
            final String SEPARATOR = "&";
            // 生成stringToSign字符串
            StringBuilder stringToSign = new StringBuilder();
            stringToSign.append("POST").append(SEPARATOR);
            stringToSign.append(percentEncode("/")).append(SEPARATOR);
            StringBuilder canonicalizedQueryString = new StringBuilder();
            for (String key : sortedKeys) {

                // 这里注意对key和value进行编码

                canonicalizedQueryString.append("&").append(percentEncode(key))

                        .append("=").append(percentEncode((String) parameters.get(key)));

            }

            // 这里注意对canonicalizedQueryString进行编码

            stringToSign.append(percentEncode(canonicalizedQueryString.toString()

                    .substring(1)));
            final String ALGORITHM = "HmacSHA1";
            final String ENCODING = "UTF-8";
            String key = accessSercurt+"&";
            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(new SecretKeySpec(key.getBytes(ENCODING), ALGORITHM));
            byte[] signData = mac.doFinal(stringToSign.toString().getBytes("utf8"));
            BASE64Encoder base64=new BASE64Encoder();
            signature = base64.encode(signData);
        } catch (InvalidKeyException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (IllegalStateException e) {
            e.printStackTrace();
        }
        return signature;
    }
    
    static void putIfNotEmpty(TreeMap<String, String> map, String key, String value) {
        if (value!=null && !value.isEmpty()) {
            map.put(key, value);
        }
    }

    
    public static String getCainiaoSignature(String supplier, 
            String method, String requestId, String timestamp, 
            String phoneNumber, String senderId,String content, String smsType, String extend, String authkey){

        String signature = "";
        try {
        	TreeMap<String,String> parameters = new TreeMap<>();
            putIfNotEmpty(parameters, "supplier", supplier);
            putIfNotEmpty(parameters, "timestamp", timestamp);
            putIfNotEmpty(parameters, "method", method);
            putIfNotEmpty(parameters, "requestId", requestId);
            putIfNotEmpty(parameters, "phoneNumber", phoneNumber);
            putIfNotEmpty(parameters, "senderId", senderId);
            putIfNotEmpty(parameters, "content", content);
            putIfNotEmpty(parameters, "smsType", smsType);
            putIfNotEmpty(parameters, "extend", extend);

            String body = buildStr(parameters);
            String finalBody = authkey + body;            
            byte[] digest = md5(finalBody.getBytes(StandardCharsets.UTF_8));
            signature = base64Encode(digest);
            

            //signature = percentEncode(signature);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return signature;
    }
    
    public static String getAliCainiaoDRSignature(String authkey, Map<String, Object> parameters){

        String signature = "";
        try {            
            // 对参数进行排序
            String[] sortedKeys = (String[]) parameters.keySet().toArray(new String[] {});
            Arrays.sort(sortedKeys);
            // 生成stringToSign字符串
            StringBuilder stringToSign = new StringBuilder();            
            StringBuilder canonicalizedQueryString = new StringBuilder();
            for (String key : sortedKeys) {
                // 这里注意对key和value进行编码
                canonicalizedQueryString.append("&").append(specialUrlEncode(key))

                        .append("=").append(specialUrlEncode(String.valueOf(parameters.get(key))));

            }

            // 这里注意对canonicalizedQueryString进行编码
            stringToSign.append(specialUrlEncode(canonicalizedQueryString.toString()
                    .substring(1)));
            String body = stringToSign.toString();
            String finalBody = authkey + body;
            byte[] digest = md5(finalBody.getBytes(StandardCharsets.UTF_8));
            signature = base64Encode(digest);        
            
        } catch (Exception e) {
            log.error("getAliCainiaoDRSignature throw exception", e);
        }
        return signature;
    }
    
    public static byte[] md5(byte[] data) {
        return DigestUtils.md5(data);
    }

    public static String base64Encode(byte[] value) {
        return new String(Base64.encodeBase64(value));
    }

    
    private static String buildStr(TreeMap<String, String> map) throws UnsupportedEncodingException {

        Iterator<String> it = map.keySet().iterator();
        StringBuilder sb = new StringBuilder();
        while (it.hasNext()) {
            String key = it.next();
            if (key!=null) {
                sb.append("&")
                        .append(specialUrlEncode(key))
                        .append("=")
                        .append(specialUrlEncode(map.get(key)));
            }
        }
        return specialUrlEncode(sb.substring(1));
    }

    private static String specialUrlEncode(String value) throws UnsupportedEncodingException {
        return URLEncoder.encode(value, "UTF-8");
    }


    public static String percentEncode(String value) throws UnsupportedEncodingException {

        return value != null
                ? URLEncoder.encode(value, "utf8").replace("+", "%20").replace("*", "%2A").replace("%7E", "~")
                : null;
    }

    private static final String ISO8601_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    public static String formatIso8601Date(Date date) {
        SimpleDateFormat df = new SimpleDateFormat(ISO8601_DATE_FORMAT);
        df.setTimeZone(new SimpleTimeZone(0, "GMT"));
        return df.format(date);
    }
        
    public static void main(String[] args) {
		//http://dysmsapi.ap-southeast-1.aliyuncs.com/?
    	//AccessKeyId=testid&Action=SendMessageToGlobe&
    	//Format=XML&From=Alicloud&Message=Hello&RegionId=ap-southeast-1
    	//&SignatureMethod=HMAC-SHA1&SignatureNonce=57acef20-c1d8-11eb-8c08-db81fda24dcc
    	//&SignatureVersion=1.0&Timestamp=2021-05-31T06%3A20%3A49Z&To=861245567%2A%2A%2A%2A&Version=2018-05-01
    	//2021-05-31T06:20:49Z
    	//861245567**** "yyyy-MM-dd HH:mm:ss"
    	//GET&%2F&AccessKeyId%3Dtestid%26Action%3DSendMessageToGlobe%26Format%3DXML%26From%3DAlicloud%26Message%3DHello%26RegionId%3Dap-southeast-1%26SignatureMethod%3DHMAC-SHA1%26SignatureNonce%3D57acef20-c1d8-11eb-8c08-db81fda24dcc%26SignatureVersion%3D1.0%26Timestamp%3D2021-05-31T06%253A20%253A49Z%26To%3D861245567%252A%252A%252A%252A%26Version%3D2018-05
		/*
		 * interSmsRequest.setSupplier("CAINIAO");
		 * interSmsRequest.setTimestamp("123123"); interSmsRequest.setMethod("MD5");
		 * interSmsRequest.setRequestId("abcdefgh");
		 * 
		 * interSmsRequest.setPhoneNumber("8613811111");
		 * interSmsRequest.setSenderId("Cainiao"); interSmsRequest.setContent("Hello!");
		 * interSmsRequest.setSmsType("OTP");
		 * 
		 * getCainiaoSignature(String supplier, String method, String
		 * requestId, String timestamp, String phoneNumber, String senderId,String
		 * content, String smsType, String extend, String authkey )
		 */
    	
    	System.out.println(getCainiaoSignature("12345","MD5", "2e1f23088d9848988b38", "Thu, 28 Jul 2022 02:35:07 GMT", "818838917791","Cainiao", 
    			"nihha","验证码", "{}", "45678"));
    	
	}

}
