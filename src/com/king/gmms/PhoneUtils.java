package com.king.gmms;

import com.google.i18n.phonenumbers.NumberParseException;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber;

import java.util.Set;


public class PhoneUtils {

    private static PhoneNumberUtil phoneNumberUtil = PhoneNumberUtil.getInstance();

    public static final String PLUS_SIGN = "+";


    /**
     * 获取区号。例如好几个国家使用同一个 contry code，需要区号来区分手机号属于哪个国家
     *
     * @param phone
     * @return
     */
    public static String getRegionCodeByPhone(final String phone) {
        final Phonenumber.PhoneNumber phoneNumber = parse(phone);
        return phoneNumber != null ? phoneNumberUtil.getRegionCodeForNumber(phoneNumber) : "null";
        //return phoneNumber != null ? phoneNumberUtil.getRegionCodeForNumber(phoneNumber) : null;
        //        System.out.println(PhoneNumberToCarrierMapper.getInstance().getNameForNumber(phoneNumber,Locale.ENGLISH));
        //        System.out.println(PhoneNumberOfflineGeocoder.getInstance().getDescriptionForNumber(phoneNumber,Locale.ENGLISH));
    }


    public static boolean isValidRegionCode(String regionCode) {
        return regionCode != null && phoneNumberUtil.getSupportedRegions().contains(regionCode);
    }
    

    private static int getCountryCode0(final String phone) {
        final Phonenumber.PhoneNumber phoneNumber = parse(phone);
        return phoneNumber != null ? phoneNumber.getCountryCode() : -1;
    }


    public static Phonenumber.PhoneNumber parse(String phone) {
    	String recipientAddress = phone;
		if (recipientAddress.startsWith("+")) {
			recipientAddress = recipientAddress.substring(1);
		}
		if (recipientAddress.startsWith("00")) {
			recipientAddress = recipientAddress.substring(2);
		}
		if (recipientAddress.startsWith("0")) {
			recipientAddress = recipientAddress.substring(1);
		}
		recipientAddress = "+"+recipientAddress;
        try {
            return phoneNumberUtil.parse(recipientAddress, "");
        } catch (NumberParseException e) {
        }
        return null;
    }


    public static boolean isFullPhoneNumber(String phone) {
        try {
            phoneNumberUtil.parse(phone, "");
            return true;
        } catch (NumberParseException e) {
        }
        return false;
    }
    
    /**
     * 手机号格式化成带"00"号的形式，例如： 008613812345678 date:2019-08-06
     */
    public static String formatPhoneWith00IfNoPrefix(String phone) {
        if (phone == null) {
            return phone;
        }
        String filterPhone = phone.replaceAll("-|\\s+|\\(|\\)", "");
        if (filterPhone.startsWith("00") || filterPhone.startsWith(PLUS_SIGN)) {
            return phone;
        } else {
            filterPhone = "00" + filterPhone;
            return filterPhone;
        }
    }


    public static void main(String[] args) {
    	String mobile = "628201109958";
    	String countryCode = PhoneUtils.getRegionCodeByPhone(mobile);
    	//int countryCode = phoneNumberUtil.getCountryCodeForRegion("US");
    	System.out.println(countryCode);
	}
}
