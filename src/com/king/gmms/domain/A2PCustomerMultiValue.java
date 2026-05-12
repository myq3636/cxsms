package com.king.gmms.domain;

import java.util.ArrayList;
import java.util.HashMap;

import com.king.framework.SystemLogger;

public class A2PCustomerMultiValue {

	ArrayList<Group> groups = new ArrayList<Group>();

	public A2PCustomerMultiValue() {
		super();
	}

	public boolean parse(String conf) {
		if (conf == null)
			return false;
		String[] seg = conf.split(A2PCustomerConfig.GROUP_SEP);
		for (int i = 0; i < seg.length; i++) {
			if (seg[i] == null || seg[i].trim().length() == 0)
				continue;
			Group g = parseGroup(seg[i]);
			if (g == null)
				return false;
			groups.add(g);
		}
		return true;
	}
	
	public boolean parseRouting(String conf) {
		if (conf == null)
			return false;
		String[] seg = conf.split("\\"+A2PCustomerConfig.GROUP_END+A2PCustomerConfig.GROUP_SEP);
		for (int i = 0; i < seg.length; i++) {			
			if (seg[i] == null || seg[i].trim().length() == 0)
				continue;
			String value = seg[i];
			if (!seg[i].endsWith(A2PCustomerConfig.GROUP_END)) {
				value = seg[i]+A2PCustomerConfig.GROUP_END;
			}
			Group g = parseGroup(value);
			if (g == null)
				return false;
			groups.add(g);
		}
		return true;
	}

	public Group parseGroup(String str) {
		if (str == null || str.trim().length() == 0)
			return null;
		if (str.startsWith(A2PCustomerConfig.GROUP_START)
				&& str.endsWith(A2PCustomerConfig.GROUP_END)) {
			str = str.substring(1, str.length() - 1);
		}
		Group g = new Group();
		String[] attrs = str.split(A2PCustomerConfig.VALUE_SEP);
		for (int i = 0; i < attrs.length; i++) {
			if (attrs[i] == null || attrs[i].trim().length() == 0)
				continue;

			AttrPair attr = new AttrPair();
			if (attr.parse(attrs[i])) {
				g.addAttr(attr);
			}
		}
		return g;
	}

	public int getTotalGroups() {
		return groups.size();
	}

	public ArrayList<Group> getAllGroups() {
		return groups;
	}

	public ArrayList<AttrPair> getAllAttrs() {
		if (groups.size() >= 1)
			return groups.get(0).getAllAttrs();
		return null;
	}

	public AttrPair getAttr(String name) {
		if (groups.size() >= 1) {
			return groups.get(0).getAttr(name);
		}
		return null;
	}
	
	public static void main(String[] args) {
		String string = "(PhonePrefix=86,Relay=SMSHighWay_RO_SMS);(PhonePrefix=86;876,Relay=SMSHighWay_RO_SMS)";
		String s[] = string.split("\\"+A2PCustomerConfig.GROUP_END+A2PCustomerConfig.GROUP_SEP);
		for(String t: s) {
			if (t.endsWith("\\"+A2PCustomerConfig.GROUP_END)) {
				System.out.println(t);
			}else {
				System.out.println(t+1);
			}
			
		}
	}
}
