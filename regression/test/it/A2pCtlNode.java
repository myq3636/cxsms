package it;

import java.lang.reflect.Field;

import com.king.gmms.GmmsUtility;
import com.king.gmms.ha.A2pCtl;

/** The real A2pCtl. The test home has no phone.dat, which GmmsUtility reports through a logger it only gets later. */
public class A2pCtlNode {
	public static void main(String[] a) throws Exception {
		Field lf = GmmsUtility.class.getDeclaredField("log");
		lf.setAccessible(true);
		if (lf.get(null) == null) lf.set(null, com.king.framework.SystemLogger.getSystemLogger(GmmsUtility.class));
		A2pCtl.main(a);
	}
}
