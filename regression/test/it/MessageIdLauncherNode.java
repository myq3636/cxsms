package it;

import java.lang.reflect.Field;

import com.king.gmms.GmmsUtility;
import com.king.gmms.GmmsLauncher;

/** The launcher production starts modules with. The test home has no phone.dat, which GmmsUtility reports through a logger it only gets later. */
public class MessageIdLauncherNode {
	public static void main(String[] a) throws Exception {
		Field lf = GmmsUtility.class.getDeclaredField("log");
		lf.setAccessible(true);
		if (lf.get(null) == null) lf.set(null, com.king.framework.SystemLogger.getSystemLogger(GmmsUtility.class));
		GmmsLauncher.main(a);
	}
}
