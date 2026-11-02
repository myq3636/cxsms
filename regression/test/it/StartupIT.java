package it;

import com.king.gmms.GmmsUtility;

/** Core starts without conf/senderReplacement.cfg (NPE fix); not part of the repo. */
public class StartupIT extends ItSupport {
	public static void main(String[] args) throws Exception {
		boolean started;
		try {
			boot();
			started = GmmsUtility.getInstance().getCustomerManager() != null
					&& GmmsUtility.getInstance().getCustomerManager().getCustomerBySSID(8888) != null;
		} catch (Throwable t) {
			t.printStackTrace(System.out);
			started = false;
		}
		check(!new java.io.File(home + "conf/senderReplacement.cfg").exists(), "NPE-03a setup: senderReplacement.cfg absent");
		check(started, "NPE-03b Core starts and loads cm.cfg without senderReplacement.cfg");
		exit();
	}
}
