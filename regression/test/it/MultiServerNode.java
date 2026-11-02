package it;

import com.king.gmms.listener.smppserver.MultiSmppServer;

/** One real server process of MultiServerIT: its own home, module, node and port, on the shared Redis. */
public class MultiServerNode extends ItSupport {
	public static void main(String[] a) throws Exception {
		boot();
		props.setProperty(System.getProperty("module") + ".Port", a[0]);
		props.setProperty("SmppServerSessionRegistryReconcileIntervalMs", "2000");
		new MultiSmppServer().startService();
		System.out.println("READY");
		Thread.sleep(Long.MAX_VALUE);
	}
}
