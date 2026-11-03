package it;

import com.king.gmms.messagequeue.MTStreamConsumer;

/** One real Core process of CoreFailoverIT: its own home, module and node, on the shared Redis. */
public class CoreNode extends ItSupport {
	public static void main(String[] a) throws Exception {
		boot();
		MTStreamConsumer.getInstance().start();
		System.out.println("READY");
		Thread.sleep(Long.MAX_VALUE);
	}
}
