package it;

import com.king.gmms.client.MultiSmppClient;
import com.king.gmms.messagequeue.OutboundStreamConsumer;

/** One real Client process of ClientFailoverIT: its own home, module and node, on the shared Redis. */
public class ClientNode extends ItSupport {
	public static void main(String[] a) throws Exception {
		boot();
		new MultiSmppClient().startService();
		OutboundStreamConsumer.getInstance().start();
		System.out.println("READY");
		Thread.sleep(Long.MAX_VALUE);
	}
}
