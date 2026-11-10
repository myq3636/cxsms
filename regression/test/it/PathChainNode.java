package it;

import com.king.gmms.client.MultiSmppClient;
import com.king.gmms.listener.smppserver.MultiSmppServer;
import com.king.gmms.messagequeue.InboundDRStreamConsumer;
import com.king.gmms.messagequeue.MTStreamConsumer;
import com.king.gmms.messagequeue.OutboundStreamConsumer;
import com.king.gmms.messagequeue.ResultStreamConsumer;

/** One real process of PathChainIT: server PORT, core, or client; its own home, module and node, on the shared Redis. */
public class PathChainNode extends ItSupport {
	public static void main(String[] a) throws Exception {
		boot();
		if ("server".equals(a[0])) {
			props.setProperty(System.getProperty("module") + ".Port", a[1]);
			new MultiSmppServer().startService();
		} else if ("core".equals(a[0])) {
			MTStreamConsumer.getInstance().start();
			ResultStreamConsumer.getInstance().start();
			InboundDRStreamConsumer.getInstance().start();
		} else {
			new MultiSmppClient().startService();
			OutboundStreamConsumer.getInstance().start();
		}
		System.out.println("READY");
		Thread.sleep(Long.MAX_VALUE);
	}
}
