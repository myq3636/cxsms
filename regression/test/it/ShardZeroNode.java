package it;

import com.king.gmms.GmmsUtility;
import com.king.gmms.ha.ModuleStatusReporter;
import com.king.gmms.messagequeue.MTStreamConsumer;

/** One real Core process of ShardZeroIT: as CoreNode, with the module status heartbeat that publishes its shards. */
public class ShardZeroNode extends ItSupport {
	public static void main(String[] a) throws Exception {
		boot();
		GmmsUtility g = GmmsUtility.getInstance();
		ModuleStatusReporter.start(g, "core", System.getProperty("module"), g.getNodeId());
		MTStreamConsumer.getInstance().start();
		System.out.println("READY");
		Thread.sleep(Long.MAX_VALUE);
	}
}
