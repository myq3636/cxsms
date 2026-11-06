package it;

import com.king.gmms.GmmsUtility;
import com.king.gmms.ha.ModuleStatusReporter;
import com.king.gmms.messagequeue.ChannelReleaseDispatcher;
import com.king.gmms.messagequeue.InboundDRStreamConsumer;
import com.king.gmms.messagequeue.MTStreamConsumer;
import com.king.gmms.messagequeue.ResultStreamConsumer;
import com.king.gmms.processor.CsmExpirySweeper;

/** One real Core process of DrainCoreIT and ModuleOpsIT: the stream consumers of a Core, its heartbeat, SQL kept in memory. */
public class DrainCoreNode extends ItSupport {
	public static void main(String[] a) throws Exception {
		msmFactory = CoreRetryIT.RecordingStore::new;
		boot();
		GmmsUtility g = GmmsUtility.getInstance();
		ModuleStatusReporter.start(g, "core", System.getProperty("module"), g.getNodeId());
		MTStreamConsumer.getInstance().start();
		// as DeliveryRouter: a JOINING Core starts these once ACTIVE
		if (!com.king.gmms.ha.DrainController.holdsCoreReaders()) {
			ResultStreamConsumer.getInstance().start();
			InboundDRStreamConsumer.getInstance().start();
			ChannelReleaseDispatcher.getInstance().start();
			CsmExpirySweeper.getInstance().start();
		}
		System.out.println("READY");
		Thread.sleep(Long.MAX_VALUE);
	}
}
