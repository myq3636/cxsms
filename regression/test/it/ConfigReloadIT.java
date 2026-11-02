package it;

import java.io.File;
import java.nio.file.Files;

import com.king.gmms.GmmsUtility;
import com.king.gmms.messagequeue.StreamQueueManager;

/** Hot reload of the new GmmsConfig keys (reloadGmmsConfig -a); not part of the repo. */
public class ConfigReloadIT extends ItSupport {
	static final String[][] NEW_KEYS = {
		{ "CoreDrInlineDispatch", "false" }, { "CoreMtIdempotency", "false" }, { "ServerDrStreamShardFilter", "true" },
		{ "CoreMtLeaseMs", "30000" }, { "CoreMtMarkerTtlSeconds", "86400" },
		{ "CoreOutboundUnreadStallMs", "90000" }, { "CoreOutboundRespTimeoutMs", "240000" },
		{ "CoreHoldReleaseTickMs", "200" }, { "CoreHoldRecoverIntervalMs", "20000" }, { "CoreHoldRecoverIdleMs", "45000" },
		{ "CoreOutboundOutboxRecheckMs", "30000" },
		{ "RedisStreamMaxDeliveries", "5" }, { "CoreStreamTaskMaxBatches", "4" }, { "CoreStreamTaskMaxMs", "1500" },
		{ "CoreResultStreamSharded", "false" }, { "CoreDrResultIdempotency", "false" },
		{ "CoreDbFallbackInline", "false" }, { "CsmReassembleMode", "legacy" }, { "CoreCsmExpirySweep", "false" },
		{ "CoreCsmExpiryIntervalSec", "30" }, { "CoreCsmExpiryBatch", "100" }, { "CoreCsmExpiryLeaseMs", "120000" },
		{ "ClientSendIdempotency", "false" }, { "ClientSendLeaseMs", "30000" },
		{ "ClientStreamTaskMaxBatches", "5" }, { "ClientStreamTaskMaxMs", "1000" },
		{ "SmppPduLogEnable", "false" }, { "SmppServerSessionKeeperShared", "false" },
		{ "SmppServerSendQueueMax", "5000" }, { "SmppServerSendQueueHighWater", "500" }, { "ServerDrAckOnResp", "false" } };

	public static void main(String[] args) throws Exception {
		boot();
		GmmsUtility gu = GmmsUtility.getInstance();
		File f = new File(home + "Gmms/GmmsConfig.properties");
		String original = new String(Files.readAllBytes(f.toPath()), "UTF-8");
		try {
			for (String[] kv : NEW_KEYS) {
				check(!kv[1].equals(gu.getCommonProperty(kv[0], "-")), "CFG setup: " + kv[0] + " not " + kv[1] + " before reload");
			}
			StringBuilder sb = new StringBuilder(original).append("\n");
			for (String[] kv : NEW_KEYS) sb.append(kv[0]).append("=").append(kv[1]).append("\n");
			Files.write(f.toPath(), sb.toString().getBytes("UTF-8"));
			check(gu.reloadGmmsConfig("-a") == 0, "CFG-01 reload -a succeeds");
			for (String[] kv : NEW_KEYS) {
				check(kv[1].equals(gu.getCommonProperty(kv[0], "-")), "CFG-01 " + kv[0] + "=" + kv[1] + " applied by hot reload");
			}
			check(StreamQueueManager.getInstance().isServerDrShardFilterEnabled(), "CFG-02 ServerDrStreamShardFilter read as true by the code");
			check(!com.king.gmms.processor.RedisCsmReassembler.enabled()
					&& !com.king.gmms.processor.DBBackupHandler.getInstance().inlineEnabled(),
					"CFG-02 CsmReassembleMode=legacy and CoreDbFallbackInline=false read by the code");
			check(!com.king.gmms.messagequeue.ClientSendMarker.enabled(), "CFG-02 ClientSendIdempotency=false read by the code");

			// invalid number: the whole reload is rejected, values stay
			Files.write(f.toPath(), (sb.toString() + "CoreMtLeaseMs=abc\n").getBytes("UTF-8"));
			check(gu.reloadGmmsConfig("-a") != 0 && "30000".equals(gu.getCommonProperty("CoreMtLeaseMs", "-")),
					"CFG-03 invalid CoreMtLeaseMs rejected, previous value kept");

			// removing a runtime key from the file brings the default back (GmmsConfig reload optimisation)
			Files.write(f.toPath(), original.getBytes("UTF-8"));
			gu.reloadGmmsConfig("-a");
			check("-".equals(gu.getCommonProperty("CoreDrInlineDispatch", "-")),
					"CFG-04 key deleted from the file: hot reload goes back to the default");
		} finally {
			Files.write(f.toPath(), original.getBytes("UTF-8"));
		}
		exit();
	}
}
