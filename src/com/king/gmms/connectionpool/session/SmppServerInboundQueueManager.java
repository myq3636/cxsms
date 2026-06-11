package com.king.gmms.connectionpool.session;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.metrics.MetricsCollector;
import com.king.gmms.metrics.MetricsNames;
import com.king.gmms.protocol.smpp.pdu.Request;
import com.king.gmms.protocol.smpp.pdu.Response;
import com.king.gmms.protocol.smpp.util.Data;
import com.king.gmms.threadpool.ThreadPoolProfile;
import com.king.gmms.threadpool.ThreadPoolProfileBuilder;

/**
 * Shared inbound executor for SMPP server sessions.
 *
 * This keeps one bounded queue per SSID and dispatches work into a shared pool,
 * so many customer sessions do not each create their own receiver pool.
 */
public class SmppServerInboundQueueManager {
    private static final SystemLogger log = SystemLogger.getSystemLogger(SmppServerInboundQueueManager.class);
    private static final SmppServerInboundQueueManager INSTANCE = new SmppServerInboundQueueManager();

    private final GmmsUtility gmmsUtility = GmmsUtility.getInstance();
    private final ConcurrentHashMap<Integer, SsidQueue> queues = new ConcurrentHashMap<Integer, SsidQueue>();
    private final ConcurrentLinkedQueue<Integer> activeSsidQueue = new ConcurrentLinkedQueue<Integer>();
    private final ConcurrentHashMap<Integer, Boolean> activeSsidSet = new ConcurrentHashMap<Integer, Boolean>();
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicInteger globalQueued = new AtomicInteger(0);
    private final AtomicInteger globalInFlight = new AtomicInteger(0);

    private volatile ExecutorService workerPool;

    public enum SubmitResult {
        DISABLED,
        QUEUED,
        CALLER_RUNS
    }

    public static SmppServerInboundQueueManager getInstance() {
        return INSTANCE;
    }

    private SmppServerInboundQueueManager() {
    }

    public boolean isEnabled() {
        return getBoolean("SmppServerInboundSharedPoolEnable", false);
    }

    public SubmitResult submit(AbstractCommonSession session, Object obj) {
        if (!isEnabled() || !(session instanceof MultiSmppSession) || !session.isServer()) {
            return SubmitResult.DISABLED;
        }
        if (obj == null) {
            return SubmitResult.QUEUED;
        }
        if (isBindRequest(obj)) {
            return SubmitResult.DISABLED;
        }

        ensureStarted();

        int ssid = resolveSsid(session);
        SsidQueue queue = getQueue(ssid);
        int perSsidLimit = getInt("SmppServerInboundPerSsidQueueSize", 50000, 1, Integer.MAX_VALUE);
        int globalLimit = getInt("SmppServerInboundGlobalQueueLimit", 2000000, 1, Integer.MAX_VALUE);
        int warnQueueSize = getInt("SmppServerInboundWarnQueueSize", 30000, 1, Integer.MAX_VALUE);

        if (globalQueued.get() >= globalLimit || queue.size.get() >= perSsidLimit) {
            log.warn("SMPP server shared inbound queue full. ssid={}, queueSize={}, globalQueued={}, perSsidLimit={}, globalLimit={}",
                    new Object[] {ssid, queue.size.get(), globalQueued.get(), perSsidLimit, globalLimit});
            if (rejectIfPossible(session, obj, ssid)) {
                return SubmitResult.CALLER_RUNS;
            }
            session.receive(obj);
            return SubmitResult.CALLER_RUNS;
        }

        queue.tasks.add(new InboundTask(session, obj, System.currentTimeMillis()));
        int ssidQueued = queue.size.incrementAndGet();
        globalQueued.incrementAndGet();
        activate(ssid);
        updateMetrics();

        if (ssidQueued >= warnQueueSize) {
            log.warn("SMPP server shared inbound queue high watermark. ssid={}, queueSize={}, globalQueued={}",
                    new Object[] {ssid, ssidQueued, globalQueued.get()});
        }
        return SubmitResult.QUEUED;
    }

    private boolean isBindRequest(Object obj) {
        if (!(obj instanceof Request)) {
            return false;
        }
        int commandId = ((Request) obj).getCommandId();
        return commandId == Data.BIND_TRANSMITTER
                || commandId == Data.BIND_RECEIVER
                || commandId == Data.BIND_TRANSCEIVER
                || commandId == Data.OUTBIND;
    }

    private void ensureStarted() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        ThreadPoolProfile profile = new ThreadPoolProfileBuilder("SmppServerInboundSharedWorker")
                .poolSize(getInt("SmppServerInboundWorkerCore", 100, 1, Integer.MAX_VALUE))
                .maxPoolSize(getInt("SmppServerInboundWorkerMax", 300, 1, Integer.MAX_VALUE))
                .maxQueueSize(getInt("SmppServerInboundWorkerQueue", 1000, 1, Integer.MAX_VALUE))
                .keepAliveTime(60L, TimeUnit.SECONDS)
                .build();
        workerPool = gmmsUtility.getExecutorServiceManager().newThreadPool(this, "SmppServerInboundWorker", profile);

        int dispatcherThreads = getInt("SmppServerInboundDispatcherThreads", 2, 1, 64);
        for (int i = 0; i < dispatcherThreads; i++) {
            Thread dispatcher = new Thread(new Dispatcher(), "SmppServerInboundDispatcher-" + i);
            dispatcher.setDaemon(true);
            dispatcher.start();
        }
        if (log.isInfoEnabled()) {
            log.info("SMPP server shared inbound queue started. workerCore={}, workerMax={}, workerQueue={}, dispatchers={}",
                    new Object[] {profile.getPoolSize(), profile.getMaxPoolSize(), profile.getMaxQueueSize(), dispatcherThreads});
        }
    }

    private SsidQueue getQueue(int ssid) {
        SsidQueue queue = queues.get(ssid);
        if (queue != null) {
            return queue;
        }
        SsidQueue created = new SsidQueue();
        SsidQueue existing = queues.putIfAbsent(ssid, created);
        return existing == null ? created : existing;
    }

    private void activate(int ssid) {
        if (activeSsidSet.putIfAbsent(ssid, Boolean.TRUE) == null) {
            activeSsidQueue.add(ssid);
        }
    }

    private int resolveSsid(AbstractCommonSession session) {
        if (session.customerInfo != null) {
            return session.customerInfo.getSSID();
        }
        if (session.connectionInfo != null) {
            return session.connectionInfo.getSsid();
        }
        return -1;
    }

    private int getInt(String key, int defaultValue, int min, int max) {
        int value = defaultValue;
        try {
            value = Integer.parseInt(gmmsUtility.getCommonProperty(key, String.valueOf(defaultValue)).trim());
        } catch (Exception e) {
            value = defaultValue;
        }
        if (value < min) {
            return min;
        }
        if (value > max) {
            return max;
        }
        return value;
    }

    private boolean getBoolean(String key, boolean defaultValue) {
        try {
            return Boolean.parseBoolean(gmmsUtility.getCommonProperty(key, String.valueOf(defaultValue)).trim());
        } catch (Exception e) {
            return defaultValue;
        }
    }

    private boolean rejectIfPossible(AbstractCommonSession session, Object obj, int ssid) {
        if (!(session instanceof MultiSmppSession) || !(obj instanceof Request)) {
            return false;
        }
        Request request = (Request) obj;
        if (!request.canResponse()) {
            return false;
        }
        try {
            Response response = request.getResponse();
            response.setCommandStatus(resolveQueueFullStatus());
            session.submit(response.getData().getBuffer());
            log.warn("SMPP server shared inbound queue rejected request. ssid={}, commandId={}, sequence={}, status={}",
                    new Object[] {ssid, request.getCommandId(), request.getSequenceNumber(), response.getCommandStatus()});
            return true;
        } catch (Exception e) {
            log.warn("Fail to send SMPP queue-full response, run in caller thread.", e);
            return false;
        }
    }

    private int resolveQueueFullStatus() {
        String value = gmmsUtility.getCommonProperty("SmppServerInboundQueueFullCommandStatus", "ESME_RTHROTTLED").trim();
        if ("ESME_RTHROTTLED".equalsIgnoreCase(value)) {
            return Data.ESME_RTHROTTLED;
        }
        try {
            return Integer.parseInt(value);
        } catch (Exception e) {
            return Data.ESME_RTHROTTLED;
        }
    }

    private class Dispatcher implements Runnable {
        public void run() {
            while (true) {
                if (!isEnabled()) {
                    sleep(50L);
                    continue;
                }
                Integer ssid = activeSsidQueue.poll();
                if (ssid == null) {
                    sleep(1L);
                    continue;
                }
                activeSsidSet.remove(ssid);
                dispatchSsid(ssid.intValue());
            }
        }

        private void dispatchSsid(int ssid) {
            SsidQueue queue = queues.get(ssid);
            if (queue == null) {
                return;
            }
            int maxInFlight = getInt("SmppServerInboundMaxInFlightPerSsid", 100, 1, Integer.MAX_VALUE);
            int batch = getInt("SmppServerInboundDispatchBatchPerSsid", 20, 1, maxInFlight);
            int available = maxInFlight - queue.inFlight.get();
            int dispatched = 0;

            while (available > 0 && dispatched < batch) {
                InboundTask task = queue.tasks.poll();
                if (task == null) {
                    break;
                }
                queue.size.decrementAndGet();
                globalQueued.decrementAndGet();
                queue.inFlight.incrementAndGet();
                globalInFlight.incrementAndGet();
                available--;
                dispatched++;
                executeTask(ssid, queue, task);
            }

            if (queue.size.get() > 0 && queue.inFlight.get() < maxInFlight) {
                activate(ssid);
            }
        }

        private void executeTask(final int ssid, final SsidQueue queue, final InboundTask task) {
            try {
                workerPool.execute(new Runnable() {
                    public void run() {
                        try {
                            long waitMs = System.currentTimeMillis() - task.enqueueTimeMs;
                            long warnWaitMs = getInt("SmppServerInboundWarnWaitMs", 1000, 1, Integer.MAX_VALUE);
                            if (waitMs >= warnWaitMs) {
                                log.warn("SMPP server shared inbound task waited too long. ssid={}, waitMs={}, queueSize={}, inFlight={}",
                                        new Object[] {ssid, waitMs, queue.size.get(), queue.inFlight.get()});
                            }
                            task.session.receive(task.obj);
                        } finally {
                            queue.inFlight.decrementAndGet();
                            globalInFlight.decrementAndGet();
                            if (queue.size.get() > 0) {
                                activate(ssid);
                            }
                            updateMetrics();
                        }
                    }
                });
            } catch (Exception e) {
                queue.inFlight.decrementAndGet();
                globalInFlight.decrementAndGet();
                log.warn("SMPP server shared inbound worker rejected task, run in dispatcher thread.", e);
                try {
                    task.session.receive(task.obj);
                } catch (Exception receiveError) {
                    log.warn("SMPP server shared inbound caller-runs task failed.", receiveError);
                }
                updateMetrics();
            }
        }

        private void sleep(long millis) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void updateMetrics() {
        MetricsCollector collector = MetricsCollector.getInstance();
        collector.setGauge(MetricsNames.gauge("queue.smpp_server_inbound", "global_queued"), globalQueued.get());
        collector.setGauge(MetricsNames.gauge("queue.smpp_server_inbound", "global_inflight"), globalInFlight.get());
        collector.setGauge(MetricsNames.gauge("queue.smpp_server_inbound", "active_ssids"), activeSsidSet.size());
        collector.setGauge(MetricsNames.gauge("queue.smpp_server_inbound", "ssid_queues"), queues.size());
    }

    private static class SsidQueue {
        private final ConcurrentLinkedQueue<InboundTask> tasks = new ConcurrentLinkedQueue<InboundTask>();
        private final AtomicInteger size = new AtomicInteger(0);
        private final AtomicInteger inFlight = new AtomicInteger(0);
    }

    private static class InboundTask {
        private final AbstractCommonSession session;
        private final Object obj;
        private final long enqueueTimeMs;

        private InboundTask(AbstractCommonSession session, Object obj, long enqueueTimeMs) {
            this.session = session;
            this.obj = obj;
            this.enqueueTimeMs = enqueueTimeMs;
        }
    }
}
