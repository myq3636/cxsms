package com.king.gmms.connectionpool.session;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.metrics.MetricsCollector;
import com.king.gmms.metrics.MetricsNames;
import com.king.gmms.protocol.smpp.pdu.Request;
import com.king.gmms.protocol.smpp.pdu.Response;
import com.king.gmms.protocol.smpp.util.Data;

/**
 * Shared inbound executor for SMPP server sessions.
 *
 * This keeps one bounded queue per SSID and dispatches work into a shared pool,
 * so many customer sessions do not each create their own receiver pool.
 * <ul>
 * <li>Enabled by default (SmppServerInboundSharedPoolEnable), decided when a session is created.</li>
 * <li>The pool grows to SmppServerInboundWorkerMax threads before queueing; when it is full the
 * dispatcher puts the task back at the head of its SSID queue and retries, it never runs tasks
 * itself (a dispatcher running a slow submit stops dispatching for every customer).</li>
 * <li>Responses (deliver_sm_resp, ...), enquire_link and unbind take a priority lane: they are not
 * held behind a customer's submit backlog (DR pending timeouts, link timeouts) and never rejected.</li>
 * </ul>
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
    /** times a task found the worker pool full and went back to its queue */
    private final AtomicLong rejected = new AtomicLong(0);

    private volatile ThreadPoolExecutor workerPool;

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
        return getBoolean("SmppServerInboundSharedPoolEnable", true);
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
        if (isPriority(obj)) {
            queue.priority.add(new InboundTask(session, obj, System.currentTimeMillis()));
            activate(ssid);
            return SubmitResult.QUEUED;
        }
        int perSsidLimit = getInt("SmppServerInboundPerSsidQueueSize", 10000, 1, Integer.MAX_VALUE);
        int globalLimit = getInt("SmppServerInboundGlobalQueueLimit", 200000, 1, Integer.MAX_VALUE);
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

    /** Responses to our requests (deliver_sm_resp ...), enquire_link and unbind: cheap, time-critical. */
    private boolean isPriority(Object obj) {
        if (obj instanceof Response) {
            return true;
        }
        if (!(obj instanceof Request)) {
            return false;
        }
        int commandId = ((Request) obj).getCommandId();
        return commandId == Data.ENQUIRE_LINK || commandId == Data.UNBIND;
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
        // core = max: a plain pool only adds threads above its core size once the queue is full
        // each worker holds a Redis connection while it produces: more workers than Redis_pool_maxActive
        // (default 50) only wait for the pool and make the tail latency worse
        int workerMax = getInt("SmppServerInboundWorkerMax", 50, 1, Integer.MAX_VALUE);
        int workerQueue = getInt("SmppServerInboundWorkerQueue", 1000, 1, Integer.MAX_VALUE);
        final AtomicInteger threadNo = new AtomicInteger();
        workerPool = new ThreadPoolExecutor(workerMax, workerMax, 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(workerQueue), new ThreadFactory() {
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, "SmppServerInboundWorker-" + threadNo.incrementAndGet());
                        t.setDaemon(true);
                        return t;
                    }
                }, new ThreadPoolExecutor.AbortPolicy());
        workerPool.allowCoreThreadTimeOut(true);

        int dispatcherThreads = getInt("SmppServerInboundDispatcherThreads", 2, 1, 64);
        for (int i = 0; i < dispatcherThreads; i++) {
            Thread dispatcher = new Thread(new Dispatcher(), "SmppServerInboundDispatcher-" + i);
            dispatcher.setDaemon(true);
            dispatcher.start();
        }
        if (log.isInfoEnabled()) {
            log.info("SMPP server shared inbound queue started. workerMax={}, workerQueue={}, dispatchers={}",
                    new Object[] {workerMax, workerQueue, dispatcherThreads});
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
                if (!dispatchSsid(ssid.intValue())) {
                    sleep(1L);   // pool full: the SSID is queued again, give the workers a moment
                }
            }
        }

        /** false when the worker pool was full: the task is back at the head of its queue. */
        private boolean dispatchSsid(int ssid) {
            SsidQueue queue = queues.get(ssid);
            if (queue == null) {
                return true;
            }
            InboundTask p;
            while ((p = queue.priority.poll()) != null) {
                if (!executeTask(ssid, queue, p, true)) {
                    queue.priority.addFirst(p);
                    activate(ssid);
                    return false;
                }
            }
            int maxInFlight = getInt("SmppServerInboundMaxInFlightPerSsid", 20, 1, Integer.MAX_VALUE);
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
                if (!executeTask(ssid, queue, task, false)) {
                    queue.inFlight.decrementAndGet();
                    globalInFlight.decrementAndGet();
                    queue.tasks.addFirst(task);
                    queue.size.incrementAndGet();
                    globalQueued.incrementAndGet();
                    activate(ssid);
                    return false;
                }
                available--;
                dispatched++;
            }

            if (queue.size.get() > 0 && queue.inFlight.get() < maxInFlight) {
                activate(ssid);
            }
            return true;
        }

        /** false when the pool is full (the caller puts the task back), never runs it on the dispatcher. */
        private boolean executeTask(final int ssid, final SsidQueue queue, final InboundTask task,
                final boolean priority) {
            try {
                workerPool.execute(new Runnable() {
                    public void run() {
                        try {
                            long waitMs = System.currentTimeMillis() - task.enqueueTimeMs;
                            long warnWaitMs = getInt("SmppServerInboundWarnWaitMs", 1000, 1, Integer.MAX_VALUE);
                            if (waitMs >= warnWaitMs) {
                                warnSlow(ssid, queue, waitMs);
                            }
                            task.session.receive(task.obj);
                        } finally {
                            if (!priority) {
                                queue.inFlight.decrementAndGet();
                                globalInFlight.decrementAndGet();
                            }
                            if (queue.size.get() > 0 || !queue.priority.isEmpty()) {
                                activate(ssid);
                            }
                            updateMetrics();
                        }
                    }
                });
                return true;
            } catch (RejectedExecutionException full) {
                rejected.incrementAndGet();
                return false;
            }
        }

        /**
         * At most one line per SSID every 10 s: with a backlog every task waits, and a line per
         * task would slow the server down further.
         */
        private void warnSlow(int ssid, SsidQueue queue, long waitMs) {
            long slow = queue.slowSinceWarn.incrementAndGet();
            long now = System.currentTimeMillis();
            long last = queue.lastSlowWarnMs.get();
            if (now - last < 10000L || !queue.lastSlowWarnMs.compareAndSet(last, now)) {
                return;
            }
            queue.slowSinceWarn.set(0);
            log.warn("SMPP server shared inbound tasks waited too long. ssid={}, tasks={}, lastWaitMs={}, queueSize={}, inFlight={}",
                    new Object[] {ssid, slow, waitMs, queue.size.get(), queue.inFlight.get()});
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
        collector.setGauge(MetricsNames.gauge("queue.smpp_server_inbound", "pool_full_retries"), rejected.get());
    }

    private static class SsidQueue {
        private final ConcurrentLinkedDeque<InboundTask> tasks = new ConcurrentLinkedDeque<InboundTask>();
        /** responses, enquire_link, unbind: dispatched first, not counted in the queue size or in-flight limit */
        private final ConcurrentLinkedDeque<InboundTask> priority = new ConcurrentLinkedDeque<InboundTask>();
        private final AtomicInteger size = new AtomicInteger(0);
        private final AtomicInteger inFlight = new AtomicInteger(0);
        private final AtomicLong slowSinceWarn = new AtomicLong(0);
        private final AtomicLong lastSlowWarnMs = new AtomicLong(0);
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
