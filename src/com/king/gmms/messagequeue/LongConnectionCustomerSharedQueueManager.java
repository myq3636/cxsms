package com.king.gmms.messagequeue;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.connection.ConnectionManager;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.metrics.MetricsCollector;
import com.king.gmms.metrics.MetricsNames;
import com.king.gmms.sender.CustomerMessageSender;
import com.king.gmms.threadpool.ThreadPoolProfile;
import com.king.gmms.threadpool.ThreadPoolProfileBuilder;
import com.king.message.gmms.GmmsMessage;

/**
 * Shared sender queues for MultiSmppServer customer responses.
 *
 * One bounded queue is kept per SSID while a small number of shared workers do
 * the actual send. This avoids creating CustSender/CustRespSender pools for
 * every customer.
 */
public class LongConnectionCustomerSharedQueueManager {
    private static final SystemLogger log = SystemLogger.getSystemLogger(LongConnectionCustomerSharedQueueManager.class);
    private static final LongConnectionCustomerSharedQueueManager INSTANCE = new LongConnectionCustomerSharedQueueManager();
    private static final String GLOBAL_PREFIX = "MultiSmppServer.";

    private final GmmsUtility gmmsUtility = GmmsUtility.getInstance();
    private final ConcurrentHashMap<Integer, SsidQueue> senderQueues = new ConcurrentHashMap<Integer, SsidQueue>();
    private final ConcurrentHashMap<Integer, SsidQueue> respQueues = new ConcurrentHashMap<Integer, SsidQueue>();
    private final ConcurrentLinkedQueue<Integer> activeSenderSsidQueue = new ConcurrentLinkedQueue<Integer>();
    private final ConcurrentLinkedQueue<Integer> activeRespSsidQueue = new ConcurrentLinkedQueue<Integer>();
    private final ConcurrentHashMap<Integer, Boolean> activeSenderSsidSet = new ConcurrentHashMap<Integer, Boolean>();
    private final ConcurrentHashMap<Integer, Boolean> activeRespSsidSet = new ConcurrentHashMap<Integer, Boolean>();
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicInteger senderQueued = new AtomicInteger(0);
    private final AtomicInteger respQueued = new AtomicInteger(0);
    private final AtomicInteger senderInFlight = new AtomicInteger(0);
    private final AtomicInteger respInFlight = new AtomicInteger(0);

    private volatile ExecutorService senderWorkerPool;
    private volatile ExecutorService respWorkerPool;

    public static LongConnectionCustomerSharedQueueManager getInstance() {
        return INSTANCE;
    }

    private LongConnectionCustomerSharedQueueManager() {
    }

    public boolean isEnabledForServer() {
        String module = System.getProperty("module");
        return module != null
                && module.startsWith("MultiSmppServer")
                && getBoolean("MultiSmppServer.LongConnectionCustomerSharedQueueEnable", false);
    }

    public boolean submit(LongConnectionCustomerMessageQueue owner, GmmsMessage msg,
            ConnectionManager connectionManager, A2PCustomerInfo cst) {
        if (!isEnabledForServer() || msg == null || owner == null || connectionManager == null || cst == null) {
            return false;
        }

        ensureStarted();

        boolean response = isResponseMessage(msg);
        ConcurrentHashMap<Integer, SsidQueue> queues = response ? respQueues : senderQueues;
        AtomicInteger globalQueued = response ? respQueued : senderQueued;
        int ssid = cst.getSSID();
        SsidQueue queue = getQueue(queues, ssid);
        int perSsidLimit = getInt("MultiSmppServer.LongConnectionCustomerPerSsidQueueSize", 50000, 1, Integer.MAX_VALUE);
        int globalLimit = getInt("MultiSmppServer.LongConnectionCustomerGlobalQueueLimit", 2000000, 1, Integer.MAX_VALUE);
        int warnQueueSize = getInt("MultiSmppServer.LongConnectionCustomerWarnQueueSize", 30000, 1, Integer.MAX_VALUE);

        if (globalQueued.get() >= globalLimit || queue.size.get() >= perSsidLimit) {
            log.warn(msg, "MultiSmppServer shared customer queue full. lane={}, ssid={}, queueSize={}, globalQueued={}, perSsidLimit={}, globalLimit={}",
                    new Object[] {laneName(response), ssid, queue.size.get(), globalQueued.get(), perSsidLimit, globalLimit});
            return false;
        }

        queue.tasks.add(new SharedSendTask(owner, msg, connectionManager, cst, System.currentTimeMillis()));
        int ssidQueued = queue.size.incrementAndGet();
        globalQueued.incrementAndGet();
        activate(ssid, response);
        updateMetrics(response);

        if (ssidQueued >= warnQueueSize) {
            log.warn(msg, "MultiSmppServer shared customer queue high watermark. lane={}, ssid={}, queueSize={}, globalQueued={}",
                    new Object[] {laneName(response), ssid, ssidQueued, globalQueued.get()});
        }
        return true;
    }

    private void ensureStarted() {
        if (!started.compareAndSet(false, true)) {
            return;
        }

        ThreadPoolProfile senderProfile = new ThreadPoolProfileBuilder("MultiSmppServerCustomerSenderWorker")
                .poolSize(getInt("MultiSmppServer.LongConnectionCustomerSenderWorkerThreads", 64, 1, Integer.MAX_VALUE))
                .maxPoolSize(getInt("MultiSmppServer.LongConnectionCustomerSenderWorkerThreads", 64, 1, Integer.MAX_VALUE))
                .maxQueueSize(getInt("MultiSmppServer.LongConnectionCustomerWorkerQueueSize", 1000, 1, Integer.MAX_VALUE))
                .keepAliveTime(60L, TimeUnit.SECONDS)
                .build();
        ThreadPoolProfile respProfile = new ThreadPoolProfileBuilder("MultiSmppServerCustomerRespWorker")
                .poolSize(getInt("MultiSmppServer.LongConnectionCustomerRespWorkerThreads", 64, 1, Integer.MAX_VALUE))
                .maxPoolSize(getInt("MultiSmppServer.LongConnectionCustomerRespWorkerThreads", 64, 1, Integer.MAX_VALUE))
                .maxQueueSize(getInt("MultiSmppServer.LongConnectionCustomerWorkerQueueSize", 1000, 1, Integer.MAX_VALUE))
                .keepAliveTime(60L, TimeUnit.SECONDS)
                .build();

        senderWorkerPool = gmmsUtility.getExecutorServiceManager().newThreadPool(this, "MultiSmppServerCustomerSenderWorker", senderProfile);
        respWorkerPool = gmmsUtility.getExecutorServiceManager().newThreadPool(this, "MultiSmppServerCustomerRespWorker", respProfile);

        int dispatcherThreads = getInt("MultiSmppServer.LongConnectionCustomerDispatcherThreads", 2, 1, 64);
        for (int i = 0; i < dispatcherThreads; i++) {
            Thread dispatcher = new Thread(new Dispatcher(false), "MultiSmppServerCustomerSenderDispatcher-" + i);
            dispatcher.setDaemon(true);
            dispatcher.start();
            Thread respDispatcher = new Thread(new Dispatcher(true), "MultiSmppServerCustomerRespDispatcher-" + i);
            respDispatcher.setDaemon(true);
            respDispatcher.start();
        }

        log.info("MultiSmppServer shared customer queue started. senderWorkers={}, respWorkers={}, workerQueue={}, dispatchers={}",
                new Object[] {senderProfile.getPoolSize(), respProfile.getPoolSize(), senderProfile.getMaxQueueSize(), dispatcherThreads});
    }

    private SsidQueue getQueue(ConcurrentHashMap<Integer, SsidQueue> queues, int ssid) {
        SsidQueue queue = queues.get(ssid);
        if (queue != null) {
            return queue;
        }
        SsidQueue created = new SsidQueue();
        SsidQueue existing = queues.putIfAbsent(ssid, created);
        return existing == null ? created : existing;
    }

    private void activate(int ssid, boolean response) {
        ConcurrentHashMap<Integer, Boolean> activeSet = response ? activeRespSsidSet : activeSenderSsidSet;
        ConcurrentLinkedQueue<Integer> activeQueue = response ? activeRespSsidQueue : activeSenderSsidQueue;
        if (activeSet.putIfAbsent(ssid, Boolean.TRUE) == null) {
            activeQueue.add(ssid);
        }
    }

    private boolean isResponseMessage(GmmsMessage msg) {
        String msgType = msg.getMessageType();
        return GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(msgType)
                || GmmsMessage.MSG_TYPE_DELIVERY_RESP.equalsIgnoreCase(msgType)
                || GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP.equalsIgnoreCase(msgType)
                || GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY_RESP.equalsIgnoreCase(msgType);
    }

    private int getInt(String key, int defaultValue, int min, int max) {
        int value = defaultValue;
        try {
            value = Integer.parseInt(getProperty(key, String.valueOf(defaultValue)).trim());
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
            return Boolean.parseBoolean(getProperty(key, String.valueOf(defaultValue)).trim());
        } catch (Exception e) {
            return defaultValue;
        }
    }

    private String getProperty(String globalKey, String defaultValue) {
        String moduleKey = toModuleKey(globalKey);
        if (moduleKey != null) {
            String moduleValue = gmmsUtility.getCommonProperty(moduleKey);
            if (moduleValue != null) {
                return moduleValue;
            }
        }
        return gmmsUtility.getCommonProperty(globalKey, defaultValue);
    }

    private String toModuleKey(String globalKey) {
        String module = System.getProperty("module");
        if (module == null || module.length() == 0 || globalKey == null || !globalKey.startsWith(GLOBAL_PREFIX)) {
            return null;
        }
        return module + "." + globalKey.substring(GLOBAL_PREFIX.length());
    }

    private String laneName(boolean response) {
        return response ? "response" : "sender";
    }

    private class Dispatcher implements Runnable {
        private final boolean response;

        private Dispatcher(boolean response) {
            this.response = response;
        }

        public void run() {
            while (true) {
                if (!isEnabledForServer()) {
                    sleep(50L);
                    continue;
                }
                ConcurrentLinkedQueue<Integer> activeQueue = response ? activeRespSsidQueue : activeSenderSsidQueue;
                ConcurrentHashMap<Integer, Boolean> activeSet = response ? activeRespSsidSet : activeSenderSsidSet;
                Integer ssid = activeQueue.poll();
                if (ssid == null) {
                    sleep(1L);
                    continue;
                }
                activeSet.remove(ssid);
                dispatchSsid(ssid.intValue());
            }
        }

        private void dispatchSsid(int ssid) {
            ConcurrentHashMap<Integer, SsidQueue> queues = response ? respQueues : senderQueues;
            SsidQueue queue = queues.get(ssid);
            if (queue == null) {
                return;
            }
            int maxInFlight = getInt("MultiSmppServer.LongConnectionCustomerMaxInFlightPerSsid", 50, 1, Integer.MAX_VALUE);
            int batch = getInt("MultiSmppServer.LongConnectionCustomerDispatchBatchPerSsid", 10, 1, maxInFlight);
            int available = maxInFlight - queue.inFlight.get();
            int dispatched = 0;

            while (available > 0 && dispatched < batch) {
                SharedSendTask task = queue.tasks.poll();
                if (task == null) {
                    break;
                }
                queue.size.decrementAndGet();
                (response ? respQueued : senderQueued).decrementAndGet();
                queue.inFlight.incrementAndGet();
                (response ? respInFlight : senderInFlight).incrementAndGet();
                available--;
                dispatched++;
                executeTask(ssid, queue, task);
            }

            if (queue.size.get() > 0 && queue.inFlight.get() < maxInFlight) {
                activate(ssid, response);
            }
        }

        private void executeTask(final int ssid, final SsidQueue queue, final SharedSendTask task) {
            try {
                ExecutorService workerPool = response ? respWorkerPool : senderWorkerPool;
                workerPool.execute(new Runnable() {
                    public void run() {
                        try {
                            runTask(ssid, queue, task);
                        } finally {
                            queue.inFlight.decrementAndGet();
                            (response ? respInFlight : senderInFlight).decrementAndGet();
                            if (queue.size.get() > 0) {
                                activate(ssid, response);
                            }
                            updateMetrics(response);
                        }
                    }
                });
            } catch (Exception e) {
                queue.inFlight.decrementAndGet();
                (response ? respInFlight : senderInFlight).decrementAndGet();
                log.warn(task.msg, "MultiSmppServer shared customer worker rejected task, run in dispatcher thread. lane={}, ssid={}",
                        new Object[] {laneName(response), ssid});
                try {
                    runTask(ssid, queue, task);
                } catch (Exception sendError) {
                    log.warn(task.msg, "MultiSmppServer shared customer caller-runs task failed. lane={}, ssid={}",
                            new Object[] {laneName(response), ssid});
                    log.warn(sendError, sendError);
                }
                updateMetrics(response);
            }
        }

        private void runTask(int ssid, SsidQueue queue, SharedSendTask task) {
            long waitMs = System.currentTimeMillis() - task.enqueueTimeMs;
            if (waitMs >= task.owner.getQueueTimeout()) {
                log.warn(task.msg, "MultiSmppServer shared customer task timeout. lane={}, ssid={}, waitMs={}, queueSize={}, inFlight={}",
                        new Object[] {laneName(response), ssid, waitMs, queue.size.get(), queue.inFlight.get()});
                task.owner.timeout(task.msg);
                return;
            }

            long warnWaitMs = getInt("MultiSmppServer.LongConnectionCustomerWarnWaitMs", 1000, 1, Integer.MAX_VALUE);
            if (waitMs >= warnWaitMs) {
                log.warn(task.msg, "MultiSmppServer shared customer task waited too long. lane={}, ssid={}, waitMs={}, queueSize={}, inFlight={}",
                        new Object[] {laneName(response), ssid, waitMs, queue.size.get(), queue.inFlight.get()});
            }

            new CustomerMessageSender(task.msg, task.connectionManager, task.cst, true).run();
        }

        private void sleep(long millis) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void updateMetrics(boolean response) {
        MetricsCollector collector = MetricsCollector.getInstance();
        String component = response ? "queue.multismppserver_customer_response" : "queue.multismppserver_customer_sender";
        collector.setGauge(MetricsNames.gauge(component, "global_queued"), (response ? respQueued : senderQueued).get());
        collector.setGauge(MetricsNames.gauge(component, "global_inflight"), (response ? respInFlight : senderInFlight).get());
        collector.setGauge(MetricsNames.gauge(component, "active_ssids"), (response ? activeRespSsidSet : activeSenderSsidSet).size());
        collector.setGauge(MetricsNames.gauge(component, "ssid_queues"), (response ? respQueues : senderQueues).size());
    }

    private static class SsidQueue {
        private final ConcurrentLinkedQueue<SharedSendTask> tasks = new ConcurrentLinkedQueue<SharedSendTask>();
        private final AtomicInteger size = new AtomicInteger(0);
        private final AtomicInteger inFlight = new AtomicInteger(0);
    }

    private static class SharedSendTask {
        private final LongConnectionCustomerMessageQueue owner;
        private final GmmsMessage msg;
        private final ConnectionManager connectionManager;
        private final A2PCustomerInfo cst;
        private final long enqueueTimeMs;

        private SharedSendTask(LongConnectionCustomerMessageQueue owner, GmmsMessage msg,
                ConnectionManager connectionManager, A2PCustomerInfo cst, long enqueueTimeMs) {
            this.owner = owner;
            this.msg = msg;
            this.connectionManager = connectionManager;
            this.cst = cst;
            this.enqueueTimeMs = enqueueTimeMs;
        }
    }
}
