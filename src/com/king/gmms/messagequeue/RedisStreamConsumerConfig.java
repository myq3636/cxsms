package com.king.gmms.messagequeue;

import java.util.concurrent.ThreadPoolExecutor;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;

/**
 * Unified Redis Stream consumer tuning.
 *
 * Property priority:
 * RedisStreamConsumer.{consumerName}.* -> RedisStreamConsumer.{role}.* -> RedisStreamConsumer.Default.*
 */
public class RedisStreamConsumerConfig {
    private static final SystemLogger logger = SystemLogger.getSystemLogger(RedisStreamConsumerConfig.class);

    private final String role;
    private final String consumerName;
    private final int defaultCorePoolSize;
    private final int defaultMaxPoolSize;
    private final int defaultQueueSize;
    private final int defaultBatchSize;
    private final int defaultIdleSleepMs;

    private RedisStreamConsumerConfig(String role, String consumerName, int defaultCorePoolSize,
            int defaultMaxPoolSize, int defaultQueueSize, int defaultBatchSize, int defaultIdleSleepMs) {
        this.role = role;
        this.consumerName = consumerName;
        this.defaultCorePoolSize = defaultCorePoolSize;
        this.defaultMaxPoolSize = defaultMaxPoolSize;
        this.defaultQueueSize = defaultQueueSize;
        this.defaultBatchSize = defaultBatchSize;
        this.defaultIdleSleepMs = defaultIdleSleepMs;
    }

    public static RedisStreamConsumerConfig load(String role, String consumerName, int defaultCorePoolSize,
            int defaultMaxPoolSize, int defaultQueueSize, int defaultBatchSize, int defaultIdleSleepMs) {
        return new RedisStreamConsumerConfig(role, consumerName, defaultCorePoolSize, defaultMaxPoolSize,
                defaultQueueSize, defaultBatchSize, defaultIdleSleepMs);
    }

    public int workerCorePoolSize() {
        return positiveInt("WorkerCorePoolSize", defaultCorePoolSize);
    }

    public int workerMaxPoolSize() {
        int core = workerCorePoolSize();
        int max = positiveInt("WorkerMaxPoolSize", defaultMaxPoolSize);
        return max < core ? core : max;
    }

    public int workerQueueSize() {
        return positiveInt("WorkerQueueSize", defaultQueueSize);
    }

    public int batchSize() {
        return positiveInt("BatchSize", defaultBatchSize);
    }

    public int dispatcherIdleSleepMs() {
        return positiveInt("DispatcherIdleSleepMs", defaultIdleSleepMs);
    }

    public void applyWorkerPool(ThreadPoolExecutor pool) {
        if (pool == null) {
            return;
        }
        int core = workerCorePoolSize();
        int max = workerMaxPoolSize();
        try {
            if (pool.getMaximumPoolSize() < max) {
                pool.setMaximumPoolSize(max);
                pool.setCorePoolSize(core);
            } else {
                pool.setCorePoolSize(core);
                pool.setMaximumPoolSize(max);
            }
        } catch (Exception e) {
            logger.warn("Failed to apply redis stream consumer pool config. consumer={}, core={}, max={}",
                    consumerName, core, max, e);
        }
    }

    public String summary() {
        return "role=" + role + ", consumer=" + consumerName
                + ", core=" + workerCorePoolSize()
                + ", max=" + workerMaxPoolSize()
                + ", queue=" + workerQueueSize()
                + ", batch=" + batchSize()
                + ", idleSleepMs=" + dispatcherIdleSleepMs();
    }

    private int positiveInt(String suffix, int defaultValue) {
        int value = intProperty("RedisStreamConsumer." + consumerName + "." + suffix, Integer.MIN_VALUE);
        if (value == Integer.MIN_VALUE) {
            value = intProperty("RedisStreamConsumer." + role + "." + suffix, Integer.MIN_VALUE);
        }
        if (value == Integer.MIN_VALUE) {
            value = intProperty("RedisStreamConsumer.Default." + suffix, defaultValue);
        }
        return value <= 0 ? defaultValue : value;
    }

    private int intProperty(String key, int defaultValue) {
        try {
            String value = GmmsUtility.getInstance().getCommonProperty(key, null);
            if (value == null || value.trim().length() == 0) {
                return defaultValue;
            }
            return Integer.parseInt(value.trim());
        } catch (Exception e) {
            logger.warn("Invalid redis stream consumer config. key={}, fallback={}", key, defaultValue);
            return defaultValue;
        }
    }
}
