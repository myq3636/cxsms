package com.king.gmms.messagequeue;

import java.util.List;

import com.king.framework.SystemLogger;
import com.king.message.gmms.GmmsMessage;

/**
 * Periodic PEL recovery runner.
 *
 * Normal doorbell workers read only new messages. This runner claims idle PEL
 * messages on a fixed cadence so old pending entries are not pulled forward by
 * unrelated new traffic.
 */
final class RedisStreamAutoClaimRunner implements Runnable {
    interface MessageProcessor {
        void process(String streamKey, GmmsMessage msg);
    }

    private static final SystemLogger logger = SystemLogger.getSystemLogger(RedisStreamAutoClaimRunner.class);

    private final String name;
    private final String streamPattern;
    private final String groupName;
    private final String consumerName;
    private final boolean submitMq;
    private final StreamQueueManager queueManager;
    private final MessageProcessor processor;

    RedisStreamAutoClaimRunner(String name, String streamPattern, String groupName, String consumerName,
            boolean submitMq, StreamQueueManager queueManager, MessageProcessor processor) {
        this.name = name;
        this.streamPattern = streamPattern;
        this.groupName = groupName;
        this.consumerName = consumerName;
        this.submitMq = submitMq;
        this.queueManager = queueManager;
        this.processor = processor;
    }

    public void run() {
        logger.info("{} auto-claim recovery started. streamPattern={}, group={}, consumer={}, isSubmitMq={}",
                name, streamPattern, groupName, consumerName, submitMq);
        while (!Thread.currentThread().isInterrupted()) {
            try {
                if (queueManager.isAutoClaimEnabled()) {
                    recoverOnce();
                }
                Thread.sleep(queueManager.getAutoClaimIntervalMs());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                logger.error(name + " auto-claim recovery error", e);
                try {
                    Thread.sleep(30000L);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        logger.info("{} auto-claim recovery stopped. consumer={}", name, consumerName);
    }

    private void recoverOnce() {
        int remaining = queueManager.getAutoClaimMaxMessagesPerRun();
        if (remaining <= 0) {
            return;
        }
        List<String> streamKeys = queueManager.listResponsibleStreams(streamPattern, submitMq);
        if (streamKeys == null || streamKeys.isEmpty()) {
            return;
        }
        for (String streamKey : streamKeys) {
            if (Thread.currentThread().isInterrupted() || remaining <= 0) {
                return;
            }
            queueManager.createGroup(streamKey, groupName, submitMq);
            int batchSize = Math.min(queueManager.getAutoClaimBatchSize(), remaining);
            // entries delivered RedisStreamMaxDeliveries times go to the dead letter stream here
            List<GmmsMessage> messages = queueManager.recoverPending(streamKey, groupName, consumerName,
                    queueManager.getAutoClaimIdleMs(), batchSize, submitMq);
            if (messages == null || messages.isEmpty()) {
                continue;
            }
            remaining -= messages.size();
            for (GmmsMessage msg : messages) {
                try {
                    processor.process(streamKey, msg);
                } catch (Exception e) {
                    logger.error(msg, name + " auto-claim message processing failed. streamKey=" + streamKey, e);
                }
            }
        }
    }
}
