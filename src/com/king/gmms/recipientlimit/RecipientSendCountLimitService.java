package com.king.gmms.recipientlimit;

import com.king.gmms.domain.A2PCustomerInfo;
import com.king.redis.RedisClient;

/**
 * Production adapter that combines customer policy resolution with Redis count storage.
 */
public final class RecipientSendCountLimitService {
    private RecipientSendCountLimitService() {
    }

    public static boolean isRejected(
            A2PCustomerInfo customer, String recipientAddress, final RedisClient redisClient) {
        if (customer == null) {
            return false;
        }
        int effectiveLimit = customer.getSmsOptionRecipientMaxSendCountIn24H(recipientAddress);
        if (effectiveLimit <= 0) {
            return false;
        }
        Long currentCount = redisClient == null ? null
                : redisClient.incrWithExpire(
                        RecipientSendCountLimiter.buildKey(recipientAddress),
                        RecipientSendCountLimiter.WINDOW_SECONDS);
        return RecipientSendCountLimiter.isRejected(effectiveLimit, currentCount);
    }
}
