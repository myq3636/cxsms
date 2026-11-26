package com.king.gmms.recipientlimit;

import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.messagequeue.StreamAttempt;
import com.king.redis.RedisClient;

/**
 * Production adapter that combines customer policy resolution with Redis count storage.
 */
public final class RecipientSendCountLimitService {
    private RecipientSendCountLimitService() {
    }

    /**
     * The limit of the recipient's country (SMSOptionRecipientMaxSendCountIn24HByCountry, else
     * SMSOptionRecipientMaxSendCountIn24H; 0: no limit) against the 24 h counter of the recipient. The counter moves
     * once per message: a Core attempt redelivered after its Core died only reads it back (StreamAttempt.countOnce;
     * outside an attempt, as at the HTTP servers, every call counts).
     */
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
                : Long.valueOf(StreamAttempt.countOnce(
                        RecipientSendCountLimiter.buildKey(recipientAddress),
                        RecipientSendCountLimiter.WINDOW_SECONDS));
        return RecipientSendCountLimiter.isRejected(effectiveLimit, currentCount);
    }
}
