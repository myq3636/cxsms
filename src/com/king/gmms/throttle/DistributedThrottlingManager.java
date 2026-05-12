package com.king.gmms.throttle;

import com.google.common.util.concurrent.RateLimiter;
import com.king.framework.SystemLogger;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 分布式/本地外发流控管理器 (V4.2 纯本地 Guava 优化版)
 * 职责：
 * 1. 维护每个 SSID 专属的 Guava RateLimiter。
 * 2. 支持通过动态 Reload 实时调整速率。
 * 3. 隔离不同客户的限速逻辑。
 */
public class DistributedThrottlingManager {
    private static final SystemLogger log = SystemLogger.getSystemLogger(DistributedThrottlingManager.class);
    
    // SSID -> RateLimiter 映射表
    private final ConcurrentMap<Integer, RateLimiter> limiters = new ConcurrentHashMap<>();
    
    private static class Holder {
        private static final DistributedThrottlingManager INSTANCE = new DistributedThrottlingManager();
    }
    
    public static DistributedThrottlingManager getInstance() {
        return Holder.INSTANCE;
    }
    
    private DistributedThrottlingManager() {}

    /**
     * 获取指定 SSID 的许可 (阻塞模式)
     * @param ssid 客户 SSID
     */
    public void acquire(int ssid, int tps) {
        if (tps <= 0) return; // 不限速
        
        RateLimiter limiter = getOrInitLimiter(ssid, tps);
        limiter.acquire();
    }

    /**
     * 更新指定 SSID 的限速值 (用于配置 Reload)
     * @param ssid 客户 SSID
     * @param newTps 新的每秒限制数
     */
    public void updateRate(int ssid, int newTps) {
        if (newTps <= 0) {
            log.info("Throttling disabled for SSID: {}", ssid);
            return;
        }
        
        RateLimiter limiter = limiters.get(ssid);
        if (limiter != null) {
            double oldRate = limiter.getRate();
            if (oldRate != (double)newTps) {
                limiter.setRate(newTps);
                log.info("Dynamic update rate for SSID: {} from {} to {} TPS", ssid, oldRate, newTps);
            }
        } else {
            // 如果还不存在，则初始化
            getOrInitLimiter(ssid, newTps);
        }
    }

    private RateLimiter getOrInitLimiter(int ssid, int tps) {
        RateLimiter limiter = limiters.get(ssid);
        if (limiter == null) {
            limiter = RateLimiter.create(tps);
            RateLimiter old = limiters.putIfAbsent(ssid, limiter);
            if (old != null) {
                limiter = old;
            } else {
                log.info("Initialized local RateLimiter for SSID: {} at {} TPS", ssid, tps);
            }
        }
        return limiter;
    }
}
