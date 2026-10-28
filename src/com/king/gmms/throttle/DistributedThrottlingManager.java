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
     * 获取指定 SSID 的许可 (阻塞模式，带初始 TPS)
     * @param ssid 客户 SSID
     */
    public void acquire(int ssid, int tps) {
        if (tps <= 0) return; // 不限速
        
        RateLimiter limiter = getOrInitLimiter(ssid, tps);
        limiter.acquire();
    }

    /**
     * 获取指定 SSID 的许可 (自动从配置加载 TPS)
     * @param ssid 客户 SSID
     */
    public void acquire(int ssid) {
        // 方案A: 每次按当前配置自对齐速率 —— reload 后下一条即生效，不依赖任何事件 hook。
        int tps = currentTps(ssid);
        if (tps <= 0) {
            // 配置为不限：移除可能存在的旧 limiter（修复 tps->0 不解除限速），直接放行。
            limiters.remove(ssid);
            return;
        }
        RateLimiter limiter = getOrInitLimiter(ssid, tps);
        if (limiter.getRate() != (double) tps) { // 速率变化时即时调整（Guava setRate 线程安全）
            limiter.setRate(tps);
            log.info("Auto-aligned RateLimiter for SSID: {} to {} TPS", ssid, tps);
        }
        limiter.acquire();
    }

    /**
     * 非阻塞取许可（OutgoingThrottlingNum，按本 Client 节点生效）：拿不到立即返回 false，
     * 调用方让出 worker，过 {@link #retryAfterMs} 再来，不在限流器上睡眠。
     */
    public boolean tryAcquire(int ssid) {
        int tps = currentTps(ssid);
        if (tps <= 0) {
            limiters.remove(ssid);
            return true;
        }
        RateLimiter limiter = getOrInitLimiter(ssid, tps);
        if (limiter.getRate() != (double) tps) {
            limiter.setRate(tps);
            log.info("Auto-aligned RateLimiter for SSID: {} to {} TPS", ssid, tps);
        }
        return limiter.tryAcquire();
    }

    /** 拿不到许可时多久后再试：约一个许可的间隔，限制在 10ms 到 1s 之间。 */
    public long retryAfterMs(int ssid) {
        int tps = currentTps(ssid);
        return tps <= 0 ? 10L : Math.max(10L, Math.min(1000L, 1000L / tps));
    }

    /** 当前出站限速 TPS（0 = 不限），按本 Client 节点生效。 */
    public int tps(int ssid) {
        return currentTps(ssid);
    }

    /** 读取当前出站限速 TPS；取不到配置时返回 0（不限速），避免误卡死出站。 */
    private int currentTps(int ssid) {
        try {
            com.king.gmms.domain.A2PCustomerInfo info =
                    com.king.gmms.GmmsUtility.getInstance().getCustomerManager().getCustomerBySSID(ssid);
            return info != null ? info.getOutgoingThrottlingNum() : 0;
        } catch (Exception e) {
            return 0;
        }
    }

    /**
     * 更新指定 SSID 的限速值 (用于配置 Reload)
     * @param ssid 客户 SSID
     * @param newTps 新的每秒限制数
     */
    public void updateRate(int ssid, int newTps) {
        if (newTps <= 0) {
            // 改为不限：移除已有 limiter，真正解除限速（原实现只打日志、旧 limiter 仍在限）。
            if (limiters.remove(ssid) != null) {
                log.info("Throttling disabled (removed limiter) for SSID: {}", ssid);
            }
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
