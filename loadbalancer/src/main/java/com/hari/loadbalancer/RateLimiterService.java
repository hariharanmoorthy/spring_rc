package com.hari.loadbalancer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class RateLimiterService {

    private static final Logger log = LoggerFactory.getLogger(RateLimiterService.class);

    private final long capacity;
    private final double refillPerSecond;
    private final long idleEvictionMillis;

    private final Map<String, ClientBucket> buckets = new ConcurrentHashMap<>();

    public RateLimiterService(
            @Value("${ratelimit.capacity:20}") long capacity,
            @Value("${ratelimit.refill-per-second:10}") double refillPerSecond,
            @Value("${ratelimit.idle-eviction-minutes:30}") long idleEvictionMinutes) {
        this.capacity = capacity;
        this.refillPerSecond = refillPerSecond;
        this.idleEvictionMillis = idleEvictionMinutes * 60_000L;
    }

    public RateLimitDecision checkLimit(String clientKey) {
        ClientBucket clientBucket = buckets.computeIfAbsent(clientKey,
                key -> new ClientBucket(new TokenBucket(capacity, refillPerSecond)));
        clientBucket.touch();

        long[] result = clientBucket.bucket.tryConsumeAndGetRemaining();
        boolean allowed = result[0] == 1L;
        long remaining = result[1];

        if (!allowed) {
            log.warn("Rate limit exceeded for client '{}' (capacity={}, refillPerSecond={})",
                    clientKey, capacity, refillPerSecond);
        }

        return new RateLimitDecision(allowed, remaining, capacity);
    }

    @Scheduled(fixedDelayString = "${ratelimit.eviction-check-interval-ms:300000}")
    public void evictIdleBuckets() {
        long now = System.currentTimeMillis();
        int before = buckets.size();
        buckets.entrySet().removeIf(entry -> now - entry.getValue().getLastAccessMillis() > idleEvictionMillis);
        int removed = before - buckets.size();
        if (removed > 0) {
            log.debug("Evicted {} idle rate-limit buckets at {}", removed, Instant.ofEpochMilli(now));
        }
    }

    private static final class ClientBucket {
        private final TokenBucket bucket;
        private volatile long lastAccessMillis;

        private ClientBucket(TokenBucket bucket) {
            this.bucket = bucket;
            this.lastAccessMillis = System.currentTimeMillis();
        }

        private void touch() {
            this.lastAccessMillis = System.currentTimeMillis();
        }

        private long getLastAccessMillis() {
            return lastAccessMillis;
        }
    }

    public record RateLimitDecision(boolean allowed, long remainingTokens, long capacity){}
}

