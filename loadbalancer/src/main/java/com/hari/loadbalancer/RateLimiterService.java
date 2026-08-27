package com.hari.loadbalancer;

import jakarta.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

@Service
public class RateLimiterService {

    private static final Logger log = LoggerFactory.getLogger(RateLimiterService.class);
    private static final String KEY_PREFIX = "ratelimit:bucket:";

    private final long capacity;
    private final double refillPerSecond;
    private final long ttlSeconds;

    private final StringRedisTemplate redisTemplate;
    private final RedisScript<List> tokenBucketScript;

    public RateLimiterService(
            @Value("${ratelimit.capacity:20}") long capacity,
            @Value("${ratelimit.refill-per-second:10}") double refillPerSecond,
            @Value("${ratelimit.idle-eviction-minutes:30}") long idleEvictionMinutes,
            StringRedisTemplate redisTemplate,
            RedisScript<List> tokenBucketScript) {
        this.capacity = capacity;
        this.refillPerSecond = refillPerSecond;
        this.ttlSeconds = idleEvictionMinutes * 60L;
        this.redisTemplate = redisTemplate;
        this.tokenBucketScript = tokenBucketScript;
    }

    public RateLimitDecision checkLimit(String clientKey) {
        String redisKey = KEY_PREFIX + clientKey;
        long now = System.currentTimeMillis();

        List<Long> result = redisTemplate.execute(
                tokenBucketScript,
                Collections.singletonList(redisKey),
                String.valueOf(capacity),
                String.valueOf(refillPerSecond),
                String.valueOf(now),
                String.valueOf(ttlSeconds));

        boolean allowed = result != null && result.get(0) == 1L;
        long remaining = result != null ? result.get(1) : 0L;

        if (!allowed) {
            log.warn("Rate limit exceeded for client '{}' (capacity={}, refillPerSecond={})",
                    clientKey, capacity, refillPerSecond);
        }

        return new RateLimitDecision(allowed, remaining, capacity);
    }

    public record RateLimitDecision(boolean allowed, long remainingTokens, long capacity){}
}

