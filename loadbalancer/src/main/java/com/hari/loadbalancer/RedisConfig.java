package com.hari.loadbalancer;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.List;

@Configuration
public class RedisConfig {

    /**
     * KEYS[1] = redis key for the client's bucket (a hash: {tokens, ts})
     * ARGV[1] = capacity
     * ARGV[2] = refill tokens per second
     * ARGV[3] = current time in millis
     * ARGV[4] = TTL (seconds) applied to the bucket key so idle clients expire automatically
     *
     * Returns a two element array: [allowed(0/1), remainingTokens]
     */
    private static final String TOKEN_BUCKET_SCRIPT = """
            local key = KEYS[1]
            local capacity = tonumber(ARGV[1])
            local refill_per_sec = tonumber(ARGV[2])
            local now = tonumber(ARGV[3])
            local ttl = tonumber(ARGV[4])

            local data = redis.call('HMGET', key, 'tokens', 'ts')
            local tokens = tonumber(data[1])
            local ts = tonumber(data[2])

            if tokens == nil then
                tokens = capacity
                ts = now
            end

            local elapsed_ms = now - ts
            if elapsed_ms < 0 then
                elapsed_ms = 0
            end
            tokens = math.min(capacity, tokens + (elapsed_ms * refill_per_sec / 1000.0))

            local allowed = 0
            if tokens >= 1 then
                allowed = 1
                tokens = tokens - 1
            end

            redis.call('HMSET', key, 'tokens', tostring(tokens), 'ts', tostring(now))
            redis.call('EXPIRE', key, ttl)

            return {allowed, math.floor(tokens)}
            """;

    @Bean
    public RedisScript<List> tokenBucketScript() {
        return new DefaultRedisScript<>(TOKEN_BUCKET_SCRIPT, List.class);
    }
}

