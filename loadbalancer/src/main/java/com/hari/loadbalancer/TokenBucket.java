package com.hari.loadbalancer;

import java.util.concurrent.TimeUnit;

public class TokenBucket {

    private final long capacity;
    private final double refillTokensPerNano;
    private double availableTokens;
    private long lastRefillTimestampNanos;

    public TokenBucket(long capacity, double refillTokensPerSecond) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        if (refillTokensPerSecond <= 0) {
            throw new IllegalArgumentException("refillTokensPerSecond must be positive");
        }
        this.capacity = capacity;
        this.refillTokensPerNano = refillTokensPerSecond / TimeUnit.SECONDS.toNanos(1L);
        this.availableTokens = capacity;
        this.lastRefillTimestampNanos = System.nanoTime();
    }

    public synchronized boolean tryConsume() {
        refill();
        if (availableTokens >= 1.0) {
            availableTokens -= 1.0;
            return true;
        }
        return false;
    }

    public synchronized long[] tryConsumeAndGetRemaining() {
        refill();
        boolean consumed = availableTokens >= 1.0;
        if (consumed) {
            availableTokens -= 1.0;
        }
        return new long[]{consumed ? 1L : 0L, (long) availableTokens};
    }

    public synchronized long getAvailableTokens() {
        refill();
        return (long) availableTokens;
    }

    private void refill() {
        long now = System.nanoTime();
        long elapsedNanos = now - lastRefillTimestampNanos;
        if (elapsedNanos <= 0) {
            return;
        }

        lastRefillTimestampNanos = now;
        double tokensToAdd = elapsedNanos * refillTokensPerNano;
        availableTokens = Math.min(capacity, availableTokens + tokensToAdd);
    }
}

