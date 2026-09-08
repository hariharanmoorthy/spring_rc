package com.hari.service;

/**
 * Marker exception thrown by Kafka consumers for permanent, non-retryable
 * business-rule failures (e.g. "employee already has a role", "employee not
 * found"). Unlike a generic {@link exception.AppException} — which can also
 * wrap transient technical failures such as a DB outage (HTTP 500) that
 * SHOULD be retried — this exception unambiguously means "retrying will
 * never help", so the Kafka error handler excludes it from the retry loop.
 */
public class NonRetryableAssignmentException extends RuntimeException {
    public NonRetryableAssignmentException(String message, Throwable cause) {
        super(message, cause);
    }
}

