package com.hari.service;

public class NonRetryableAssignmentException extends RuntimeException {
    public NonRetryableAssignmentException(String message, Throwable cause) {
        super(message, cause);
    }
}

