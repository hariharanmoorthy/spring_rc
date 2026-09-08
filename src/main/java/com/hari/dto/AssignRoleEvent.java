package com.hari.dto;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

/**
 * Event published to Kafka whenever a role-assignment request is received.
 * The actual DB mutation happens asynchronously in the consumer, which retries
 * with backoff until it succeeds (or is routed to the dead-letter topic after
 * exhausting retries, if a max is ever configured).
 */
public record AssignRoleEvent(
        String eventId,
        int employeeId,
        int roleId,
        Instant requestedAt
) implements Serializable {

    public AssignRoleEvent(int employeeId, int roleId) {
        this(UUID.randomUUID().toString(), employeeId, roleId, Instant.now());
    }
}

