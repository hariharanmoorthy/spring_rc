package com.hari.dto;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

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

