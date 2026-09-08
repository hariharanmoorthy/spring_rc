package com.hari.service;

import com.hari.config.KafkaConfig;
import com.hari.dto.AssignRoleEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class RoleAssignmentProducer {

    private static final Logger log = LoggerFactory.getLogger(RoleAssignmentProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public RoleAssignmentProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(int employeeId, int roleId) {
        AssignRoleEvent event = new AssignRoleEvent(employeeId, roleId);
        // Key by employeeId so all events for the same employee land on the
        // same partition and are processed in order.
        kafkaTemplate.send(KafkaConfig.ROLE_ASSIGNMENT_TOPIC, String.valueOf(employeeId), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish role assignment event {} for employee {}",
                                event.eventId(), employeeId, ex);
                    } else {
                        log.info("Published role assignment event {} for employee {} -> role {}",
                                event.eventId(), employeeId, roleId);
                    }
                });
    }
}

