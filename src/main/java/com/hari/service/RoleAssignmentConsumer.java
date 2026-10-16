package com.hari.service;

import com.hari.config.KafkaConfig;
import com.hari.dto.AssignRoleEvent;
import com.hari.repository.EmployeeRepository;
import exception.AppException;
import exception.ExceptionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;


@Component
public class RoleAssignmentConsumer {

    private static final Logger log = LoggerFactory.getLogger(RoleAssignmentConsumer.class);

    /** Statuses that represent a genuine, permanent business-rule failure. */
    private static final Set<HttpStatus> PERMANENT_STATUSES =
            Set.of(HttpStatus.BAD_REQUEST, HttpStatus.NOT_FOUND, HttpStatus.CONFLICT);

    private final EmployeeRepository employeeRepository;

    public RoleAssignmentConsumer(EmployeeRepository employeeRepository) {
        this.employeeRepository = employeeRepository;
    }

    @KafkaListener(
            topics = KafkaConfig.ROLE_ASSIGNMENT_TOPIC,
            groupId = "employee-role-assignment-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onAssignRole(AssignRoleEvent event) {
        log.info("Processing role assignment event {}: employee={}, role={}",
                event.eventId(), event.employeeId(), event.roleId());
        try {
            Map<String, Object> employee = employeeRepository.findById(event.employeeId());
            if (employee.get("role_id") != null) {
                throw ExceptionUtil.badRequest(
                        "Employee " + event.employeeId() + " already has a role assigned");
            }
            employeeRepository.assignRole(event.employeeId(), event.roleId());
            log.info("Successfully assigned role {} to employee {} (event {})",
                    event.roleId(), event.employeeId(), event.eventId());
        } catch (AppException ae) {
            if (PERMANENT_STATUSES.contains(ae.getStatus())) {
                log.error("Permanent failure processing role assignment event {} (employee={}, role={}): {}. " +
                        "This event will NOT be retried.", event.eventId(), event.employeeId(), event.roleId(),
                        ae.getMessage());
                throw new NonRetryableAssignmentException(ae.getMessage(), ae);
            }
            // e.g. 500 wrapping a DB connectivity failure - must be retried.
            log.warn("Transient failure (status={}) processing role assignment event {} (employee={}, role={}). " +
                    "It will be retried.", ae.getStatus(), event.eventId(), event.employeeId(), event.roleId(), ae);
            throw ae;
        } catch (Exception e) {
            log.warn("Failed to process role assignment event {} (employee={}, role={}). " +
                    "It will be retried.", event.eventId(), event.employeeId(), event.roleId(), e);
            throw e;
        }
    }
}



