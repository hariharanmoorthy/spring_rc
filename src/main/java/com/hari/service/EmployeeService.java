package com.hari.service;

import com.hari.dto.EmployeeRequest;
import com.hari.dto.EmployeeResponse;
import com.hari.repository.EmployeeRepository;
import exception.ExceptionUtil;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class EmployeeService {

    private final EmployeeRepository employeeRepository;
    private final RoleAssignmentProducer roleAssignmentProducer;

    public EmployeeService(EmployeeRepository employeeRepository, RoleAssignmentProducer roleAssignmentProducer) {
        this.employeeRepository = employeeRepository;
        this.roleAssignmentProducer = roleAssignmentProducer;
    }

    public List<Map<String, Object>> getAll() throws Exception {
        return employeeRepository.findAll();
    }

    public Map<String, Object> getById(int id) throws Exception {
        validateId(id);
        return employeeRepository.findById(id);
    }

    public Map<String, Object> create(EmployeeRequest request) throws Exception {
        return employeeRepository.create(request);
    }

    public Map<String, Object> update(int id, EmployeeRequest request) throws Exception {
        validateId(id);
        return employeeRepository.update(id, request);
    }

    public Map<String, Object> delete(int id) throws Exception {
        validateId(id);
        return employeeRepository.delete(id);
    }

    private void validateId(int id) throws Exception {
        if (id <= 0) {
            throw ExceptionUtil.badRequest("Employee ID must be a positive integer");
        }
    }

    public void assignRole(Integer employeeId, Integer roleId) throws Exception {
        if (employeeId == null){
            throw ExceptionUtil.badRequest("Employee ID cannot be null");
        }
        if (roleId == null){
            throw ExceptionUtil.badRequest("Role ID cannot be null");
        }

        // Best-effort synchronous check so obviously-invalid requests (employee
        // not found, or a role already assigned) are rejected immediately
        // instead of always returning 202 and only failing silently later in
        // the Kafka consumer. This is "fail open": if the DB read itself
        // blows up (e.g. outage), we swallow that error and still queue the
        // event, preserving the original guarantee that assignment requests
        // are never lost even if the DB is temporarily down. The consumer
        // still re-validates before writing, so this is just an optimization
        // for the common case, not a replacement for that check.
        try {
            Map<String, Object> employee = employeeRepository.findById(employeeId);
            if (employee.get("role_id") != null) {
                throw ExceptionUtil.badRequest(
                        "Employee " + employeeId + " already has a role assigned");
            }
        } catch (exception.AppException ae) {
            // Genuine 404 (not found) or 400 (bad request, e.g. the check
            // above) should be surfaced immediately; any other AppException
            // (e.g. 500 wrapping a DB connectivity issue) is treated as
            // "unknown" and falls through to queue the event.
            if (ae.getStatus() == org.springframework.http.HttpStatus.NOT_FOUND
                    || ae.getStatus() == org.springframework.http.HttpStatus.BAD_REQUEST) {
                throw ae;
            }
        }

        roleAssignmentProducer.publish(employeeId, roleId);
    }
}
