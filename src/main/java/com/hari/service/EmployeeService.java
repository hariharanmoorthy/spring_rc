package com.hari.service;

import com.hari.dto.EmployeeRequest;
import com.hari.repository.EmployeeRepository;
import exception.ExceptionUtil;
import org.springframework.stereotype.Service;

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

    public List<Map<String, Object>> getAll() {
        return employeeRepository.findAll();
    }

    public Map<String, Object> getById(int id) {
        validateId(id);
        return employeeRepository.findById(id);
    }

    public Map<String, Object> create(EmployeeRequest request) {
        return employeeRepository.create(request);
    }

    public Map<String, Object> update(int id, EmployeeRequest request) {
        validateId(id);
        return employeeRepository.update(id, request);
    }

    public Map<String, Object> delete(int id) {
        validateId(id);
        return employeeRepository.delete(id);
    }

    private void validateId(int id) {
        if (id <= 0) {
            throw ExceptionUtil.badRequest("Employee ID must be a positive integer");
        }
    }

    public void assignRole(Integer employeeId, Integer roleId) {
        if (employeeId == null){
            throw ExceptionUtil.badRequest("Employee ID cannot be null");
        }
        if (roleId == null){
            throw ExceptionUtil.badRequest("Role ID cannot be null");
        }

        try {
            Map<String, Object> employee = employeeRepository.findById(employeeId);
            if (employee.get("role_id") != null) {
                throw ExceptionUtil.badRequest(
                        "Employee " + employeeId + " already has a role assigned");
            }
        } catch (exception.AppException ae) {
            if (ae.getStatus() == org.springframework.http.HttpStatus.NOT_FOUND
                    || ae.getStatus() == org.springframework.http.HttpStatus.BAD_REQUEST) {
                throw ae;
            }
        }

        roleAssignmentProducer.publish(employeeId, roleId);
    }
}
