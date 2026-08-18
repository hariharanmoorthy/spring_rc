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

    public EmployeeService(EmployeeRepository employeeRepository) {
        this.employeeRepository = employeeRepository;
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
}
