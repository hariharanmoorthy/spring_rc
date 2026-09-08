package com.hari.controllers;

import com.hari.dto.EmployeeRequest;
import com.hari.service.EmployeeService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;


@RestController
@RequestMapping("/api/v1/employees")
public class EmployeeController {

    private final EmployeeService employeeService;

    public EmployeeController(EmployeeService employeeService) {
        this.employeeService = employeeService;
    }

    @GetMapping
    public List<Map<String, Object>> getAll() throws Exception {
        return employeeService.getAll();
    }

    @GetMapping("/{id}")
    public Map<String, Object> getById(@PathVariable int id) throws Exception {
        return employeeService.getById(id);
    }

    @ResponseStatus(HttpStatus.CREATED)
    @PostMapping(consumes = "application/json")
    public Map<String, Object> create(@RequestBody EmployeeRequest request) throws Exception {
        return employeeService.create(request);
    }

    @PutMapping(value = "/{id}", consumes = "application/json")
    public Map<String, Object> update(
            @PathVariable int id,
            @RequestBody EmployeeRequest request) throws Exception {
        return employeeService.update(id, request);
    }

    @DeleteMapping("/{id}")
    public Map<String, Object> delete(@PathVariable int id) throws Exception {
        return employeeService.delete(id);
    }

    @PutMapping(value = "/{employeeId}/assign-role")
    public ResponseEntity<String> assignRole(@RequestBody AssignRoleRequest request) throws Exception {
            employeeService.assignRole(request.employeeId(), request.roleId());
            return ResponseEntity.status(HttpStatus.ACCEPTED)
                    .body("Role assignment request queued for processing");
    }

    public record AssignRoleRequest(int employeeId, int roleId) {}
}

