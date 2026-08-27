package com.hari.repository;

import com.hari.dto.EmployeeRequest;
import com.hari.repository.repoUtil.RepoUtil;
import exception.ExceptionUtil;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@Repository
public class EmployeeRepository {
    private static final String FETCH_ALL =
            "SELECT e.id, e.name, e.salary, r.name as role_name FROM employee e left join role r on r.id=e.role_id ORDER BY e.id";

    private static final String FETCH_BY_ID =
            "SELECT id, name, salary, role_id FROM employee WHERE id = ?";

    private static final String INSERT =
            "INSERT INTO employee (name, salary) VALUES (?, ?) RETURNING id, name, salary";

    private static final String UPDATE =
            "UPDATE employee SET name = ?, salary = ? WHERE id = ? RETURNING id, name, salary";

    private static final String DELETE =
            "DELETE FROM employee WHERE id = ? RETURNING id, name, salary";

    private static final String EXISTS =
            "SELECT COUNT(*) AS cnt FROM employee WHERE id = ?";

    private static final String ASSIGN_ROLE =
            "UPDATE employee SET role_id = ? WHERE id = ? RETURNING id, name, salary, role_id";

public void assignRole(int employeeId, int roleId) throws Exception {
    try {
        Map<String, Object> row = RepoUtil.executeQuery(ASSIGN_ROLE, roleId, employeeId);
        if (row.isEmpty()) throw ExceptionUtil.notFound("Employee not found with ID: " + employeeId);
    } catch (exception.AppException ae) {
        throw ae;
    } catch (Exception e) {
        throw new RuntimeException("Error assigning role to employee in the database", e);
    }
}

    public List<Map<String, Object>> findAll() throws Exception {
        try {
            return RepoUtil.fetchQuery(FETCH_ALL);
        } catch (Exception e) {
            throw ExceptionUtil.internalError("Failed to fetch employees");
        }
    }

    public Map<String, Object> findById(int id) throws Exception {
        try {
            Optional<Map<String, Object>> row = RepoUtil.fetchOne(FETCH_BY_ID, id);
            return row.orElseThrow(() ->
                    ExceptionUtil.notFound("Employee not found with ID: " + id));
        } catch (exception.AppException ae) {
            throw ae;
        } catch (Exception e) {
            throw ExceptionUtil.internalError("Failed to fetch employee with ID: " + id);
        }
    }

    public Map<String, Object> create(EmployeeRequest req) throws Exception {
        validateRequest(req);
        try {
            Map<String, Object> row = RepoUtil.executeQuery(INSERT, req.getName().trim(), req.getSalary());
            if (row.isEmpty()) throw ExceptionUtil.internalError("Insert returned no data");
            return row;
        } catch (exception.AppException ae) {
            throw ae;
        } catch (Exception e) {
            throw ExceptionUtil.internalError("Failed to create employee");
        }
    }

    public Map<String, Object> update(int id, EmployeeRequest req) throws Exception {
        validateRequest(req);
        assertExists(id);
        try {
            Map<String, Object> row = RepoUtil.executeQuery(UPDATE, req.getName(), req.getSalary(), id);
            if (row.isEmpty()) throw ExceptionUtil.notFound("Employee not found with ID: " + id);
            return row;
        } catch (exception.AppException ae) {
            throw ae;
        } catch (Exception e) {
            throw ExceptionUtil.internalError("Failed to update employee with ID: " + id);
        }
    }

    public Map<String, Object> delete(int id) throws Exception {
        assertExists(id);
        try {
            Map<String, Object> row = RepoUtil.executeQuery(DELETE, id);
            if (row.isEmpty()) throw ExceptionUtil.notFound("Employee not found with ID: " + id);
            return row;
        } catch (exception.AppException ae) {
            throw ae;
        } catch (Exception e) {
            throw ExceptionUtil.internalError("Failed to delete employee with ID: " + id);
        }
    }

    private void assertExists(int id) throws Exception {
        try {
            Map<String, Object> row = RepoUtil.executeQuery(EXISTS, id);
            Number count = (Number) row.get("cnt");
            if (count == null || count.intValue() == 0) {
                throw ExceptionUtil.notFound("Employee not found with ID: " + id);
            }
        } catch (exception.AppException ae) {
            throw ae;
        } catch (Exception e) {
            throw ExceptionUtil.internalError("Existence check failed for ID: " + id);
        }
    }

    private void validateRequest(EmployeeRequest req) throws Exception {
        if (req == null) {
            throw ExceptionUtil.badRequest("Request body must not be null");
        }
        if (req.getName() == null || req.getName().isBlank()) {
            throw ExceptionUtil.badRequest("Employee name must not be blank");
        }
        if (req.getName().trim().length() > 100) {
            throw ExceptionUtil.badRequest("Employee name must not exceed 100 characters");
        }
        if (req.getSalary() < 0) {
            throw ExceptionUtil.badRequest("Salary must not be negative");
        }
        if (req.getSalary() > 10_000_000) {
            throw ExceptionUtil.badRequest("Salary value is unreasonably large");
        }
    }
}
