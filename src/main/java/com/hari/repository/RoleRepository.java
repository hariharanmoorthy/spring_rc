package com.hari.repository;

import com.hari.repository.repoUtil.RepoUtil;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository
public class RoleRepository {
    private static final String GETALLROLES = "SELECT * FROM role";

    private static final String InsertRole = "INSERT INTO role(name) VALUES (?) RETURNING *";

    public List<Map<String,Object>> findAll() {
        try {
            return RepoUtil.fetchQuery(GETALLROLES);
        } catch (Exception e) {
            throw new RuntimeException("Error fetching roles from the database", e);
        }
    }

    public Map<String,Object> createRole(String roleName) throws Exception {
        try {
           return RepoUtil.executeQuery(InsertRole, roleName);
        } catch (Exception e) {
            throw new RuntimeException("Error inserting role into the database", e);
        }
    }
}
