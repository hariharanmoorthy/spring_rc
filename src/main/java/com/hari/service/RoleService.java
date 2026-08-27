package com.hari.service;

import com.hari.repository.RoleRepository;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class RoleService {

    private final RoleRepository roleRepository;

    public RoleService(RoleRepository roleRepository) {
        this.roleRepository = roleRepository;
    }

    public List<Map<String,Object>> getAll() {
        return roleRepository.findAll();
    }

    public Map<String,Object> createRole(String roleName) throws Exception {
        if (roleName == null || roleName.trim().isEmpty()) {
            throw new IllegalArgumentException("Role name cannot be null or empty");
        }
        return roleRepository.createRole(roleName);
    }
}
