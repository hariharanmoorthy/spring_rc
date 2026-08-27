package com.hari.controllers;

import com.hari.dto.RoleDTO;
import com.hari.service.RoleService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/roles")
public class RoleController {
    private final RoleService roleService;

    public RoleController(RoleService roleService) {
        this.roleService = roleService;
    }

    @GetMapping
    public List<Map<String,Object>> getAll() throws Exception{
        return roleService.getAll();
    }

    @PostMapping(value = "/create", consumes = "application/json")
    public Map<String,Object> createRole(@RequestBody RoleDTO role) throws Exception {
        return roleService.createRole(role.role());
    }
}
