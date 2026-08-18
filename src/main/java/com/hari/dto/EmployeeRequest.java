package com.hari.dto;

/**
 * Request body for create / update employee operations.
 * Validation is enforced at the service/repository layer.
 */
public class EmployeeRequest {

    private String name;
    private double salary;

    public EmployeeRequest() {}

    public EmployeeRequest(String name, double salary) {
        this.name   = name;
        this.salary = salary;
    }

    public String getName()            { return name;   }
    public double getSalary()          { return salary; }
    public void   setName(String n)    { this.name   = n; }
    public void   setSalary(double s)  { this.salary = s; }
}
