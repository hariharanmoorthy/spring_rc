package com.hari.dto;

/**
 * Typed DTO returned for every employee response.
 * Using a concrete type instead of raw Map<String,Object>
 * improves serialisation control and IDE visibility.
 */
public class EmployeeResponse {

    private int    id;
    private String name;
    private double salary;

    public EmployeeResponse() {}

    public EmployeeResponse(int id, String name, double salary) {
        this.id     = id;
        this.name   = name;
        this.salary = salary;
    }

    public int    getId()     { return id;     }
    public String getName()   { return name;   }
    public double getSalary() { return salary; }

    public void setId(int id)          { this.id     = id;     }
    public void setName(String name)   { this.name   = name;   }
    public void setSalary(double sal)  { this.salary = sal;    }
}
