package com.hari.dto;

import java.time.Instant;

public class ApiResponse<T> {

    private final boolean success;
    private final String message;
    private final T data;
    private final String timestamp;

    private ApiResponse(boolean success, String message, T data) {
        this.success   = success;
        this.message   = message;
        this.data      = data;
        this.timestamp = Instant.now().toString();
    }

    public static <T> ApiResponse<T> ok(T data) {
        return new ApiResponse<>(true, "Success", data);
    }

    public static <T> ApiResponse<T> ok(String message, T data) {
        return new ApiResponse<>(true, message, data);
    }

    public static <T> ApiResponse<T> created(T data) {
        return new ApiResponse<>(true, "Created successfully", data);
    }

    public boolean isSuccess()  { return success;   }
    public String  getMessage() { return message;   }
    public T       getData()    { return data;       }
    public String  getTimestamp(){ return timestamp; }
}
