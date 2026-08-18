package exception;

import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.NoHandlerFoundException;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

@ControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(AppException.class)
    public ResponseEntity<Map<String, Object>> handleAppException(AppException ex) {
        return build(ex.getStatus().value(), ex.getStatus().getReasonPhrase(), ex.getMessage());
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<Map<String, Object>> handleUnreadableBody(HttpMessageNotReadableException ex) {
        return build(400, "Bad Request", "Malformed or missing request body");
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<Map<String, Object>> handleTypeMismatch(MethodArgumentTypeMismatchException ex) {
        return build(400, "Bad Request",
                "Invalid value '" + ex.getValue() + "' for parameter '" + ex.getName() + "'");
    }

    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    public ResponseEntity<Map<String, Object>> handleMethodNotAllowed(HttpRequestMethodNotSupportedException ex) {
        return build(405, "Method Not Allowed", ex.getMessage());
    }

    @ExceptionHandler(NoHandlerFoundException.class)
    public ResponseEntity<Map<String, Object>> handleNotFound(NoHandlerFoundException ex) {
        return build(404, "Not Found", "No resource at: " + ex.getRequestURL());
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleGeneric(Exception ex) {
        return build(500, "Internal Server Error", ex.getMessage());
    }

    private ResponseEntity<Map<String, Object>> build(int status, String error, String message) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status",    status);
        body.put("error",     error);
        body.put("message",   message);
        body.put("timestamp", Instant.now().toString());
        return ResponseEntity.status(status).body(body);
    }
}
