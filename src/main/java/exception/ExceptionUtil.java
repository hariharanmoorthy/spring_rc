package exception;

import org.springframework.http.HttpStatus;

public final class ExceptionUtil {

    private ExceptionUtil() { /* utility class */ }

    /** 400 Bad Request */
    public static AppException badRequest(String message) {
        return new AppException(HttpStatus.BAD_REQUEST, message);
    }

    /** 400 Bad Request — alias kept for backward compatibility */
    public static AppException ValidationException(String message) {
        return badRequest(message);
    }

    /** 404 Not Found */
    public static AppException notFound(String message) {
        return new AppException(HttpStatus.NOT_FOUND, message);
    }

    /** 409 Conflict */
    public static AppException conflict(String message) {
        return new AppException(HttpStatus.CONFLICT, message);
    }

    /** 500 Internal Server Error */
    public static AppException internalError(String message) {
        return new AppException(HttpStatus.INTERNAL_SERVER_ERROR, message);
    }
}
