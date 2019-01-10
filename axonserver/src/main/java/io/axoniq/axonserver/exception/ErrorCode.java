package io.axoniq.axonserver.exception;

import io.grpc.Status;
import org.springframework.http.HttpStatus;
import org.springframework.util.StringUtils;

/**
 * Author: marc
 */
public enum ErrorCode {
    // Generic errors processing client request
    AUTHENTICATION_TOKEN_MISSING("AXONIQ-1000", Status.PERMISSION_DENIED, HttpStatus.FORBIDDEN, true),
    AUTHENTICATION_INVALID_TOKEN("AXONIQ-1001", Status.PERMISSION_DENIED, HttpStatus.FORBIDDEN, true),
    NODE_IS_REPLICA("AXONIQ-1100", Status.UNAVAILABLE, HttpStatus.SERVICE_UNAVAILABLE, true),
    NO_SUCH_APPLICATION("AXONIQ-1300", Status.NOT_FOUND, HttpStatus.NOT_FOUND, true),
    NO_SUCH_NODE("AXONIQ-1301", Status.NOT_FOUND, HttpStatus.NOT_FOUND, true),
    CONTEXT_NOT_FOUND("AXONIQ-1302", Status.NOT_FOUND, HttpStatus.NOT_FOUND, true),
    NO_AXONSERVER_FOR_CONTEXT("AXONIQ-1400", Status.UNAVAILABLE, HttpStatus.SERVICE_UNAVAILABLE, true),
    AXONSERVER_NODE_NOT_CONNECTED("AXONIQ-1500", Status.UNAVAILABLE, HttpStatus.SERVICE_UNAVAILABLE, true),

    // Input errors
    INVALID_SEQUENCE("AXONIQ-2000", Status.OUT_OF_RANGE, HttpStatus.CONFLICT, true),
    PAYLOAD_TOO_LARGE("AXONIQ-2001", Status.INVALID_ARGUMENT, HttpStatus.BAD_REQUEST, true),
    TOO_MANY_EVENTS("AXONIQ-2002", Status.INVALID_ARGUMENT, HttpStatus.BAD_REQUEST, true),
    NO_MASTER_AVAILABLE("AXONIQ-2100", Status.NOT_FOUND, HttpStatus.NOT_FOUND, true),
    NOT_RUNNING_IN_CLUSTER("AXONIQ-2101", Status.NOT_FOUND, HttpStatus.BAD_REQUEST, true),
    INVALID_TRANSACTION_TOKEN("AXONIQ-2200", Status.OUT_OF_RANGE, HttpStatus.CONFLICT, true),
    CLUSTER_NOT_ALLOWED("AXONIQ-2301", Status.PERMISSION_DENIED, HttpStatus.FORBIDDEN, true),
    CONTEXT_CREATION_NOT_ALLOWED("AXONIQ-2302", Status.PERMISSION_DENIED, HttpStatus.FORBIDDEN, true),
    NOT_SUPPORTED_IN_DEVELOPMENT("AXONIQ-2303", Status.PERMISSION_DENIED, HttpStatus.FORBIDDEN, true),
    CANNOT_DELETE_DEFAULT("AXONIQ-2304", Status.PERMISSION_DENIED, HttpStatus.FORBIDDEN, true),
    MAX_CLUSTER_SIZE_REACHED("AXONIQ-2305", Status.PERMISSION_DENIED, HttpStatus.FORBIDDEN, true),
    ALREADY_MEMBER_OF_CLUSTER("AXONIQ-2306", Status.ALREADY_EXISTS, HttpStatus.NOT_ACCEPTABLE, true),
    NOT_A_MEMBER("AXONIQ-2307", Status.PERMISSION_DENIED, HttpStatus.FORBIDDEN, true),
    SAME_NODE_NAME("AXONIQ-2500", Status.INVALID_ARGUMENT, HttpStatus.BAD_REQUEST, true),
    UNKNOWN_HOST("AXONIQ-2501", Status.INVALID_ARGUMENT, HttpStatus.BAD_REQUEST, true),
    CANNOT_JOIN("AXONIQ-2502", Status.INVALID_ARGUMENT, HttpStatus.BAD_REQUEST, true),
    UNKNOWN_ROLE("AXONIQ-2510", Status.INVALID_ARGUMENT, HttpStatus.BAD_REQUEST, true),

    // Rate limit errors
    EVENT_RATE_EXCEEDED("AXONIQ-3000", Status.RESOURCE_EXHAUSTED, HttpStatus.TOO_MANY_REQUESTS, true),

    // Command handling errors
    NO_HANDLER_FOR_COMMAND("AXONIQ-4000", Status.UNAVAILABLE, HttpStatus.INTERNAL_SERVER_ERROR),
    CONNECTION_TO_HANDLER_LOST("AXONIQ-4001", Status.UNAVAILABLE, HttpStatus.INTERNAL_SERVER_ERROR),

    // Query handling errors
    NO_HANDLER_FOR_QUERY("AXONIQ-5000", Status.NOT_FOUND, HttpStatus.BAD_REQUEST, true),

    // Backup errors
    NODE_NOT_READY_FOR_BACKUP("AXONIQ-7000", Status.UNAVAILABLE, HttpStatus.SERVICE_UNAVAILABLE),

    // Client encryption errors - these never occur on the server, but listed here anyway to make sure
    // we keep the numbering unique
    CRYPTO_UNEXPECTED_ERROR("AXONIQ-8000", null, null),   // stuff that in theory cannot happen
    CRYPTO_KEY_ERROR("AXONIQ-8001", null, null),          // bad key - in AES, that means bad key length
    CRYPTO_DECRYPTION_ERROR("AXONIQ-8002",
                            null,
                            null),   // couldn't decrypt - cryptotext corrupted, or not the right key

    // Internal errors
    DATAFILE_READ_ERROR("AXONIQ-9000", Status.CANCELLED, HttpStatus.INTERNAL_SERVER_ERROR),
    INDEX_READ_ERROR("AXONIQ-9001", Status.CANCELLED, HttpStatus.INTERNAL_SERVER_ERROR),
    DATAFILE_WRITE_ERROR("AXONIQ-9100", Status.CANCELLED, HttpStatus.INTERNAL_SERVER_ERROR),
    INDEX_WRITE_ERROR("AXONIQ-9101", Status.CANCELLED, HttpStatus.INTERNAL_SERVER_ERROR),
    DIRECTORY_CREATION_FAILED("AXONIQ-9102", Status.CANCELLED, HttpStatus.INTERNAL_SERVER_ERROR),
    VALIDATION_FAILED("AXONIQ-9200", Status.CANCELLED, HttpStatus.INTERNAL_SERVER_ERROR),
    TRANSACTION_ROLLED_BACK("AXONIQ-9900", Status.CANCELLED, HttpStatus.INTERNAL_SERVER_ERROR),
    NO_EVENTSTORE("AXONIQ-6000", Status.UNAVAILABLE, HttpStatus.SERVICE_UNAVAILABLE),
    //
    OTHER("AXONIQ-0001", Status.CANCELLED, HttpStatus.INTERNAL_SERVER_ERROR);

    private final String code;
    private final Status grpcCode;
    private final HttpStatus httpCode;
    private boolean clientException;

    ErrorCode(String code, Status grpcCode, HttpStatus httpCode) {
        this.code = code;
        this.grpcCode = grpcCode;
        this.httpCode = httpCode;
    }

    ErrorCode(String code, Status grpcCode, HttpStatus httpCode, boolean clientException) {
        this(code, grpcCode, httpCode);
        this.clientException = clientException;
    }

    public static ErrorCode find(String errorCode) {
        if(StringUtils.isEmpty(errorCode)) return ErrorCode.OTHER;

        for (ErrorCode value : ErrorCode.values()) {
            if( value.code.equals(errorCode)) {
                return value;
            }
        }
        return ErrorCode.OTHER;
    }


    public String getCode() {
        return code;
    }

    public Status getGrpcCode() {
        return grpcCode;
    }

    public HttpStatus getHttpCode() {
        return httpCode;
    }

    public boolean isClientException() {
        return clientException;
    }

}


