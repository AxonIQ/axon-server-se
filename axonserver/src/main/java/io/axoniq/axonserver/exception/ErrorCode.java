/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.exception;

import io.grpc.Status;
import org.springframework.http.HttpStatus;
import org.springframework.util.StringUtils;

/**
 * Error codes and their mappings to gRPC errors and HTTP errors.
 *
 * @author Marc Gathier
 */
public enum ErrorCode {
    // Generic errors processing client request
    AUTHENTICATION_TOKEN_MISSING("AXONIQ-1000", Status.PERMISSION_DENIED, HttpStatus.FORBIDDEN, true),
    AUTHENTICATION_INVALID_TOKEN("AXONIQ-1001", Status.PERMISSION_DENIED, HttpStatus.FORBIDDEN, true),
    UNSUPPORTED_INSTRUCTION("AXONIQ-1002", Status.UNKNOWN, HttpStatus.BAD_REQUEST, true),
    INSTRUCTION_EXECUTION_ERROR("AXONIQ-1003", Status.INTERNAL, HttpStatus.INTERNAL_SERVER_ERROR, false),
    INSTRUCTION_RESULT_TIMEOUT("AXONIQ-1004", Status.INTERNAL, HttpStatus.INTERNAL_SERVER_ERROR, false),
    NODE_IS_REPLICA("AXONIQ-1100", Status.UNAVAILABLE, HttpStatus.SERVICE_UNAVAILABLE, true),
    NO_SUCH_APPLICATION("AXONIQ-1300", Status.NOT_FOUND, HttpStatus.NOT_FOUND, true),
    NO_SUCH_NODE("AXONIQ-1301", Status.NOT_FOUND, HttpStatus.NOT_FOUND, true),
    CONTEXT_NOT_FOUND("AXONIQ-1302", Status.NOT_FOUND, HttpStatus.NOT_FOUND, true),
    CONTEXT_EXISTS("AXONIQ-1304", Status.ALREADY_EXISTS, HttpStatus.BAD_REQUEST, true),
    NO_AXONSERVER_FOR_CONTEXT("AXONIQ-1400", Status.UNAVAILABLE, HttpStatus.SERVICE_UNAVAILABLE, true),
    AXONSERVER_NODE_NOT_CONNECTED("AXONIQ-1500", Status.UNAVAILABLE, HttpStatus.SERVICE_UNAVAILABLE, true),
    TOO_MANY_REQUESTS("AXONIQ-1900", Status.RESOURCE_EXHAUSTED, HttpStatus.TOO_MANY_REQUESTS),

    // Input errors
    INVALID_SEQUENCE("AXONIQ-2000", Status.OUT_OF_RANGE, HttpStatus.CONFLICT, true),
    PAYLOAD_TOO_LARGE("AXONIQ-2001", Status.INVALID_ARGUMENT, HttpStatus.BAD_REQUEST, true),
    TOO_MANY_EVENTS("AXONIQ-2002", Status.INVALID_ARGUMENT, HttpStatus.BAD_REQUEST, true),
    NO_LEADER_AVAILABLE("AXONIQ-2100", Status.NOT_FOUND, HttpStatus.NOT_FOUND, true),
    NOT_RUNNING_IN_CLUSTER("AXONIQ-2101", Status.NOT_FOUND, HttpStatus.BAD_REQUEST, true),
    CONTEXT_UPDATE_IN_PROGRESS("AXONIQ-2102", Status.NOT_FOUND, HttpStatus.BAD_REQUEST, false),
    INVALID_TRANSACTION_TOKEN("AXONIQ-2200", Status.OUT_OF_RANGE, HttpStatus.CONFLICT, true),
    CLUSTER_NOT_ALLOWED("AXONIQ-2301", Status.PERMISSION_DENIED, HttpStatus.FORBIDDEN, true),
    CONTEXT_CREATION_NOT_ALLOWED("AXONIQ-2302", Status.PERMISSION_DENIED, HttpStatus.FORBIDDEN, true),
    NOT_SUPPORTED_IN_DEVELOPMENT("AXONIQ-2303", Status.PERMISSION_DENIED, HttpStatus.FORBIDDEN, true),
    CANNOT_DELETE_INTERNAL_CONTEXT("AXONIQ-2304", Status.PERMISSION_DENIED, HttpStatus.FORBIDDEN, true),
    MAX_CLUSTER_SIZE_REACHED("AXONIQ-2305", Status.PERMISSION_DENIED, HttpStatus.FORBIDDEN, true),
    ALREADY_MEMBER_OF_CLUSTER("AXONIQ-2306", Status.ALREADY_EXISTS, HttpStatus.NOT_ACCEPTABLE, true),
    NOT_A_MEMBER("AXONIQ-2307", Status.PERMISSION_DENIED, HttpStatus.FORBIDDEN, true),
    INVALID_CONTEXT_NAME("AXONIQ-2308", Status.INVALID_ARGUMENT, HttpStatus.BAD_REQUEST, true),
    SELECT_NODES_FOR_CONTEXT("AXONIQ-2309", Status.INVALID_ARGUMENT, HttpStatus.BAD_REQUEST, true),
    CANNOT_REMOVE_LAST_NODE("AXONIQ-2310", Status.FAILED_PRECONDITION, HttpStatus.NOT_ACCEPTABLE, true),
    INVALID_PROPERTY_VALUE("AXONIQ-2311", Status.INVALID_ARGUMENT, HttpStatus.BAD_REQUEST, true),
    SAME_NODE_NAME("AXONIQ-2500", Status.INVALID_ARGUMENT, HttpStatus.BAD_REQUEST, true),
    UNKNOWN_HOST("AXONIQ-2501", Status.INVALID_ARGUMENT, HttpStatus.BAD_REQUEST, true),
    CANNOT_JOIN("AXONIQ-2502", Status.INVALID_ARGUMENT, HttpStatus.BAD_REQUEST, true),
    UNKNOWN_ROLE("AXONIQ-2510", Status.INVALID_ARGUMENT, HttpStatus.BAD_REQUEST, true),
    INVALID_QUERY("AXONIQ-2511", Status.INVALID_ARGUMENT, HttpStatus.BAD_REQUEST, true),

    SCHEDULED_EVENT_NOT_FOUND("AXONIQ-2610", Status.INVALID_ARGUMENT, HttpStatus.BAD_REQUEST, true),
    EXCEPTION_IN_INTERCEPTOR("AXONIQ-6100", Status.UNKNOWN, HttpStatus.INTERNAL_SERVER_ERROR),

    // Rate limit errors
    EVENT_RATE_EXCEEDED("AXONIQ-3000", Status.RESOURCE_EXHAUSTED, HttpStatus.TOO_MANY_REQUESTS, true),
    EVENT_REJECTED_BY_INTERCEPTOR("AXONIQ-3004", Status.INVALID_ARGUMENT, HttpStatus.BAD_REQUEST),
    SNAPSHOT_REJECTED_BY_INTERCEPTOR("AXONIQ-3005", Status.INVALID_ARGUMENT, HttpStatus.BAD_REQUEST),

    // Command handling errors
    NO_HANDLER_FOR_COMMAND("AXONIQ-4000", Status.NOT_FOUND, HttpStatus.INTERNAL_SERVER_ERROR),
    CONNECTION_TO_HANDLER_LOST("AXONIQ-4001", Status.CANCELLED, HttpStatus.INTERNAL_SERVER_ERROR),
    COMMAND_TIMEOUT("AXONIQ-4002", Status.CANCELLED, HttpStatus.INTERNAL_SERVER_ERROR),
    COMMAND_DISPATCH_ERROR("AXONIQ-4003", Status.CANCELLED, HttpStatus.INTERNAL_SERVER_ERROR),
    COMMAND_REJECTED_BY_INTERCEPTOR("AXONIQ-4004", Status.INVALID_ARGUMENT, HttpStatus.BAD_REQUEST),

    // Query handling errors
    NO_HANDLER_FOR_QUERY("AXONIQ-5000", Status.NOT_FOUND, HttpStatus.BAD_REQUEST, true),
    QUERY_DISPATCH_ERROR("AXONIQ-5002", Status.NOT_FOUND, HttpStatus.BAD_REQUEST, true),
    QUERY_REJECTED_BY_INTERCEPTOR("AXONIQ-5004", Status.INVALID_ARGUMENT, HttpStatus.BAD_REQUEST),
    SUBSCRIPTION_QUERY_REJECTED_BY_INTERCEPTOR("AXONIQ-5005", Status.INVALID_ARGUMENT, HttpStatus.BAD_REQUEST),

    NO_EVENTSTORE("AXONIQ-6000", Status.NOT_FOUND, HttpStatus.SERVICE_UNAVAILABLE),
    CLIENT_DISCONNECTED("AXONIQ-6001", Status.NOT_FOUND, HttpStatus.INTERNAL_SERVER_ERROR),

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
    INTERRUPTED("AXONIQ-9500", Status.CANCELLED, HttpStatus.INTERNAL_SERVER_ERROR),
    EXTENSIONS_DISABLED("AXONIQ-9301", Status.CANCELLED, HttpStatus.INTERNAL_SERVER_ERROR),

    //cluster error
    SERVER_TOO_SLOW("AXONIQ-10001", Status.CANCELLED, HttpStatus.INTERNAL_SERVER_ERROR),
    UNCOMMITTED_CONFIGURATION("AXONIQ-10002", Status.CANCELLED, HttpStatus.INTERNAL_SERVER_ERROR),
    UNCOMMITTED_TERM("AXONIQ-10007", Status.CANCELLED, HttpStatus.INTERNAL_SERVER_ERROR),
    REPLICATION_TIMEOUT("AXONIQ-10003", Status.CANCELLED, HttpStatus.INTERNAL_SERVER_ERROR),

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

    public static ErrorCode fromException(Exception ex) {
        if( ex instanceof MessagingPlatformException) return ((MessagingPlatformException) ex).getErrorCode();
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


