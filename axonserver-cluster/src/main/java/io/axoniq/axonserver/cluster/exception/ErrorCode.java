package io.axoniq.axonserver.cluster.exception;

/**
 * Author: marc
 */
public enum ErrorCode {

    PAYLOAD_TOO_LARGE("AXONIQ-2001"),

    DIRECTORY_CREATION_FAILED("AXONIQ-9102"),
    VALIDATION_FAILED("AXONIQ-9200"),
    DATAFILE_READ_ERROR("AXONIQ-9000"),
    DATAFILE_WRITE_ERROR("AXONIQ-9100"),
    INDEX_WRITE_ERROR("AXONIQ-9101"),
    INTERRUPTED("AXONIQ-9500"),

    SERVER_TOO_SLOW("AXONIQ-10001"),
    UNCOMMITTED_CONFIGURATION("AXONIQ-10002"),
    REPLICATION_TIMEOUT("AXONIQ-10003"),
    ;

    private final String code;

    ErrorCode(String code) {
        this.code = code;
    }

    public String code() {
        return code;
    }}


