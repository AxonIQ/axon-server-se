package io.axoniq.axonserver.cluster.exception;

/**
 * @author Marc Gathier
 */
public enum ErrorCode {
    DIRECTORY_CREATION_FAILED,
    VALIDATION_FAILED,
    PAYLOAD_TOO_LARGE,
    DATAFILE_READ_ERROR,
    DATAFILE_WRITE_ERROR,
    INDEX_WRITE_ERROR,
    INTERRUPTED
    ;

}
