package io.axoniq.axonserver.filestorage.impl;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public enum FileStoreErrorCode {
    VALIDATION_FAILED,
    DIRECTORY_CREATION_FAILED,
    INTERRUPTED, DATAFILE_READ_ERROR, PAYLOAD_TOO_LARGE, NOT_FOUND;
}
