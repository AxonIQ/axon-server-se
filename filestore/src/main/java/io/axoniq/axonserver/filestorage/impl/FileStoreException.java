package io.axoniq.axonserver.filestorage.impl;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public class FileStoreException extends RuntimeException {

    private final FileStoreErrorCode errorCode;

    public FileStoreException(FileStoreErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public FileStoreException(FileStoreErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }
}
