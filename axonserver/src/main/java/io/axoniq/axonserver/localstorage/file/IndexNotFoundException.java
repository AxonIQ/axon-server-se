package io.axoniq.axonserver.localstorage.file;

/**
 * Signals that an attempt to open an index file has failed.
 *
 * @author Sara Pellegrini
 * @since 4.1.7
 */
public class IndexNotFoundException extends RuntimeException {

    public IndexNotFoundException() {
    }

    public IndexNotFoundException(String message) {
        super(message);
    }

    public IndexNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
