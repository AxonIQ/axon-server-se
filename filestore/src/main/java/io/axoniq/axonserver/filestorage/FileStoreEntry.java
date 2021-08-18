package io.axoniq.axonserver.filestorage;

/**
 * @author Marc Gathier
 * @since
 */
public interface FileStoreEntry {
    byte[] bytes();

    byte version();
}
