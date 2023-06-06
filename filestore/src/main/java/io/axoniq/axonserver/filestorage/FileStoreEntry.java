package io.axoniq.axonserver.filestorage;

/**
 * Generic description of an entry in a file store.
 * @author Marc Gathier
 * @since 4.6.0
 */
public interface FileStoreEntry {

    /**
     * Gets the data bytes for the entry.
     * @return byte array of data for the entry
     */
    byte[] bytes();

    /**
     * Gets version information for the entry.
     * @return version information for the entry
     */
    byte version();
}
