package io.axoniq.axonserver.localstorage.transformation;

/**
 * Transformer to transform events between protobuf event bytes and stored event bytes.
 * Transformer implementations may perform compression or encryption before storing.
 * @author Marc Gathier
 */
public interface EventTransformer {

    /**
     * Converts stored bytes to protobuf bytes.
     * @param eventBytes bytes as stored
     * @return protobuf event bytes
     */
    byte[] fromStorage(byte[] eventBytes);

    /**
     * Converts protobuf event bytes to bytes to store.
     * @param bytes protobuf event bytes
     * @return transformed bytes to store
     */
    byte[] toStorage(byte[] bytes);
}
