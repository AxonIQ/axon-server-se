package io.axoniq.axonserver.localstorage.transformation;

import io.axoniq.axonserver.localstorage.file.StorageProperties;

/**
 * Defines the interface for an EventTransformer factory.
 * @author Marc Gathier
 */
public interface EventTransformerFactory {

    /**
     * Get an event transformer. The transformer may be based on version and flags (for existing data) or based on settings in the storage properties (for new data).
     * @param version the version for existing data
     * @param flags the flags for existing data
     * @param storageProperties storage properties to consider for new data
     * @return the transformer
     */
    EventTransformer get(byte version, int flags, StorageProperties storageProperties);
}
