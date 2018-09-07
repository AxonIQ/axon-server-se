package io.axoniq.axonserver.localstorage.transformation;

import io.axoniq.axonserver.localstorage.file.StorageProperties;

/**
 * Author: marc
 */
public interface EventTransformerFactory {

    EventTransformer get(byte version, int flags, StorageProperties storageProperties);
}
