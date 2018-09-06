package io.axoniq.axonhub.localstorage.transformation;

import io.axoniq.axonhub.localstorage.file.StorageProperties;

/**
 * Author: marc
 */
public interface EventTransformerFactory {

    EventTransformer get(byte version, int flags, StorageProperties storageProperties);
}
