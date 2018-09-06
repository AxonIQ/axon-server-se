package io.axoniq.axonhub.localstorage.transformation;

import io.axoniq.axonhub.localstorage.file.StorageProperties;

/**
 * Author: marc
 */
public class DefaultEventTransformerFactory implements EventTransformerFactory {
    @Override
    public EventTransformer get(byte version, int flags, StorageProperties storageProperties) {
        return NoOpEventTransformer.INSTANCE;
    }

}
