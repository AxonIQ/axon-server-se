package io.axoniq.axonserver.localstorage.transformation;

import io.axoniq.axonserver.localstorage.file.StorageProperties;

/**
 * Author: marc
 */
public class DefaultEventTransformerFactory implements EventTransformerFactory {
    @Override
    public EventTransformer get(byte version, int flags, StorageProperties storageProperties) {
        return NoOpEventTransformer.INSTANCE;
    }

}
