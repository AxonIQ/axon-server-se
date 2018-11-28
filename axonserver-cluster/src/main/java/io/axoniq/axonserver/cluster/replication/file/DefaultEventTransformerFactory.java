package io.axoniq.axonserver.cluster.replication.file;

/**
 * Author: marc
 */
public class DefaultEventTransformerFactory implements EventTransformerFactory {

    @Override
    public EventTransformer get(byte version, int flags, StorageProperties storageProperties) {
        return new DefaultEventTransformer();
    }
}
