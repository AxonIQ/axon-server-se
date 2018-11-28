package io.axoniq.axonserver.cluster.replication.file;

/**
 * Author: marc
 */
public interface EventTransformerFactory {

    EventTransformer get(byte version, int flags, StorageProperties storageProperties);
}
