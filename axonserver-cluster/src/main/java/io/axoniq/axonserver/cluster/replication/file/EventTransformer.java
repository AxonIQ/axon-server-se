package io.axoniq.axonserver.cluster.replication.file;

/**
 * Author: marc
 */
public interface EventTransformer {

    byte[] readEvent(byte[] bytes);

    byte[] transform(byte[] bytes);
}
