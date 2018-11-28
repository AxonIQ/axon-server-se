package io.axoniq.axonserver.cluster.replication.file;

/**
 * Author: marc
 */
public class DefaultEventTransformer implements EventTransformer {

    @Override
    public byte[] readEvent(byte[] bytes) {
        return bytes;
    }

    @Override
    public byte[] transform(byte[] bytes) {
        return bytes;
    }
}
