package io.axoniq.axonserver.cluster;

@FunctionalInterface
public interface Registration extends AutoCloseable {

    void cancel();

    @Override
    default void close() {
        cancel();
    }
}
