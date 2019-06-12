package io.axoniq.axonserver.enterprise.logconsumer;

import io.axoniq.axonserver.grpc.cluster.Entry;

/**
 * Consumes log entries and applies them to the state machine. Implementations should be idempotent.
 *
 * @author Marc Gathier
 */
public interface LogEntryConsumer {

    void consumeLogEntry(String groupId, Entry entry) throws Exception;

    default int priority() {
        return 0;
    }

    default boolean entryType(Entry e, String name) {
        return e.hasSerializedObject() && e.getSerializedObject().getType().equals(name);
    }

    default boolean entryType(Entry e, Class clazz) {
        return entryType(e, clazz.getName());
    }
}
