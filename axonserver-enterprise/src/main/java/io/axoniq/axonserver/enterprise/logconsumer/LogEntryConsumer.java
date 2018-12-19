package io.axoniq.axonserver.enterprise.logconsumer;

import io.axoniq.axonserver.grpc.cluster.Entry;

/**
 * Author: marc
 */
public interface LogEntryConsumer {
    void consumeLogEntry(String groupId, Entry entry);

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
