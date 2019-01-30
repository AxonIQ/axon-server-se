package io.axoniq.axonserver.enterprise.storage.transaction;

import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.SerializedEvent;

import java.util.List;
import java.util.function.Consumer;

/**
 * @author Marc Gathier
 */
public interface ReplicationManager {

    int getQuorum(String context);

    void registerListener(EventTypeContext type, Consumer<Long> replicationCompleted);

    void publish(EventTypeContext type, List<SerializedEvent> eventList, long token);
}
