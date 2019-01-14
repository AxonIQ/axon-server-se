package io.axoniq.axonserver.enterprise.storage.transaction;

import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.EventTypeContext;

import java.util.List;
import java.util.function.Consumer;

/**
 * @author Marc Gathier
 */
public interface ReplicationManager {

    int getQuorum(String context);

    void registerListener(EventTypeContext type, Consumer<Long> replicationCompleted);

    void publish(EventTypeContext type, List<Event> eventList, long token);
}
