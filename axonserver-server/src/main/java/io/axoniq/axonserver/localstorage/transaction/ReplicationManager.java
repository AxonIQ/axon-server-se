package io.axoniq.axonserver.localstorage.transaction;

import io.axoniq.axondb.Event;
import io.axoniq.axonserver.localstorage.EventTypeContext;

import java.util.List;
import java.util.function.Consumer;

/**
 * Author: marc
 */
public interface ReplicationManager {

    int getQuorum(String context);

    void registerListener(EventTypeContext type, Consumer<Long> replicationCompleted);

    void publish(EventTypeContext type, List<Event> eventList, long token);
}
