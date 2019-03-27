package io.axoniq.axonserver.topology;

import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.message.event.EventStore;

import javax.annotation.PostConstruct;

/**
 * @author Marc Gathier
 */
public class DefaultEventStoreLocator implements EventStoreLocator {
    private final LocalEventStore localEventStore;

    public DefaultEventStoreLocator(LocalEventStore localEventStore) {
        this.localEventStore = localEventStore;
    }

    @PostConstruct
    public void init() {
        localEventStore.initContext(Topology.DEFAULT_CONTEXT, false);
    }

    @Override
    public boolean isMaster(String nodeName, String contextName) {
        return true;
    }

    @Override
    public EventStore getEventStore(String context) {
        if( Topology.DEFAULT_CONTEXT.equals(context))
            return localEventStore;
        return null;
    }

    @Override
    public String getMaster(String context) {
        return null;
    }
}
