package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import org.jetbrains.annotations.NotNull;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Iteratable of currently connected AxonServer nodes.
 *
 * @author Marc Gathier
 * @since 4.3
 */
@Component
public class ActiveConnections implements Iterable<String> {

    private final Set<String> activeConnections = new CopyOnWriteArraySet<>();

    @NotNull
    @Override
    public Iterator<String> iterator() {
        return activeConnections.iterator();
    }

    /**
     * Event handler that updates the activeConnections when an Axon Server node is connected.
     *
     * @param connected the event
     */
    @EventListener
    public void on(ClusterEvents.AxonServerInstanceConnected connected) {
        activeConnections.add(connected.getNodeName());
    }

    /**
     * Event handler that updates the activeConnections when an Axon Server node is disconnected.
     *
     * @param disconnected the event
     */
    @EventListener
    public void on(ClusterEvents.AxonServerInstanceDisconnected disconnected) {
        activeConnections.remove(disconnected.getNodeName());
    }
}
