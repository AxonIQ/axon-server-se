package io.axoniq.axonserver.enterprise.cluster.events;

import io.axoniq.axonserver.applicationevents.AxonServerEvent;
import io.axoniq.axonserver.applicationevents.AxonServerEventPublisher;
import io.axoniq.axonserver.applicationevents.LocalEventPublisher;
import io.axoniq.axonserver.grpc.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;

/**
 * Publish the internal events to the listeners registered both in the current Axon Server instance
 * and in the other Axon Server instances that are part of the current cluster.
 *
 * @author Sara Pellegrini
 * @since 4.4
 */
@Primary
@Component
public class ClusterEventPublisher implements AxonServerEventPublisher {

    private final Publisher<AxonServerEvent> remotePublisher;

    private final Publisher<Object> localPublisher;

    @Autowired
    public ClusterEventPublisher(RemoteEventPublisher remotePublisher,
                                 LocalEventPublisher localPublisher) {
        this(remotePublisher, localPublisher::publishEvent);
    }

    /**
     * Creates an instance with the specified remote and local publisher.
     *
     * @param remotePublisher used to publish internal events to other Axon Server instances into the same cluster
     * @param localPublisher  used to publish internal events to the local instance of Axon Server
     */
    public ClusterEventPublisher(
            Publisher<AxonServerEvent> remotePublisher,
            Publisher<Object> localPublisher) {
        this.remotePublisher = remotePublisher;
        this.localPublisher = localPublisher;
    }

    /**
     * Notifies all matching listeners registered with this internal event type in the current local instance of Axon
     * Server. If the event is an instance of {@link AxonServerEvent}, the publisher notifies also the remote Axon
     * Server instances that are part of the current cluster.
     *
     * @param event the event to publish
     */
    @Override
    public void publishEvent(@Nonnull Object event) {
        if (event instanceof AxonServerEvent) {
            AxonServerEvent clusterScopeEvent = (AxonServerEvent) event;
            remotePublisher.publish(clusterScopeEvent);
        }
        localPublisher.publish(event);
    }
}
