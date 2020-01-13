package io.axoniq.axonserver.enterprise.cluster.events;

import io.axoniq.axonserver.applicationevents.AxonServerEvent;
import io.axoniq.axonserver.grpc.Publisher;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Forwards internal {@link AxonServerEvent}s to other Axon Server nodes into the same cluster.
 *
 * @author Sara Pellegrini
 * @since 4.4
 */
@Component
public class RemoteEventPublisher implements Publisher<AxonServerEvent> {

    private final Publisher<ConnectorCommand> publisher;

    private final Map<Class<?>, AxonServerEventSerializer<?>> serializers = new HashMap<>();

    /**
     * @param publisher   used to publish a {@link ConnectorCommand} to all remote Axon Server nodes in the same cluster
     * @param serializers used to serialize the {@link AxonServerEvent} to be sent
     */
    public RemoteEventPublisher(
            Publisher<ConnectorCommand> publisher,
            List<AxonServerEventSerializer<?>> serializers) {
        this.publisher = publisher;
        serializers.forEach(serializer -> this.serializers.put(serializer.eventType(), serializer));
    }

    /**
     * Publishes a {@link ConnectorCommand} to all the other Axon Server nodes in the same cluster wrapping the message
     *
     * @param event the {@link AxonServerEvent} to be sent to all the other Axon Server nodes
     */
    @Override
    public void publish(AxonServerEvent event) {
        ConnectorCommand message = serialize(event);
        publisher.publish(message);
    }

    private <T extends AxonServerEvent> ConnectorCommand serialize(T event) {
        if (serializers.containsKey(event.getClass())) {
            return ((AxonServerEventSerializer<T>) serializers.get(event.getClass())).serialize(event);
        }
        if (serializers.containsKey(AxonServerEvent.class)) {
            return ((AxonServerEventSerializer<AxonServerEvent>) serializers.get(AxonServerEvent.class))
                    .serialize(event);
        }
        throw new RuntimeException("No serializer found for event: " + event);
    }
}
