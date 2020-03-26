package io.axoniq.axonserver.enterprise.cluster.events;

import io.axoniq.axonserver.applicationevents.AxonServerEvent;
import io.axoniq.axonserver.enterprise.cluster.internal.MessagingClusterService;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand.RequestCase;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Receives all the internal events from other Axon Server nodes into the same cluster and publish them locally.
 *
 * @author Sara Pellegrini
 * @since 4.4
 */
@Component
public class RemoteEventReceiver {

    private final ApplicationEventPublisher localPublisher;

    private final Map<RequestCase, AxonServerEventSerializer<?>> deserializers = new HashMap<>();


    /**
     * @param localPublisher          used to publish locally the received internal events
     * @param serializers             used to deserialize the received internal events
     * @param messagingClusterService used to register this instance as a listener for all internal events
     *                                received from the other Axon Server nodes in the same cluster
     */
    @Autowired
    public RemoteEventReceiver(@Qualifier("applicationEventPublisher") ApplicationEventPublisher localPublisher,
                               List<AxonServerEventSerializer<?>> serializers,
                               MessagingClusterService messagingClusterService) {
        this(localPublisher, serializers, messagingClusterService::registerConnectorCommandHandler);
    }

    /**
     * @param localPublisher               used to publish locally the received internal events
     * @param serializers                  used to deserialize the received internal events
     * @param registrationOnInternalEvents used to register this instance as a listener for all internal events
     *                                     received from the other Axon Server nodes in the same cluster
     */
    public RemoteEventReceiver(ApplicationEventPublisher localPublisher,
                               List<AxonServerEventSerializer<?>> serializers,
                               BiConsumer<RequestCase, Consumer<ConnectorCommand>> registrationOnInternalEvents) {
        this.localPublisher = localPublisher;

        serializers.forEach(s -> {
            deserializers.put(s.requestCase(), s);
            registrationOnInternalEvents.accept(s.requestCase(), this::accept);
        });
    }

    /**
     * Publishes locally the internal {@link AxonServerEvent} wrapped in the {@link ConnectorCommand} accepted.
     *
     * @param connectorCommand the gRPC message containing the internal {@link AxonServerEvent}
     */
    private void accept(ConnectorCommand connectorCommand) {
        RequestCase requestCase = connectorCommand.getRequestCase();
        AxonServerEvent event = deserializers.get(requestCase).deserialize(connectorCommand);
        localPublisher.publishEvent(event);
    }
}
