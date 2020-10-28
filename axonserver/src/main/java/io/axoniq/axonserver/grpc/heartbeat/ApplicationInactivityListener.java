package io.axoniq.axonserver.grpc.heartbeat;

import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.grpc.ClientIdRegistry;
import io.axoniq.axonserver.grpc.ClientIdRegistry.ConnectionType;
import io.axoniq.axonserver.grpc.CommandService;
import io.axoniq.axonserver.grpc.QueryService;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Listens to the application inactivity timeout events and force the disconnection of the streams to the client.
 *
 * @author Sara Pellegrini
 * @since 4.3.7, 4.4.1
 */
@Component
public class ApplicationInactivityListener {

    private final Map<ConnectionType, StreamCloser> streamClosers = new ConcurrentHashMap<>();
    private final ClientIdRegistry clientIdRegistry;

    @Autowired
    public ApplicationInactivityListener(ClientIdRegistry clientIdRegistry,
                                         CommandService commandService,
                                         QueryService queryService) {
        this(Collections.EMPTY_MAP, clientIdRegistry);
        streamClosers.put(ConnectionType.COMMAND, commandService::completeStreamForInactivity);
        streamClosers.put(ConnectionType.QUERY, queryService::completeStreamForInactivity);
    }

    public ApplicationInactivityListener(Map<ConnectionType, StreamCloser> streamClosers,
                                         ClientIdRegistry clientIdRegistry) {
        this.clientIdRegistry = clientIdRegistry;
        this.streamClosers.putAll(streamClosers);
    }

    /**
     * Forces the streams to be closed when a client turns out to be inactive/not properly connected.
     *
     * @param evt the event of inactivity timeout for a specific client component
     */
    @EventListener
    public void on(TopologyEvents.ApplicationInactivityTimeout evt) {
        String clientId = evt.clientId();
        String context = evt.clientStreamIdentification().getContext();
        streamClosers.forEach((connectionType, streamCloser) -> {
            Set<String> streamIds = clientIdRegistry.streamIdsFor(clientId, connectionType);
            for (String streamId : streamIds) {
                ClientStreamIdentification streamIdentification = new ClientStreamIdentification(context, streamId);
                streamCloser.forceDisconnection(clientId, streamIdentification);
            }
        });
    }

    @FunctionalInterface
    public interface StreamCloser {

        void forceDisconnection(String clientId, ClientStreamIdentification streamIdentification);
    }
}
