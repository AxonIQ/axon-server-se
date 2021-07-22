package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.applicationevents.SubscriptionEvents.SubscribeCommand;
import io.axoniq.axonserver.applicationevents.SubscriptionEvents.SubscribeQuery;
import io.axoniq.axonserver.applicationevents.TopologyEvents.ApplicationConnected;
import io.axoniq.axonserver.applicationevents.TopologyEvents.ApplicationDisconnected;
import io.axoniq.axonserver.applicationevents.TopologyEvents.CommandHandlerDisconnected;
import io.axoniq.axonserver.applicationevents.TopologyEvents.QueryHandlerDisconnected;
import io.axoniq.axonserver.serializer.Media;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static io.axoniq.axonserver.grpc.ClientIdRegistry.ConnectionType.*;

/**
 * @author Sara Pellegrini
 * @since 4.3.8, 4.4.1
 */
@Component
public class DefaultClientIdRegistry implements ClientIdRegistry {

    private final Logger logger = LoggerFactory.getLogger(DefaultClientIdRegistry.class);

    //Map<ConnectionType, Map<streamId, clientId>>>
    private final Map<ConnectionType, Map<String, ClientContext>> clientIdMapPerType = new ConcurrentHashMap<>();

    @Override
    public boolean register(String clientStreamId, ClientContext client, ConnectionType type) {
        Map<String, ClientContext> connectionTypeMap = clientIdMapPerType.computeIfAbsent(type,
                                                                                   t -> new ConcurrentHashMap<>());
        ClientContext prev = connectionTypeMap.put(clientStreamId, client);
        if (logger.isInfoEnabled()) {
            Set<String> streamSet = connectionTypeMap.entrySet()
                                                     .stream()
                                                     .filter(e -> e.getValue().equals(client))
                                                     .map(Map.Entry::getKey)
                                                     .collect(Collectors.toSet());
            if (streamSet.size() != 1) {
                logger.info("Multiple mappings for {} stream for clientId {}: {}", type, client, streamSet);
            }
        }
        return prev == null;
    }

    @Override
    public boolean unregister(String clientStreamId, ConnectionType type) {
        Map<String, ClientContext> connectionTypeMap = clientIdMapPerType.getOrDefault(type, Collections.emptyMap());
        ClientContext clientId = connectionTypeMap.remove(clientStreamId);
        return clientId != null;
    }

    @Override
    public ClientContext clientId(String clientStreamId) {
        for (ConnectionType connectionType : values()) {
            ClientContext clientId = clientIdMapPerType.getOrDefault(connectionType, Collections.emptyMap())
                                                .get(clientStreamId);
            if (clientId != null) {
                return clientId;
            }
        }
        throw new IllegalStateException("Client " + clientStreamId + " is not present in this registry.");
    }

    @Override
    public Set<String> streamIdsFor(ClientContext clientContext, ConnectionType type) {
        Map<String, ClientContext> connectionTypeMap = clientIdMapPerType.getOrDefault(type, Collections.emptyMap());
        Set<String> current = connectionTypeMap.entrySet()
                                               .stream()
                                               .filter(e -> e.getValue().equals(clientContext))
                                               .map(Map.Entry::getKey)
                                               .collect(Collectors.toSet());
        return Collections.unmodifiableSet(current);
    }


    @Override
    public void printOn(Media media) {
        clientIdMapPerType.forEach((type, map) -> media.with(type.toString(), m -> map.forEach((k, v) -> m.with(k, v.toString()))));
    }


    @Order(0)
    @EventListener
    public void on(ApplicationConnected event) {
        logger.info("Platform stream connected: {} [stream id -> {}]",
                    event.getClientId(),
                    event.getClientStreamId());
        register(event.getClientStreamId(), new ClientContext(event.getClientId(), event.getContext()), PLATFORM);
    }

    @EventListener
    public void on(ApplicationDisconnected event) {
        logger.info("Platform stream disconnected: {} [stream id -> {}]",
                    event.getClientId(),
                    event.getClientStreamId());
        unregister(event.getClientStreamId(), PLATFORM);
    }

    @EventListener
    public void on(SubscribeCommand event) {
        String clientId = event.getHandler().getClientId();
        String clientStreamId = event.clientStreamIdentification().getClientStreamId();
        if (!clientIdMapPerType.getOrDefault(COMMAND, Collections.emptyMap()).containsKey(clientStreamId)) {
            logger.info("Command stream connected: {} [stream id -> {}]", clientId, clientStreamId);
        }
        register(clientStreamId, new ClientContext(clientId, event.getContext()), COMMAND);
    }

    @EventListener
    public void on(CommandHandlerDisconnected event) {
        logger.info("Command stream disconnected: {} [stream id -> {}]",
                    event.getClientId(),
                    event.getClientStreamId());
        unregister(event.getClientStreamId(), COMMAND);
    }

    @EventListener
    public void on(SubscribeQuery event) {
        String clientId = event.getQueryHandler().getClientId();
        String clientStreamId = event.clientIdentification().getClientStreamId();
        if (!clientIdMapPerType.getOrDefault(QUERY, Collections.emptyMap()).containsKey(clientStreamId)) {
            logger.info("Query stream connected: {} [stream id -> {}]", clientId, clientStreamId);
        }
        register(clientStreamId, new ClientContext(clientId, event.getContext()), QUERY);
    }

    @EventListener
    public void on(QueryHandlerDisconnected event) {
        logger.info("Query stream disconnected: {} [stream id -> {}]",
                    event.getClientId(),
                    event.getClientStreamId());
        unregister(event.getClientStreamId(), QUERY);
    }
}
