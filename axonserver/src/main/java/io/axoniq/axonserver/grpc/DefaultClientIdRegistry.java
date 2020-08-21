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
    private final Map<ConnectionType, Map<String, String>> clientIdMapPerType = new ConcurrentHashMap<>();

    @Override
    public boolean register(String clientStreamId, String clientId, ConnectionType type) {
        Map<String, String> connectionTypeMap = clientIdMapPerType.computeIfAbsent(type,
                                                                                   t -> new ConcurrentHashMap<>());
        String prev = connectionTypeMap.put(clientStreamId, clientId);
        Set<String> streamSet = connectionTypeMap.entrySet()
                                                 .stream()
                                                 .filter(e -> e.getValue().equals(clientId))
                                                 .map(Map.Entry::getKey)
                                                 .collect(Collectors.toSet());
        if (streamSet.size() != 1) {
            logger.warn("Multiple mapping for {} stream for clientId {}: {}", type, clientId, streamSet);
        }
        return prev == null;
    }

    @Override
    public boolean unregister(String clientStreamId, ConnectionType type) {
        Map<String, String> connectionTypeMap = clientIdMapPerType.getOrDefault(type, Collections.emptyMap());
        String clientId = connectionTypeMap.remove(clientStreamId);
        return clientId != null;
    }

    @Override
    public String clientId(String clientStreamId) {
        Map<String, String> clientMap = clientMap();
        if (!clientMap.containsKey(clientStreamId)) {
            throw new IllegalStateException("Client " + clientStreamId + " is not present in this registry.");
        }
        return clientMap.get(clientStreamId);
    }

    private Map<String, String> clientMap() {
        return clientIdMapPerType.values()
                                 .stream()
                                 .flatMap(map -> map.entrySet().stream())
                                 .collect(Collectors.toMap(Map.Entry::getKey,
                                                           Map.Entry::getValue));
    }

    @Override
    public Set<String> streamIdsFor(String clientId, ConnectionType type) {
        Map<String, String> connectionTypeMap = clientIdMapPerType.computeIfAbsent(type, t -> Collections.emptyMap());
        Set<String> current = connectionTypeMap.entrySet()
                                               .stream()
                                               .filter(e -> e.getValue().equals(clientId))
                                               .map(Map.Entry::getKey)
                                               .collect(Collectors.toSet());
        if (current.isEmpty()) {
            throw new IllegalStateException("No platform stream found for client " + clientId);
        }
        return Collections.unmodifiableSet(current);
    }


    @Override
    public void printOn(Media media) {
        clientIdMapPerType.forEach((type, map) -> media.with(type.toString(), m -> map.forEach(m::with)));
    }


    @EventListener
    public void on(ApplicationConnected event) {
        register(event.getClientStreamId(), event.getClientId(), PLATFORM);
    }

    @EventListener
    public void on(ApplicationDisconnected event) {
        unregister(event.getClientStreamId(), PLATFORM);
    }

    @EventListener
    public void on(SubscribeCommand event) {
        register(event.clientStreamIdentification().getClientStreamId(), event.getHandler().getClientId(), COMMAND);
    }

    @EventListener
    public void on(CommandHandlerDisconnected event) {
        unregister(event.getClientStreamId(), COMMAND);
    }

    @EventListener
    public void on(SubscribeQuery event) {
        register(event.clientIdentification().getClientStreamId(), event.getQueryHandler().getClientId(), QUERY);
    }

    @EventListener
    public void on(QueryHandlerDisconnected event) {
        unregister(event.getClientStreamId(), QUERY);
    }
}
