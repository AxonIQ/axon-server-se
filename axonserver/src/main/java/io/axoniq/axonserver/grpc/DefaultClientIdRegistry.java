package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.applicationevents.TopologyEvents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * @author Sara Pellegrini
 * @since 4.3.8, 4.4.1
 */
@Component
public class DefaultClientIdRegistry implements ClientIdRegistry {

    private final Logger logger = LoggerFactory.getLogger(DefaultClientIdRegistry.class);
    private final ConcurrentMap<String, String> clientMap = new ConcurrentHashMap();
    private final ConcurrentMap<String, Set<String>> platformStreamMap = new ConcurrentHashMap<>();

    public boolean registerPlatformConnection(String clientStreamId, String clientId) {
        String prev = clientMap.put(clientStreamId, clientId);
        platformStreamMap.computeIfAbsent(clientId, c -> new CopyOnWriteArraySet<>()).add(clientStreamId);
        return prev == null;
    }

    public boolean unregisterPlatformConnection(String clientStreamId) {
        String clientId = clientMap.remove(clientStreamId);
        if (clientId != null) {
            platformStreamMap.computeIfPresent(clientId, (c, current) -> {
                current.remove(clientStreamId);
                if (current.isEmpty()) {
                    return null;
                }
                return current;
            });
        }
        return clientId == null;
    }

    @Override
    public boolean register(String clientStreamId, String clientId) {
        String prev = clientMap.put(clientStreamId, clientId);
        return prev == null;
    }

    @Override
    public boolean unregister(String clientStreamId) {
        return clientMap.remove(clientStreamId) != null;
    }

    @Override
    public String clientId(String clientStreamId) {
        if (!clientMap.containsKey(clientStreamId)) {
            throw new IllegalStateException("Client " + clientStreamId + " is not present in this registry.");
        }
        return clientMap.get(clientStreamId);
    }

    @Override
    public Set<String> platformStreamIdsFor(String clientId) {
        Set<String> current = platformStreamMap.getOrDefault(clientId, Collections.emptySet());
        if (current.isEmpty()) {
            throw new IllegalStateException("No platform stream found for client " + clientId);
        }
        return Collections.unmodifiableSet(current);
    }

    @EventListener
    public void on(TopologyEvents.ApplicationConnected event) {
        registerPlatformConnection(event.getClientStreamId(), event.getClientId());
    }

    @EventListener
    public void on(TopologyEvents.ApplicationDisconnected event) {
        unregisterPlatformConnection(event.getClientStreamId());
    }
}
