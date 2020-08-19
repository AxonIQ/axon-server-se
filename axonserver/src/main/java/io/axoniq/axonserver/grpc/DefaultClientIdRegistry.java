package io.axoniq.axonserver.grpc;

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

    private final ConcurrentMap<String, String> clientMap = new ConcurrentHashMap();
    private final ConcurrentMap<String, Set<String>> platformStreamMap = new ConcurrentHashMap<>();

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

    @Override
    public void registerPlatform(String clientStreamId, String clientId) {
        register(clientStreamId, clientId);
        registerPlatformConnection(clientStreamId, clientId);
    }

    @Override
    public void unregisterPlatform(String clientStreamId) {
        String clientId = clientMap.remove(clientStreamId);
        if (clientId != null) {
            unregisterPlatformConnection(clientStreamId, clientId);
        }
    }

    private void registerPlatformConnection(String clientStreamId, String clientId) {
        platformStreamMap.computeIfAbsent(clientId, c -> new CopyOnWriteArraySet<>()).add(clientStreamId);
    }

    private void unregisterPlatformConnection(String clientStreamId, String clientId) {
        platformStreamMap.computeIfPresent(clientId, (c, current) -> {
            current.remove(clientStreamId);
            if (current.isEmpty()) {
                return null;
            }
            return current;
        });
    }
}
