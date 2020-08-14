package io.axoniq.axonserver.grpc;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * @author Sara Pellegrini
 * @since 4.3.8, 4.4.1
 */
@Component
public class DefaultClientIdRegistry implements ClientIdRegistry {

    private final ConcurrentMap<String, String> clientMap = new ConcurrentHashMap();

    @Override
    public String register(String clientName) {
        String clientId = clientName + ":" + UUID.randomUUID().toString();
        clientMap.put(clientId, clientName);
        return clientId;
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
    public Set<String> clientStreamIdsFor(String clientId) {
        return clientMap.entrySet()
                        .stream()
                        .filter(e -> clientId.equals(e.getValue()))
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toSet());
    }
}
