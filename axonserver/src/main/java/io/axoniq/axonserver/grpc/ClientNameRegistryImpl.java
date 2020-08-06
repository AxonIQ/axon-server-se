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
public class ClientNameRegistryImpl implements ClientNameRegistry {

    private final ConcurrentMap<String, String> clientMap = new ConcurrentHashMap();

    @Override
    public String register(String clientName) {
        String clientId = clientName + ":" + UUID.randomUUID().toString();
        clientMap.put(clientId, clientName);
        return clientId;
    }

    @Override
    public boolean register(String clientUUID, String clientName) {
        String prev = clientMap.put(clientUUID, clientName);
        return prev == null;
    }

    @Override
    public boolean unregister(String clientUUID) {
        return clientMap.remove(clientUUID) != null;
    }

    @Override
    public String clientNameOf(String clientUUID) throws IllegalStateException {
        if (!clientMap.containsKey(clientUUID)) {
            throw new IllegalStateException("Client " + clientUUID + " is not present in this registry.");
        }
        return clientMap.get(clientUUID);
    }

    @Override
    public Set<String> clientUuidsFor(String clientName) {
        return clientMap.entrySet()
                        .stream()
                        .filter(e -> clientName.equals(e.getValue()))
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toSet());
    }
}
