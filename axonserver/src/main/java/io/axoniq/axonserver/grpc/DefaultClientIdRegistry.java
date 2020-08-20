package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.serializer.Media;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * @author Sara Pellegrini
 * @since 4.3.8, 4.4.1
 */
@Component
public class DefaultClientIdRegistry implements ClientIdRegistry {

    private final Map<String, String> clientMap = new ConcurrentHashMap<>();
    private final Map<ConnectionType, Map<String, Set<String>>>
            clientIdMapPerType = new ConcurrentHashMap<>();

    @Override
    public boolean register(String clientStreamId, String clientId, ConnectionType type) {
        String prev = clientMap.put(clientStreamId, clientId);
        registerStreamForClient(clientStreamId, clientId, type);
        return prev == null;
    }

    @Override
    public boolean unregister(String clientStreamId, ConnectionType type) {
        String clientId = clientMap.remove(clientStreamId);
        if (clientId != null) {
            unregisterStreamForClient(clientStreamId, clientId, type);
        }
        return clientId != null;
    }

    @Override
    public String clientId(String clientStreamId) {
        if (!clientMap.containsKey(clientStreamId)) {
            throw new IllegalStateException("Client " + clientStreamId + " is not present in this registry.");
        }
        return clientMap.get(clientStreamId);
    }

    @Override
    public Set<String> streamIdsFor(String clientId, ConnectionType type) {
        Set<String> current = clientIdMapPerType.computeIfAbsent(type, t -> Collections.emptyMap())
                                                .getOrDefault(clientId, Collections.emptySet());
        if (current.isEmpty()) {
            throw new IllegalStateException("No platform stream found for client " + clientId);
        }
        return Collections.unmodifiableSet(current);
    }


    @Override
    public void printOn(Media media) {
        media.with("clientMap", clientMap);
        clientIdMapPerType.forEach((type, mappings) ->
                                           mappings.forEach((clientId, streamIds) ->
                                                                    media.with(type + "." + clientId,
                                                                               String.valueOf(streamIds))));
    }

    private void registerStreamForClient(String clientStreamId, String clientId, ConnectionType type) {
        clientIdMapPerType.computeIfAbsent(type, t -> new ConcurrentHashMap<>())
                          .computeIfAbsent(clientId, c -> new CopyOnWriteArraySet<>()).add(clientStreamId);
    }

    private void unregisterStreamForClient(String clientStreamId, String clientId, ConnectionType type) {
        clientIdMapPerType.getOrDefault(type, Collections.emptyMap())
                          .computeIfPresent(clientId, (c, current) -> {
                              current.remove(clientStreamId);
                              if (current.isEmpty()) {
                                  return null;
                              }
                              return current;
                          });
    }
}
