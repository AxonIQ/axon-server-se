package io.axoniq.axonserver.component.tags;

import io.axoniq.axonserver.applicationevents.TopologyEvents.ApplicationDisconnected;
import io.axoniq.axonserver.grpc.ClientIdRegistry;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Provides the tags of all clients connected to the local Axon Server instance.
 *
 * @author Sara Pellegrini
 * @since 4.2
 */
@Component
public class ClientTagsCache implements Function<ClientStreamIdentification, Map<String, String>> {

    private final Map<ClientStreamIdentification, Map<String, String>> tags = new HashMap<>();
    private final ClientIdRegistry clientIdRegistry;

    public ClientTagsCache(ClientIdRegistry clientIdRegistry) {
        this.clientIdRegistry = clientIdRegistry;
    }

    /**
     * Returns a map of all tags defined from the specified client.
     *
     * @param client the client identifier
     * @return the tags map
     */
    @Override
    public Map<String, String> apply(ClientStreamIdentification client) {
        try {
            String clientId = clientIdRegistry.clientId(client.getClientStreamId());
            if (clientId != null) {
                Set<String> platformClientStreamIds = clientIdRegistry.platformStreamIdsFor(clientId);
                if (!platformClientStreamIds.isEmpty()) {
                    client = new ClientStreamIdentification(client.getContext(),
                                                            platformClientStreamIds.iterator().next());
                }
            }
        } catch (IllegalStateException illegalStateException) {

        }
        return Collections.unmodifiableMap(
                tags.getOrDefault(client, Collections.emptyMap()));
    }

    /**
     * Updates the tags cached for a specific client when the client connects to the local instance.
     *
     * @param update the update event
     */
    @EventListener
    public void on(ClientTagsUpdate update) {
        tags.put(update.client(), update.tags());
    }

    /**
     * Deletes the tags cached for a specific client when the client disconnects from the local instance.
     *
     * @param evt the client disconnection event
     */
    @EventListener
    public void on(ApplicationDisconnected evt) {
        ClientStreamIdentification client = new ClientStreamIdentification(evt.getContext(), evt.getClientStreamId());
        tags.remove(client);
    }
}
