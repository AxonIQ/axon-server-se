package io.axoniq.axonserver.component.tags;

import io.axoniq.axonserver.applicationevents.TopologyEvents.ApplicationDisconnected;
import io.axoniq.axonserver.message.ClientIdentification;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Provides the tags of all clients connected directly to the local Axon Server instance.
 *
 * @author Sara Pellegrini
 * @since 4.2
 */
@Component
public class ClientTagsCache implements Function<ClientIdentification, Map<String, String>> {

    private final Map<ClientIdentification, Map<String, String>> tags = new HashMap<>();

    /**
     * Returns a map of all tags defined from the specified client.
     *
     * @param client the client identifier
     * @return the tags map
     */
    @Override
    public Map<String, String> apply(ClientIdentification client) {
        return Collections.unmodifiableMap(tags.getOrDefault(client, Collections.emptyMap()));
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
        ClientIdentification client = new ClientIdentification(evt.getContext(), evt.getClientId());
        tags.remove(client);
    }
}
