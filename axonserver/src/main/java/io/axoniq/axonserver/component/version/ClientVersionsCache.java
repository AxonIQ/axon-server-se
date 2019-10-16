package io.axoniq.axonserver.component.version;

import io.axoniq.axonserver.applicationevents.TopologyEvents.ApplicationDisconnected;
import io.axoniq.axonserver.message.ClientIdentification;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Provides the versions of all clients connected directly to the local Axon Server instance.
 *
 * @author Sara Pellegrini
 * @since 4.2.1
 */
@Component
public class ClientVersionsCache implements Function<ClientIdentification, String> {

    private final Map<ClientIdentification, String> versions = new HashMap<>();

    /**
     * Returns the version for the specified client.
     *
     * @param client the client identifier
     * @return the axon framework version
     */
    @Override
    public String apply(ClientIdentification client) {
        return versions.get(client);
    }

    /**
     * Updates the version cached for a specific client when the client connects to the local instance.
     *
     * @param update the update event
     */
    @EventListener
    public void on(ClientVersionUpdate update) {
        versions.put(update.client(), update.version());
    }

    /**
     * Deletes the version cached for a specific client when the client disconnects from the local instance.
     *
     * @param evt the client disconnection event
     */
    @EventListener
    public void on(ApplicationDisconnected evt) {
        ClientIdentification client = new ClientIdentification(evt.getContext(), evt.getClient());
        versions.remove(client);
    }
}
