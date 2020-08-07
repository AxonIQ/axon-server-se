package io.axoniq.axonserver.component.version;

import io.axoniq.axonserver.applicationevents.TopologyEvents.ApplicationDisconnected;
import io.axoniq.axonserver.grpc.ClientNameRegistry;
import io.axoniq.axonserver.message.ClientIdentification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Provides the versions of all clients connected directly to the local Axon Server instance.
 *
 * @author Sara Pellegrini
 * @since 4.2.3
 */
@Component
public class ClientVersionsCache implements Function<ClientIdentification, String> {

    private Logger logger = LoggerFactory.getLogger(ClientVersionsCache.class);

    private final Map<ClientIdentification, String> versions = new HashMap<>();

    private final ClientNameRegistry clientNameRegistry;

    public ClientVersionsCache(ClientNameRegistry clientNameRegistry) {
        this.clientNameRegistry = clientNameRegistry;
    }

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
        logger.debug("Version update received from client {} to version {}.", update.client(), update.version());
    }

    /**
     * Deletes the version cached for a specific client when the client disconnects from the local instance.
     *
     * @param evt the client disconnection event
     */
    @EventListener
    public void on(ApplicationDisconnected evt) {
        ClientIdentification client = new ClientIdentification(evt.getContext(), evt.getClientId());
        versions.remove(client);
        logger.trace("Version cleaned for client {} because disconnected.", client);
    }
}
