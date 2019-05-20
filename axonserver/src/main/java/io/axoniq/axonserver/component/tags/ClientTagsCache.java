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
 * @author Sara Pellegrini
 * @since 4.2
 */
@Component
public class ClientTagsCache implements Function<ClientIdentification, Map<String, String>> {

    private final Map<ClientIdentification, Map<String, String>> tags = new HashMap<>();

    @Override
    public Map<String, String> apply(ClientIdentification client) {
        return tags.getOrDefault(client, Collections.emptyMap());
    }

    @EventListener
    public void on(ClientTagsUpdate update) {
        tags.put(update.client(), update.tags());
    }

    @EventListener
    public void on(ApplicationDisconnected evt) {
        ClientIdentification client = new ClientIdentification(evt.getContext(), evt.getClient());
        tags.remove(client);
    }
}
