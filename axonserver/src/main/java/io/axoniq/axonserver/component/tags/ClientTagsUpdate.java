package io.axoniq.axonserver.component.tags;


import io.axoniq.axonserver.message.ClientIdentification;

import java.util.Collections;
import java.util.Map;

/**
 * @author Sara Pellegrini
 * @since 4.2
 */
public class ClientTagsUpdate {

    private final ClientIdentification client;

    private final Map<String, String> tags;

    public ClientTagsUpdate(String clientName, String context, Map<String, String> tags) {
        this(new ClientIdentification(context, clientName), tags);
    }

    public ClientTagsUpdate(ClientIdentification client, Map<String, String> tags) {
        this.client = client;
        this.tags = tags;
    }

    public ClientIdentification client() {
        return client;
    }

    public Map<String, String> tags() {
        return Collections.unmodifiableMap(tags);
    }
}
