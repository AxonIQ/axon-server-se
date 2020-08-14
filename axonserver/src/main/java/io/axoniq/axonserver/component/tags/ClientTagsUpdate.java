package io.axoniq.axonserver.component.tags;


import io.axoniq.axonserver.message.ClientStreamIdentification;

import java.util.Collections;
import java.util.Map;

/**
 * Event containing client's tags that is published any time a node connects to the Axon Server instance or
 * when the client's tags have been updated.
 *
 * @author Sara Pellegrini
 * @since 4.2
 */
public class ClientTagsUpdate {

    private final ClientStreamIdentification client;

    private final Map<String, String> tags;

    /**
     * Creates an instance with the specified client's name, context and tags.
     *
     * @param clientName the client's name
     * @param context    the client's context
     * @param tags       the client's tags
     */
    public ClientTagsUpdate(String clientName, String context, Map<String, String> tags) {
        this(new ClientStreamIdentification(context, clientName), tags);
    }

    /**
     * Creates an instance for the specified client and tags.
     *
     * @param client the client identifier
     * @param tags   the client's tags
     */
    public ClientTagsUpdate(ClientStreamIdentification client, Map<String, String> tags) {
        this.client = client;
        this.tags = tags;
    }

    /**
     * Returns the client identifier.
     *
     * @return the client identifier.
     */
    public ClientStreamIdentification client() {
        return client;
    }

    /**
     * Returns the client's tags.
     * @return the client's tags
     */
    public Map<String, String> tags() {
        return Collections.unmodifiableMap(tags);
    }
}
