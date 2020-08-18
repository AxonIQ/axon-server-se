package io.axoniq.axonserver.component.version;

import io.axoniq.axonserver.message.ClientStreamIdentification;

/**
 * Event containing client's Axon Framework version that is published any time a node
 * connects to the Axon Server instance.
 *
 * @author Sara Pellegrini
 * @since 4.2.3
 */
public class ClientVersionUpdate {

    private final ClientStreamIdentification client;

    private final String version;

    /**
     * Creates an instance with the specified client's name, context and Axon Framework version.
     *
     * @param clientName the client's name
     * @param context    the client's context
     * @param version    the client's Axon Framework version
     */
    public ClientVersionUpdate(String clientName, String context, String version) {
        this(new ClientStreamIdentification(context, clientName), version);
    }

    /**
     * Creates an instance for the specified client and Axon Framework version.
     *
     * @param client  the client identifier
     * @param version the client's Axon Framework version
     */
    public ClientVersionUpdate(ClientStreamIdentification client, String version) {
        this.client = client;
        this.version = version;
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
     * Returns the client's Axon Framework version.
     * @return the client's Axon Framework version.
     */
    public String version() {
        return version;
    }
}
