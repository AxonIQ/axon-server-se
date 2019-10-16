package io.axoniq.axonserver.component.version;

import io.axoniq.axonserver.message.ClientIdentification;

/**
 * @author Sara Pellegrini
 * @since 4.2.1
 */
public class ClientVersionUpdate {

    private final ClientIdentification client;

    private final String version;

    public ClientVersionUpdate(String clientName, String context, String version) {
        this(new ClientIdentification(context, clientName), version);
    }

    public ClientVersionUpdate(ClientIdentification client, String version) {
        this.client = client;
        this.version = version;
    }

    public ClientIdentification client() {
        return client;
    }

    public String version() {
        return version;
    }
}
