package io.axoniq.axonserver.component.instance;

import org.springframework.stereotype.Component;

import java.util.function.Function;

/**
 * Provides the context for the specified client application.
 *
 * @author Sara Pellegrini
 * @since 4.4
 */
@Component
public class ClientContextProvider implements Function<String, String> {

    private final Iterable<Client> allClients;

    /**
     * Creates an instance based on all connected clients.
     *
     * @param allClients all client applications connected to AxonServer.
     */
    public ClientContextProvider(Iterable<Client> allClients) {
        this.allClients = allClients;
    }

    /**
     * Returns the context for the specified client name.
     *
     * @param clientName the identifier of the client application
     * @return the context for the specified client application
     */
    @Override
    public String apply(String clientName) {
        for (Client client : allClients) {
            if (client.name().equals(clientName)) {
                return client.context();
            }
        }
        throw new IllegalArgumentException("Client not found");
    }
}
