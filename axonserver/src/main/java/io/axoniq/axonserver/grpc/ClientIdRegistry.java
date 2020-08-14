package io.axoniq.axonserver.grpc;

import java.util.Set;

/**
 * @author Sara Pellegrini
 * @since 4.3.8, 4.4.1
 */
public interface ClientIdRegistry {

    String register(String clientId);

    boolean register(String clientStreamId, String clientId);

    boolean unregister(String clientStreamId);

    String clientId(String clientStreamId) throws IllegalStateException;

    Set<String> clientStreamIdsFor(String clientName) throws IllegalStateException;
}
