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

    /**
     * Returns the unique identifier of the client that opened the specified stream
     *
     * @param clientStreamId the unique identifier of the stream opened by the client
     * @return the unique identifier of the client that opened the specified stream
     *
     * @throws IllegalStateException if the registry doesn't contain the specified stream id
     */
    String clientId(String clientStreamId);

    /**
     * Returns the identifiers of the set of streams opened by the specified client
     *
     * @param clientId the unique identifier of the client
     * @return the identifiers of the set of streams opened by the specifie client
     *
     * @throws IllegalStateException if the registry doesn't contain the specified client id
     */
    Set<String> clientStreamIdsFor(String clientId);
}
