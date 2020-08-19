package io.axoniq.axonserver.grpc;

import java.util.Set;

/**
 * @author Sara Pellegrini
 * @since 4.4.1
 */
public interface ClientIdRegistry {

    /**
     * Registers the relation between a stream and a client id
     *
     * @param clientStreamId the unique stream identifier of the stream
     * @param clientId       the client identifier
     * @return true if new registration
     */
    boolean register(String clientStreamId, String clientId);

    void registerPlatform(String clientStreamId, String clientId);

    void unregisterPlatform(String clientStreamId);

    /**
     * Unregisters the relation between a stream and a client id
     *
     * @param clientStreamId the unique stream identifier of the stream
     * @return true if registration existed
     */
    boolean unregister(String clientStreamId);

    /**
     * Returns the unique identifier of the client that opened the specified stream.
     *
     * @param clientStreamId the unique identifier of the stream opened by the client
     * @return the unique identifier of the client that opened the specified stream
     *
     * @throws IllegalStateException if the registry doesn't contain the specified stream id
     */
    String clientId(String clientStreamId);

    /**
     * Returns the identifiers of the set of platform streams opened by the specified client.
     *
     * @param clientId the unique identifier of the client
     * @return the identifiers of the set of platform streams opened by the specific client
     *
     * @throws IllegalStateException if the registry doesn't contain the specified client id
     */
    Set<String> platformStreamIdsFor(String clientId);
}
