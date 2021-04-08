package io.axoniq.axonserver.refactoring.transport;

import io.axoniq.axonserver.refactoring.transport.rest.serializer.Printable;

import java.util.Set;

/**
 * @author Sara Pellegrini
 * @since 4.3.8, 4.4.1
 */
public interface ClientIdRegistry extends Printable {

    /**
     * Registers the relation between a stream and a client id
     *
     * @param clientStreamId the unique stream identifier of the stream
     * @param clientId       the client identifier
     * @return true if new registration
     */
    boolean register(String clientStreamId, String clientId, ConnectionType type);

    /**
     * Unregisters the relation between a stream and a client id
     *
     * @param clientStreamId the unique stream identifier of the stream
     * @return true if registration existed
     */
    boolean unregister(String clientStreamId, ConnectionType type);

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
     */
    Set<String> streamIdsFor(String clientId, ConnectionType type);

    default String streamIdFor(String clientId, ConnectionType type) {
        Set<String> streamIds = streamIdsFor(clientId, type);
        if (streamIds.isEmpty()) {
            throw new IllegalStateException("No " + type + " stream found for client " + clientId);
        }
        return streamIds.iterator().next();
    }

    enum ConnectionType {
        PLATFORM,
        QUERY,
        COMMAND
    }
}
