package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.serializer.Printable;

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
     * @param client       the client identifier
     * @return true if new registration
     */
    boolean register(String clientStreamId, ClientContext client, ConnectionType type);

    /**
     * Unregisters the relation between a stream and a client id
     *
     * @param clientStreamId the unique stream identifier of the stream
     * @return true if registration existed
     */
    boolean unregister(String clientStreamId, ConnectionType type);

    /**
     * Returns the unique identifier and context of the client that opened the specified stream.
     *
     * @param clientStreamId the unique identifier of the stream opened by the client
     * @return the unique identifier of the client that opened the specified stream
     *
     * @throws IllegalStateException if the registry doesn't contain the specified stream id
     */
    ClientContext clientId(String clientStreamId);

    /**
     * Returns the identifiers of the set of platform streams opened by the specified client.
     *
     * @param clientContext the unique identifier of the client
     * @return the identifiers of the set of platform streams opened by the specific client
     */
    Set<String> streamIdsFor(ClientContext clientContext, ConnectionType type);

    default String streamIdFor(ClientContext client, ConnectionType type) {
        Set<String> streamIds = streamIdsFor(client, type);
        if (streamIds.isEmpty()) {
            throw new IllegalStateException("No " + type + " stream found for client " + client);
        }
        return streamIds.iterator().next();
    }

    enum ConnectionType {
        PLATFORM,
        QUERY,
        COMMAND
    }
}
