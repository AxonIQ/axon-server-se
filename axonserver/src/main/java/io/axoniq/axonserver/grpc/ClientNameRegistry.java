package io.axoniq.axonserver.grpc;

import java.util.Set;

/**
 * @author Sara Pellegrini
 * @since 4.3.8, 4.4.1
 */
public interface ClientNameRegistry {

    String register(String clientName);

    boolean register(String clientUUID, String clientName);

    boolean unregister(String clientUUID);

    String clientNameOf(String clientUUID) throws IllegalStateException;

    Set<String> clientUuidsFor(String clientName);
}
