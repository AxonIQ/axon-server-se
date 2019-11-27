package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.Role;

/**
 * @author Marc Gathier
 */
public interface ReplicatorPeerStatus {

    long lastMessageReceived();

    long matchIndex();

    long nextIndex();

    String nodeName();

    Role role();
}
