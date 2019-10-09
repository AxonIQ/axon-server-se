package io.axoniq.axonserver.cluster;

/**
 * @author Marc Gathier
 */
public interface ReplicatorPeerStatus {

    boolean votingNode();

    long lastMessageReceived();

    boolean primaryNode();

    long matchIndex();
}
