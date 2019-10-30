package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.Role;

/**
 * @author Marc Gathier
 */
public class FakeReplicatorPeer implements ReplicatorPeerStatus {

    private final long lastMessageReceived;
    private final Role role;
    private long matchIndex;

    public FakeReplicatorPeer(long lastMessageReceived, Role role) {
        this.lastMessageReceived = lastMessageReceived;
        this.role = role;
    }

    @Override
    public long lastMessageReceived() {
        return lastMessageReceived;
    }

    @Override
    public Role role() {
        return role;
    }

    @Override
    public long matchIndex() {
        return matchIndex;
    }

    public void setMatchIndex(long matchIndex) {
        this.matchIndex = matchIndex;
    }

    @Override
    public long nextIndex() {
        return 0;
    }

    @Override
    public String nodeName() {
        return "Name";
    }
}
