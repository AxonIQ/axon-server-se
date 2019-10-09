package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.util.RoleUtils;
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
    public boolean primaryNode() {
        return RoleUtils.primaryNode(role);
    }

    @Override
    public boolean votingNode() {
        return RoleUtils.votingNode(role);
    }

    @Override
    public long matchIndex() {
        return matchIndex;
    }

    public void setMatchIndex(long matchIndex) {
        this.matchIndex = matchIndex;
    }
}
