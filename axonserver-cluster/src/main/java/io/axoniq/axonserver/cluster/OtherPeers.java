package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.Node;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author Sara Pellegrini
 * @since 4.1
 */
public class OtherPeers implements Iterable<RaftPeer> {

    private final Supplier<String> nodeId;
    private final CurrentConfiguration configuration;
    private final Function<String, RaftPeer> peerMapping;

    public OtherPeers(RaftGroup raftGroup, CurrentConfiguration configuration) {
        this(() -> raftGroup.localNode().nodeId(), configuration, raftGroup::peer);
    }

    public OtherPeers(Supplier<String> nodeId,
                      CurrentConfiguration configuration,
                      Function<String, RaftPeer> peerMapping) {
        this.nodeId = nodeId;
        this.configuration = configuration;
        this.peerMapping = peerMapping;
    }

    @NotNull
    @Override
    public Iterator<RaftPeer> iterator() {
        return configuration.groupMembers().stream()
                            .map(Node::getNodeId).filter(id -> !id.equals(nodeId.get()))
                            .map(peerMapping).iterator();
    }
}
