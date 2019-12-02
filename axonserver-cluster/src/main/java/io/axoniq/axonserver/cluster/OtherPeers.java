package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.Node;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Iterable of all {@link RaftPeer}s that are part of the current configuration of the raft group,
 * excluded the current node.
 *
 * @author Sara Pellegrini
 * @since 4.1
 */
public class OtherPeers implements Iterable<RaftPeer> {

    private final Supplier<String> nodeId;
    private final CurrentConfiguration configuration;
    private final Function<Node, RaftPeer> peerMapping;
    private final Predicate<Node> filter;

    public OtherPeers(RaftGroup raftGroup, CurrentConfiguration configuration) {
        this(() -> raftGroup.localNode().nodeId(), configuration, raftGroup::peer, n -> true);
    }

    public OtherPeers(RaftGroup raftGroup, CurrentConfiguration configuration, Predicate<Node> filter) {
        this(() -> raftGroup.localNode().nodeId(), configuration, raftGroup::peer, filter);
    }

    public OtherPeers(Supplier<String> nodeId,
                      CurrentConfiguration configuration,
                      Function<Node, RaftPeer> peerMapping, Predicate<Node> filter) {
        this.nodeId = nodeId;
        this.configuration = configuration;
        this.peerMapping = peerMapping;
        this.filter = filter;
    }

    @NotNull
    @Override
    public Iterator<RaftPeer> iterator() {
        return configuration.groupMembers().stream()
                            .filter(node -> !node.getNodeId().equals(nodeId.get()))
                            .filter(filter)
                            .map(peerMapping).iterator();
    }
}
