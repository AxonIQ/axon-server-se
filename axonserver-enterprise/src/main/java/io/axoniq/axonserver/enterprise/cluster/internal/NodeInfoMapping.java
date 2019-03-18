package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.jpa.ContextClusterNode;
import io.axoniq.axonserver.grpc.internal.ContextRole;
import io.axoniq.axonserver.grpc.internal.NodeInfo;

import java.util.function.Function;

/**
 * Mapping function to obtain a {@link NodeInfo} from {@link ClusterNode}
 *
 * @author Sara Pellegrini
 * @since 4.1
 */
public class NodeInfoMapping implements Function<ClusterNode, NodeInfo> {

    private final Function<ContextClusterNode, ContextRole> contextRoleMapping;

    public NodeInfoMapping() {
        this(new ContextRoleMapping());
    }

    NodeInfoMapping(
            Function<ContextClusterNode, ContextRole> contextRoleMapping) {
        this.contextRoleMapping = contextRoleMapping;
    }

    @Override
    public NodeInfo apply(ClusterNode n) {
        return NodeInfo.newBuilder()
                       .setNodeName(n.getName())
                       .setHostName(n.getHostName())
                       .setInternalHostName(n.getInternalHostName())
                       .setGrpcPort(n.getGrpcPort())
                       .setGrpcInternalPort(n.getGrpcInternalPort())
                       .setHttpPort(n.getHttpPort())
                       .addAllContexts(() -> n.getContexts().stream().map(contextRoleMapping).iterator())
                       .build();
    }
}
