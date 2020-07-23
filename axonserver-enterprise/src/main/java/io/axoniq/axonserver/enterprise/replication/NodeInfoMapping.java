package io.axoniq.axonserver.enterprise.replication;

import io.axoniq.axonserver.enterprise.cluster.internal.ContextRoleMapping;
import io.axoniq.axonserver.enterprise.jpa.AdminReplicationGroupMember;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
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

    private final Function<AdminReplicationGroupMember, ContextRole> contextRoleMapping;

    public NodeInfoMapping() {
        this(new ContextRoleMapping());
    }

    NodeInfoMapping(
            Function<AdminReplicationGroupMember, ContextRole> contextRoleMapping) {
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
                       .addAllContexts(() -> n.getReplicationGroups().stream().map(contextRoleMapping).iterator())
                       .build();
    }
}
