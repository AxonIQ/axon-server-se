package io.axoniq.axonserver.enterprise.replication;

import io.axoniq.axonserver.enterprise.jpa.AdminReplicationGroup;
import io.axoniq.axonserver.enterprise.jpa.AdminReplicationGroupMember;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.grpc.internal.ContextConfiguration;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.grpc.internal.NodeInfoWithLabel;

import java.util.function.Function;

/**
 * Mapping function to obtain a {@link ContextConfiguration} from a {@link AdminReplicationGroup}
 *
 * @author Sara Pellegrini
 * @since 4.1
 */
public class ContextConfigurationMapping implements Function<AdminReplicationGroup, ContextConfiguration> {

    private final Function<ClusterNode, NodeInfo> clusterNodeMapping;

    public ContextConfigurationMapping() {
        this(new NodeInfoMapping());
    }

    public ContextConfigurationMapping(
            Function<ClusterNode, NodeInfo> clusterNodeMapping) {
        this.clusterNodeMapping = clusterNodeMapping;
    }

    @Override
    public ContextConfiguration apply(AdminReplicationGroup context) {
        ContextConfiguration.Builder builder = ContextConfiguration.newBuilder()
                                                                   .setPending(context.isChangePending())
                                                                   .setContext(context.getName());

        for (AdminReplicationGroupMember node : context.getMembers()) {
            builder.addNodes(NodeInfoWithLabel.newBuilder()
                                              .setLabel(node.getClusterNodeLabel())
                                              .setNode(clusterNodeMapping.apply(node.getClusterNode())))
                   .build();
        }
        return builder.build();
    }
}
