package io.axoniq.axonserver.enterprise.replication;

import io.axoniq.axonserver.enterprise.jpa.AdminContext;
import io.axoniq.axonserver.enterprise.jpa.AdminReplicationGroup;
import io.axoniq.axonserver.enterprise.jpa.AdminReplicationGroupMember;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.grpc.internal.NodeInfoWithLabel;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupConfiguration;

import java.util.function.Function;

/**
 * Mapping function to obtain a {@link ReplicationGroupConfiguration} from a {@link AdminContext}
 *
 * @author Sara Pellegrini
 * @since 4.1
 */
public class ReplicationGroupConfigurationMapping
        implements Function<AdminReplicationGroup, ReplicationGroupConfiguration> {

    private final Function<ClusterNode, NodeInfo> clusterNodeMapping;

    public ReplicationGroupConfigurationMapping() {
        this(new NodeInfoMapping());
    }

    public ReplicationGroupConfigurationMapping(
            Function<ClusterNode, NodeInfo> clusterNodeMapping) {
        this.clusterNodeMapping = clusterNodeMapping;
    }

    @Override
    public ReplicationGroupConfiguration apply(AdminReplicationGroup context) {
        ReplicationGroupConfiguration.Builder builder = ReplicationGroupConfiguration.newBuilder()
                                                                                     .setPending(context.isChangePending())
                                                                                     .setReplicationGroupName(context.getName());

        for (AdminReplicationGroupMember node : context.getMembers()) {
            builder.addNodes(NodeInfoWithLabel.newBuilder()
                                              .setLabel(node.getClusterNodeLabel())
                                              .setNode(clusterNodeMapping.apply(node.getClusterNode())))
                   .build();
        }
        return builder.build();
    }

}
