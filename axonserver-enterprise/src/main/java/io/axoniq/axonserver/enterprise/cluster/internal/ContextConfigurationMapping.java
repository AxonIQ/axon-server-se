package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.jpa.Context;
import io.axoniq.axonserver.enterprise.jpa.ContextClusterNode;
import io.axoniq.axonserver.grpc.internal.ContextConfiguration;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.grpc.internal.NodeInfoWithLabel;

import java.util.function.Function;

/**
 * Mapping function to obtain a {@link ContextConfiguration} from a {@link Context}
 *
 * @author Sara Pellegrini
 * @since 4.1
 */
public class ContextConfigurationMapping implements Function<Context, ContextConfiguration> {

    private final Function<ClusterNode, NodeInfo> clusterNodeMapping;

    public ContextConfigurationMapping() {
        this(new NodeInfoMapping());
    }

    public ContextConfigurationMapping(
            Function<ClusterNode, NodeInfo> clusterNodeMapping) {
        this.clusterNodeMapping = clusterNodeMapping;
    }

    @Override
    public ContextConfiguration apply(Context context) {
        ContextConfiguration.Builder builder = ContextConfiguration.newBuilder()
                                                                   .setPending(context.isChangePending())
                                                                   .setContext(context.getName());
        for (ContextClusterNode node : context.getNodes()) {
            builder.addNodes(NodeInfoWithLabel.newBuilder()
                                              .setLabel(node.getClusterNodeLabel())
                                              .setNode(clusterNodeMapping.apply(node.getClusterNode())))
                                              .build();
        }
        return builder.build();
    }

}
