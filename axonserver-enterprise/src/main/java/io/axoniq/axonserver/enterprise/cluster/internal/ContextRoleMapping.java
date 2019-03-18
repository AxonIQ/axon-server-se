package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.enterprise.jpa.ContextClusterNode;
import io.axoniq.axonserver.grpc.internal.ContextRole;

import java.util.function.Function;

/**
 * Mapping function to create a {@link ContextRole} from a {@link ContextClusterNode}
 *
 * @author Sara Pellegrini
 * @since 4.1
 */
public class ContextRoleMapping implements Function<ContextClusterNode, ContextRole> {

    @Override
    public ContextRole apply(ContextClusterNode contextClusterNode) {
        return ContextRole.newBuilder()
                   .setName(contextClusterNode.getClusterNode().getName())
                   .setNodeLabel(contextClusterNode.getClusterNodeLabel())
                   .setMessaging(contextClusterNode.isMessaging())
                   .setStorage(contextClusterNode.isStorage())
                   .build();
    }
}
