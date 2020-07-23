package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.enterprise.jpa.AdminReplicationGroupMember;
import io.axoniq.axonserver.grpc.internal.ContextRole;

import java.util.function.Function;

/**
 * Mapping function to create a {@link ContextRole} from a {@link AdminReplicationGroupMember}
 *
 * @author Sara Pellegrini
 * @since 4.1
 */
public class ContextRoleMapping implements Function<AdminReplicationGroupMember, ContextRole> {

    @Override
    public ContextRole apply(AdminReplicationGroupMember contextClusterNode) {
        return ContextRole.newBuilder()
                          .setName(contextClusterNode.getReplicationGroup().getName())
                          .setNodeLabel(contextClusterNode.getClusterNodeLabel())
                          .build();
    }
}
