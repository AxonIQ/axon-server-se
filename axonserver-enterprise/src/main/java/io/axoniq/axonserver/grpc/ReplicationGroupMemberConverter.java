package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupMember;

/**
 * Converter between {@link ReplicationGroupMember} and {@link Node}.
 *
 * @author Marc Gathier
 * @since 4.3
 */
public class ReplicationGroupMemberConverter {

    private ReplicationGroupMemberConverter() {

    }

    public static ReplicationGroupMember asContextMember(Node r) {
        return ReplicationGroupMember.newBuilder()
                                     .setHost(r.getHost())
                                     .setPort(r.getPort())
                                     .setNodeId(r.getNodeId())
                                     .setNodeName(r.getNodeName())
                                     .setRole(r.getRole())
                                     .build();
    }

    public static Node asNode(ReplicationGroupMember member) {
        return Node.newBuilder()
                   .setHost(member.getHost())
                   .setPort(member.getPort())
                   .setNodeId(member.getNodeId())
                   .setNodeName(member.getNodeName())
                   .setRole(member.getRole())
                   .build();
    }
}