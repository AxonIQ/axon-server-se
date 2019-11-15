package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.internal.ContextMember;

/**
 * Converter between {@link ContextMember} and {@link Node}.
 *
 * @author Marc Gathier
 * @since 4.3
 */
public class ContextMemberConverter {

    private ContextMemberConverter() {

    }

    public static ContextMember asContextMember(Node r) {
        return ContextMember.newBuilder()
                            .setHost(r.getHost())
                            .setPort(r.getPort())
                            .setNodeId(r.getNodeId())
                            .setNodeName(r.getNodeName())
                            .setRole(r.getRole())
                            .build();
    }

    public static Node asNode(ContextMember member) {
        return Node.newBuilder()
                   .setHost(member.getHost())
                   .setPort(member.getPort())
                   .setNodeId(member.getNodeId())
                   .setNodeName(member.getNodeName())
                   .setRole(member.getRole())
                   .build();
    }
}