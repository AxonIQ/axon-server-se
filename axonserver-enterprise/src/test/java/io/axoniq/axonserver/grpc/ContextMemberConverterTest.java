package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.internal.ContextMember;
import org.junit.*;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class ContextMemberConverterTest {

    @Test
    public void asContextMember() {
        Node node = Node.newBuilder()
                        .setNodeId("nodeid")
                        .setNodeName("nodename")
                        .setHost("host")
                        .setPort(123)
                        .setRole(Role.PASSIVE_BACKUP)
                        .build();
        ContextMember contextMember = ContextMemberConverter.asContextMember(node);
        assertEquals(node.getNodeId(), contextMember.getNodeId());
        assertEquals(node.getNodeName(), contextMember.getNodeName());
        assertEquals(node.getHost(), contextMember.getHost());
        assertEquals(node.getPort(), contextMember.getPort());
        assertEquals(node.getRole(), contextMember.getRole());
    }

    @Test
    public void asNode() {
        ContextMember contextMember = ContextMember.newBuilder()
                                                   .setNodeId("nodeid")
                                                   .setNodeName("nodename")
                                                   .setHost("host")
                                                   .setPort(123)
                                                   .setRole(Role.PASSIVE_BACKUP)
                                                   .build();
        Node node = ContextMemberConverter.asNode(contextMember);
        assertEquals(contextMember.getNodeId(), node.getNodeId());
        assertEquals(contextMember.getNodeName(), node.getNodeName());
        assertEquals(contextMember.getHost(), node.getHost());
        assertEquals(contextMember.getPort(), node.getPort());
        assertEquals(contextMember.getRole(), node.getRole());
    }
}