package io.axoniq.axonserver.enterprise.replication.group;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.internal.DeleteReplicationGroupRequest;
import io.axoniq.axonserver.grpc.internal.NodeReplicationGroup;
import io.axoniq.axonserver.grpc.internal.ReplicationGroup;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupConfiguration;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupEntry;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupMember;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupName;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupUpdateConfirmation;
import io.axoniq.axonserver.test.FakeStreamObserver;
import io.grpc.StatusRuntimeException;
import org.junit.*;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class GrpcRaftGroupServiceTest {

    private GrpcRaftGroupService testSubject;
    private LocalRaftGroupService localRaftGroupService = mock(LocalRaftGroupService.class);

    @Before
    public void setUp() throws Exception {
        when(localRaftGroupService.deleteServer(anyString(), anyString())).thenReturn(CompletableFuture.completedFuture(
                ReplicationGroupUpdateConfirmation.newBuilder().build()));
        when(localRaftGroupService.getStatus(anyString())).thenReturn(ReplicationGroup.newBuilder().build());
        testSubject = new GrpcRaftGroupService(localRaftGroupService);
    }


    @Test
    public void initReplicationGroup() {
        when(localRaftGroupService.initReplicationGroup(anyString(), anyList())).thenReturn(CompletableFuture
                                                                                                    .completedFuture(
                                                                                                            ReplicationGroupConfiguration
                                                                                                                    .newBuilder()
                                                                                                                    .build()));

        FakeStreamObserver<ReplicationGroupConfiguration> streamObserver = new FakeStreamObserver<>();
        testSubject.initReplicationGroup(ReplicationGroup.newBuilder().build(), streamObserver);
        assertEquals(1, streamObserver.values().size());
        assertEquals(1, streamObserver.completedCount());
    }

    @Test
    public void initReplicationGroupException() {
        when(localRaftGroupService.initReplicationGroup(anyString(), anyList()))
                .thenThrow(new RuntimeException("Error"));

        FakeStreamObserver<ReplicationGroupConfiguration> streamObserver = new FakeStreamObserver<>();
        testSubject.initReplicationGroup(ReplicationGroup.newBuilder().build(), streamObserver);
        assertEquals(0, streamObserver.values().size());
        assertEquals(1, streamObserver.errors().size());
        assertEquals(StatusRuntimeException.class, streamObserver.errors().get(0).getClass());
    }

    @Test
    public void initReplicationGroupCompletesExceptionally() {
        CompletableFuture<ReplicationGroupConfiguration> result = new CompletableFuture<>();
        result.completeExceptionally(new RuntimeException("Completes exceptionally"));
        when(localRaftGroupService.initReplicationGroup(anyString(), anyList())).thenReturn(result);

        FakeStreamObserver<ReplicationGroupConfiguration> streamObserver = new FakeStreamObserver<>();
        testSubject.initReplicationGroup(ReplicationGroup.newBuilder().build(), streamObserver);
        assertEquals(0, streamObserver.values().size());
        assertEquals(1, streamObserver.errors().size());
        assertEquals(StatusRuntimeException.class, streamObserver.errors().get(0).getClass());
    }

    @Test
    public void deleteReplicationGroup() {
        when(localRaftGroupService.deleteReplicationGroup(anyString(), anyBoolean())).thenReturn(CompletableFuture
                                                                                                         .completedFuture(
                                                                                                                 null));
        FakeStreamObserver<InstructionAck> streamObserver = new FakeStreamObserver<>();
        testSubject.deleteReplicationGroup(DeleteReplicationGroupRequest.newBuilder().build(), streamObserver);
        assertEquals(1, streamObserver.values().size());
        assertEquals(1, streamObserver.completedCount());
        InstructionAck instructionAck = streamObserver.values().get(0);
        assertTrue(instructionAck.getSuccess());
    }

    @Test
    public void deleteReplicationGroupException() {
        when(localRaftGroupService.deleteReplicationGroup(anyString(), anyBoolean())).thenThrow(new RuntimeException(
                "Error"));
        FakeStreamObserver<InstructionAck> streamObserver = new FakeStreamObserver<>();
        testSubject.deleteReplicationGroup(DeleteReplicationGroupRequest.newBuilder().build(), streamObserver);
        assertEquals(0, streamObserver.values().size());
        assertEquals(1, streamObserver.errors().size());
        assertEquals(StatusRuntimeException.class, streamObserver.errors().get(0).getClass());
    }

    @Test
    public void deleteReplicationGroupCompletesExceptionally() {
        CompletableFuture<Void> result = new CompletableFuture<>();
        result.completeExceptionally(new RuntimeException("Completes exceptionally"));
        when(localRaftGroupService.deleteReplicationGroup(anyString(), anyBoolean())).thenReturn(result);
        FakeStreamObserver<InstructionAck> streamObserver = new FakeStreamObserver<>();
        testSubject.deleteReplicationGroup(DeleteReplicationGroupRequest.newBuilder().build(), streamObserver);
        assertEquals(0, streamObserver.values().size());
        assertEquals(1, streamObserver.errors().size());
        assertEquals(StatusRuntimeException.class, streamObserver.errors().get(0).getClass());
    }

    @Test
    public void addServer() {
        CompletableFuture<ReplicationGroupUpdateConfirmation> result = CompletableFuture.completedFuture(
                ReplicationGroupUpdateConfirmation.newBuilder().build());
        when(localRaftGroupService.addServer(anyString(), any(Node.class))).thenReturn(result);
        FakeStreamObserver<ReplicationGroupUpdateConfirmation> streamObserver = new FakeStreamObserver<>();

        testSubject.addServer(ReplicationGroup.newBuilder()
                                              .setName("name")
                                              .addMembers(ReplicationGroupMember.newBuilder().setNodeId("Node1"))
                                              .build(),
                              streamObserver);
        assertEquals(1, streamObserver.values().size());
        assertEquals(1, streamObserver.completedCount());
    }

    @Test
    public void addServerException() {
        CompletableFuture<ReplicationGroupUpdateConfirmation> result = CompletableFuture.completedFuture(
                ReplicationGroupUpdateConfirmation.newBuilder().build());
        when(localRaftGroupService.addServer(anyString(), any(Node.class))).thenReturn(result);
        FakeStreamObserver<ReplicationGroupUpdateConfirmation> streamObserver = new FakeStreamObserver<>();

        testSubject.addServer(ReplicationGroup.newBuilder()
                                              .setName("name")
                                              .build(),
                              streamObserver);
        assertEquals(0, streamObserver.values().size());
        assertEquals(1, streamObserver.errors().size());
        assertEquals(StatusRuntimeException.class, streamObserver.errors().get(0).getClass());
    }

    @Test
    public void addServerCompletesExceptionally() {
        CompletableFuture<ReplicationGroupUpdateConfirmation> result = new CompletableFuture<>();
        result.completeExceptionally(new RuntimeException("Completed exceptionally"));
        when(localRaftGroupService.addServer(anyString(), any(Node.class))).thenReturn(result);
        FakeStreamObserver<ReplicationGroupUpdateConfirmation> streamObserver = new FakeStreamObserver<>();

        testSubject.addServer(ReplicationGroup.newBuilder()
                                              .setName("name")
                                              .build(),
                              streamObserver);
        assertEquals(0, streamObserver.values().size());
        assertEquals(1, streamObserver.errors().size());
        assertEquals(StatusRuntimeException.class, streamObserver.errors().get(0).getClass());
    }

    @Test
    public void removeServer() {
        CompletableFuture<ReplicationGroupUpdateConfirmation> result = CompletableFuture.completedFuture(
                ReplicationGroupUpdateConfirmation.newBuilder().build());
        when(localRaftGroupService.deleteServer(anyString(), anyString())).thenReturn(result);
        FakeStreamObserver<ReplicationGroupUpdateConfirmation> streamObserver = new FakeStreamObserver<>();

        testSubject.removeServer(ReplicationGroup.newBuilder()
                                                 .setName("name")
                                                 .addMembers(ReplicationGroupMember.newBuilder().setNodeId("Node1"))
                                                 .build(),
                                 streamObserver);
        assertEquals(1, streamObserver.values().size());
        assertEquals(1, streamObserver.completedCount());
    }

    @Test
    public void removeServerException() {
        CompletableFuture<ReplicationGroupUpdateConfirmation> result = CompletableFuture.completedFuture(
                ReplicationGroupUpdateConfirmation.newBuilder().build());
        when(localRaftGroupService.deleteServer(anyString(), anyString())).thenReturn(result);
        FakeStreamObserver<ReplicationGroupUpdateConfirmation> streamObserver = new FakeStreamObserver<>();

        testSubject.removeServer(ReplicationGroup.newBuilder()
                                                 .setName("name")
                                                 .build(),
                                 streamObserver);
        assertEquals(0, streamObserver.values().size());
        assertEquals(1, streamObserver.errors().size());
        assertEquals(StatusRuntimeException.class, streamObserver.errors().get(0).getClass());
    }

    @Test
    public void removeServerCompletesExceptionally() {
        CompletableFuture<ReplicationGroupUpdateConfirmation> result = new CompletableFuture<>();
        result.completeExceptionally(new RuntimeException("Completed exceptionally"));
        when(localRaftGroupService.deleteServer(anyString(), anyString())).thenReturn(result);
        FakeStreamObserver<ReplicationGroupUpdateConfirmation> streamObserver = new FakeStreamObserver<>();

        testSubject.removeServer(ReplicationGroup.newBuilder()
                                                 .setName("name")
                                                 .addMembers(ReplicationGroupMember.newBuilder()
                                                                                   .setNodeId("NodeId")
                                                                                   .build())
                                                 .build(),
                                 streamObserver);
        assertEquals(0, streamObserver.values().size());
        assertEquals(1, streamObserver.errors().size());
        assertEquals(StatusRuntimeException.class, streamObserver.errors().get(0).getClass());
    }

    @Test
    public void getStatus() {
        ReplicationGroup result = ReplicationGroup.newBuilder().setName("Name").build();
        when(localRaftGroupService.getStatus(any(Consumer.class))).then(invocation -> {
            Consumer<ReplicationGroup> consumer = invocation.getArgument(0);
            consumer.accept(result);
            return null;
        });
        FakeStreamObserver<ReplicationGroup> streamObserver = new FakeStreamObserver<>();
        testSubject.getStatus(ReplicationGroup.newBuilder().build(), streamObserver);
        assertEquals(1, streamObserver.values().size());
        assertEquals(1, streamObserver.completedCount());
    }

    @Test
    public void getStatusException() {
        when(localRaftGroupService.getStatus(any(Consumer.class))).thenThrow(new RuntimeException("Exception"));
        FakeStreamObserver<ReplicationGroup> streamObserver = new FakeStreamObserver<>();
        testSubject.getStatus(ReplicationGroup.newBuilder().build(), streamObserver);
        assertEquals(0, streamObserver.values().size());
        assertEquals(1, streamObserver.errors().size());
        assertEquals(StatusRuntimeException.class, streamObserver.errors().get(0).getClass());
    }

    @Test
    public void appendEntry() {
        when(localRaftGroupService.appendEntry(anyString(), anyString(), any(byte[].class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        FakeStreamObserver<InstructionAck> streamObserver = new FakeStreamObserver<>();
        testSubject.appendEntry(ReplicationGroupEntry.newBuilder()
                                                     .setEntryName("name")
                                                     .setReplicationGroupName("replication group name")
                                                     .setEntry(ByteString.copyFromUtf8("data"))
                                                     .build(), streamObserver);
        assertEquals(1, streamObserver.values().size());
        assertEquals(1, streamObserver.completedCount());
        InstructionAck ack = streamObserver.values().get(0);
        assertTrue(ack.getSuccess());
    }

    @Test
    public void appendEntryException() {
        when(localRaftGroupService.appendEntry(anyString(), anyString(), any(byte[].class)))
                .thenThrow(new RuntimeException("Exception"));
        FakeStreamObserver<InstructionAck> streamObserver = new FakeStreamObserver<>();
        testSubject.appendEntry(ReplicationGroupEntry.newBuilder()
                                                     .setEntryName("name")
                                                     .setReplicationGroupName("replication group name")
                                                     .setEntry(ByteString.copyFromUtf8("data"))
                                                     .build(), streamObserver);
        assertEquals(0, streamObserver.values().size());
        assertEquals(1, streamObserver.errors().size());
        assertEquals(StatusRuntimeException.class, streamObserver.errors().get(0).getClass());
    }

    @Test
    public void appendEntryCompletesExceptionally() {
        CompletableFuture<Void> result = new CompletableFuture<>();
        result.completeExceptionally(new RuntimeException("Complete exceptionally"));
        when(localRaftGroupService.appendEntry(anyString(), anyString(), any(byte[].class)))
                .thenReturn(result);
        FakeStreamObserver<InstructionAck> streamObserver = new FakeStreamObserver<>();
        testSubject.appendEntry(ReplicationGroupEntry.newBuilder()
                                                     .setEntryName("name")
                                                     .setReplicationGroupName("replication group name")
                                                     .setEntry(ByteString.copyFromUtf8("data"))
                                                     .build(), streamObserver);
        assertEquals(0, streamObserver.values().size());
        assertEquals(1, streamObserver.errors().size());
        assertEquals(StatusRuntimeException.class, streamObserver.errors().get(0).getClass());
    }

    @Test
    public void configuration() {
        when(localRaftGroupService.configuration(anyString())).thenReturn(CompletableFuture.completedFuture(
                ReplicationGroupConfiguration.newBuilder().build()));
        FakeStreamObserver<ReplicationGroupConfiguration> streamObserver = new FakeStreamObserver<>();
        testSubject.configuration(ReplicationGroupName.newBuilder()
                                                      .setName("Group1")
                                                      .build(), streamObserver);
        assertEquals(1, streamObserver.values().size());
        assertEquals(1, streamObserver.completedCount());
    }

    @Test
    public void configurationException() {
        when(localRaftGroupService.configuration(anyString()))
                .thenThrow(new RuntimeException("Exception"));
        FakeStreamObserver<ReplicationGroupConfiguration> streamObserver = new FakeStreamObserver<>();
        testSubject.configuration(ReplicationGroupName.newBuilder()
                                                      .setName("Group1")
                                                      .build(), streamObserver);
        assertEquals(0, streamObserver.values().size());
        assertEquals(1, streamObserver.errors().size());
        assertEquals(StatusRuntimeException.class, streamObserver.errors().get(0).getClass());
    }

    @Test
    public void configurationCompleteExceptionally() {
        CompletableFuture<ReplicationGroupConfiguration> result = new CompletableFuture<>();
        result.completeExceptionally(new RuntimeException("Completed exceptionally"));
        when(localRaftGroupService.configuration(anyString())).thenReturn(result);
        FakeStreamObserver<ReplicationGroupConfiguration> streamObserver = new FakeStreamObserver<>();
        testSubject.configuration(ReplicationGroupName.newBuilder()
                                                      .setName("Group1")
                                                      .build(), streamObserver);
        assertEquals(0, streamObserver.values().size());
        assertEquals(1, streamObserver.errors().size());
        assertEquals(StatusRuntimeException.class, streamObserver.errors().get(0).getClass());
    }

    @Test
    public void transferLeadership() {
        when(localRaftGroupService.transferLeadership(anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        FakeStreamObserver<InstructionAck> streamObserver = new FakeStreamObserver<>();
        testSubject.transferLeadership(ReplicationGroupName.newBuilder()
                                                           .setName("Group1")
                                                           .build(), streamObserver);
        assertEquals(1, streamObserver.values().size());
        assertEquals(1, streamObserver.completedCount());
        InstructionAck ack = streamObserver.values().get(0);
        assertTrue(ack.getSuccess());
    }

    @Test
    public void transferLeadershipException() {
        when(localRaftGroupService.transferLeadership(anyString()))
                .thenThrow(new RuntimeException("Exception"));
        FakeStreamObserver<InstructionAck> streamObserver = new FakeStreamObserver<>();
        testSubject.transferLeadership(ReplicationGroupName.newBuilder()
                                                           .setName("Group1")
                                                           .build(), streamObserver);
        assertEquals(0, streamObserver.values().size());
        assertEquals(1, streamObserver.errors().size());
        assertEquals(StatusRuntimeException.class, streamObserver.errors().get(0).getClass());
    }

    @Test
    public void transferLeadershipCompletesExceptionally() {
        CompletableFuture<Void> result = new CompletableFuture<>();
        result.completeExceptionally(new RuntimeException("Completed exceptionally"));
        when(localRaftGroupService.transferLeadership(anyString()))
                .thenReturn(result);
        FakeStreamObserver<InstructionAck> streamObserver = new FakeStreamObserver<>();
        testSubject.transferLeadership(ReplicationGroupName.newBuilder()
                                                           .setName("Group1")
                                                           .build(), streamObserver);
        assertEquals(0, streamObserver.values().size());
        assertEquals(1, streamObserver.errors().size());
        assertEquals(StatusRuntimeException.class, streamObserver.errors().get(0).getClass());
    }

    @Test
    public void preDeleteNodeFromReplicationGroup() {
        when(localRaftGroupService.prepareDeleteNodeFromReplicationGroup(anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        FakeStreamObserver<InstructionAck> streamObserver = new FakeStreamObserver<>();
        testSubject.preDeleteNodeFromReplicationGroup(NodeReplicationGroup.newBuilder()
                                                                          .setNodeName("node")
                                                                          .setReplicationGroupName("Group1")
                                                                          .build(), streamObserver);
        assertEquals(1, streamObserver.values().size());
        assertEquals(1, streamObserver.completedCount());
        InstructionAck ack = streamObserver.values().get(0);
        assertTrue(ack.getSuccess());
    }

    @Test
    public void preDeleteNodeFromReplicationGroupException() {
        when(localRaftGroupService.prepareDeleteNodeFromReplicationGroup(anyString(), anyString()))
                .thenThrow(new MessagingPlatformException(ErrorCode.CANNOT_DELETE_INTERNAL_CONTEXT, "Exception"));
        FakeStreamObserver<InstructionAck> streamObserver = new FakeStreamObserver<>();
        testSubject.preDeleteNodeFromReplicationGroup(NodeReplicationGroup.newBuilder()
                                                                          .setNodeName("node")
                                                                          .setReplicationGroupName("Group1")
                                                                          .build(), streamObserver);
        assertEquals(0, streamObserver.values().size());
        assertEquals(1, streamObserver.errors().size());
        assertEquals(StatusRuntimeException.class, streamObserver.errors().get(0).getClass());
    }

    @Test
    public void preDeleteNodeFromReplicationGroupCompletesExceptionally() {
        CompletableFuture<Void> result = new CompletableFuture<>();
        result.completeExceptionally(new RuntimeException("Completed exceptionally"));
        when(localRaftGroupService.prepareDeleteNodeFromReplicationGroup(anyString(), anyString()))
                .thenReturn(result);
        FakeStreamObserver<InstructionAck> streamObserver = new FakeStreamObserver<>();
        testSubject.preDeleteNodeFromReplicationGroup(NodeReplicationGroup.newBuilder()
                                                                          .setNodeName("node")
                                                                          .setReplicationGroupName("Group1")
                                                                          .build(), streamObserver);
        assertEquals(0, streamObserver.values().size());
        assertEquals(1, streamObserver.errors().size());
        assertEquals(StatusRuntimeException.class, streamObserver.errors().get(0).getClass());
    }

    @Test
    public void requiresContextInterceptor() {
        assertFalse(testSubject.requiresContextInterceptor());
    }
}