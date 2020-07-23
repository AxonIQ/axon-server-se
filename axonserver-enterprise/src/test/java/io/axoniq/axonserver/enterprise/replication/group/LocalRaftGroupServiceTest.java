package io.axoniq.axonserver.enterprise.replication.group;

import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.enterprise.replication.GrpcRaftController;
import io.axoniq.axonserver.grpc.cluster.ConfigChangeFailure;
import io.axoniq.axonserver.grpc.cluster.ConfigChangeResult;
import io.axoniq.axonserver.grpc.cluster.ConfigChangeSuccess;
import io.axoniq.axonserver.grpc.cluster.ErrorMessage;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.internal.ContextApplication;
import io.axoniq.axonserver.grpc.internal.ReplicationGroup;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupConfiguration;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupUpdateConfirmation;
import org.junit.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class LocalRaftGroupServiceTest {

    private LocalRaftGroupService testSubject;
    private GrpcRaftController grpcRaftController;
    private RaftNode raftNode = mock(RaftNode.class);
    private Map<String, Node> members = new HashMap<>();

    @Before
    public void setup() {
        when(raftNode.currentGroupMembers()).then(invocation -> new ArrayList<>(members.values()));
        when(raftNode.addNode(any(Node.class))).then(invocation -> {
            Node node = invocation.getArgument(0);
            members.put(node.getNodeName(), node);
            ConfigChangeResult configChangeResult = ConfigChangeResult.newBuilder()
                                                                      .setSuccess(ConfigChangeSuccess.newBuilder()
                                                                                                     .build())
                                                                      .build();
            return CompletableFuture.completedFuture(configChangeResult);
        });
        when(raftNode.appendEntry(anyString(), any(byte[].class))).thenReturn(CompletableFuture.completedFuture(null));
        when(raftNode.groupId()).thenReturn("theGroupId");
        when(raftNode.removeNode(anyString())).then(invocation -> {
            members.remove(invocation.getArgument(0));
            ConfigChangeResult configChangeResult = ConfigChangeResult.newBuilder()
                                                                      .setSuccess(ConfigChangeSuccess.newBuilder()
                                                                                                     .build())
                                                                      .build();
            return CompletableFuture.completedFuture(configChangeResult);
        });
        when(raftNode.removeGroup()).thenReturn(CompletableFuture.completedFuture(null));
        when(raftNode.transferLeadership()).thenReturn(CompletableFuture.completedFuture(null));

        RaftGroup raftGroup = mock(RaftGroup.class);
        when(raftGroup.localNode()).thenReturn(raftNode);
        grpcRaftController = mock(GrpcRaftController.class);
        when(grpcRaftController.getRaftGroup(anyString())).thenReturn(raftGroup);
        when(grpcRaftController.getRaftNode(anyString())).thenReturn(raftNode);
        when(grpcRaftController.waitForLeader(any())).thenReturn(raftNode);
        when(grpcRaftController.initRaftGroup(anyString(), anyString(), anyString())).thenReturn(raftGroup);
        testSubject = new LocalRaftGroupService(grpcRaftController);
    }

    @Test
    public void stepDown() {
        testSubject.stepDown("demo");
    }

    @Test
    public void appendEntry() throws InterruptedException, ExecutionException, TimeoutException {
        testSubject.appendEntry("demo", ContextApplication.newBuilder().setName("demoApplication").build())
                   .get(1, TimeUnit.SECONDS);
    }

    @Test
    public void addServer() throws InterruptedException, ExecutionException, TimeoutException {
        ReplicationGroupUpdateConfirmation result = testSubject.addServer("demo",
                                                                          Node.newBuilder()
                                                                              .setNodeName(
                                                                                      "newNode")
                                                                              .build())
                                                               .get(1,
                                                                    TimeUnit.SECONDS);
        assertEquals(1, result.getMembersCount());
    }

    @Test
    public void getStatus() {
        members.put("node1", Node.newBuilder().setNodeId("node1id").build());
        ReplicationGroup status = testSubject.getStatus("demo");
        assertEquals(1, status.getMembersCount());
    }

    @Test
    public void configuration() throws InterruptedException, ExecutionException, TimeoutException {
        members.put("node1", Node.newBuilder().setNodeId("node1id").build());
        ReplicationGroupConfiguration configuration = testSubject.configuration(
                "demo").get(1, TimeUnit.SECONDS);
        assertEquals(1, configuration.getNodesCount());
    }

    @Test
    public void deleteServer() throws InterruptedException, ExecutionException, TimeoutException {
        testSubject.deleteServer("demo", "node1").get(1, TimeUnit.SECONDS);
    }

    @Test
    public void deleteServerException() throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<ConfigChangeResult> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("Failed in unit test"));
        when(raftNode.removeNode(anyString())).thenReturn(failed);
        ReplicationGroupUpdateConfirmation result = testSubject.deleteServer("demo", "node1").get(1,
                                                                                                  TimeUnit.SECONDS);
        assertFalse(result.getSuccess());
        assertEquals("Failed in unit test", result.getMessage());
    }

    @Test
    public void deleteServerFail() throws InterruptedException, ExecutionException, TimeoutException {
        ConfigChangeResult failed = ConfigChangeResult.newBuilder()
                                                      .setFailure(ConfigChangeFailure.newBuilder()
                                                                                     .setError(ErrorMessage.newBuilder()
                                                                                                           .setCode(
                                                                                                                   "AXONIQ-1111")
                                                                                                           .setMessage(
                                                                                                                   "Configuration change in progress")
                                                                                                           .build())
                                                                                     .build())
                                                      .build();
        when(raftNode.removeNode(anyString())).thenReturn(CompletableFuture.completedFuture(failed));
        ReplicationGroupUpdateConfirmation result = testSubject.deleteServer("demo", "node1").get(1,
                                                                                                  TimeUnit.SECONDS);
        assertFalse(result.getSuccess());
        assertEquals("Configuration change in progress", result.getMessage());
    }


    @Test
    public void initReplicationGroup() throws InterruptedException, ExecutionException, TimeoutException {
        testSubject.initReplicationGroup("demo1",
                                         Arrays.asList(
                                                 Node.newBuilder().setNodeId("node1").build(),
                                                 Node.newBuilder().setNodeId("node2").build()))
                   .get(1, TimeUnit.SECONDS);
    }

    @Test
    public void deleteReplicationGroup() throws InterruptedException, ExecutionException, TimeoutException {
        testSubject.deleteReplicationGroup("demo", true).get(1, TimeUnit.SECONDS);
    }

    @Test
    public void transferLeadership() throws InterruptedException, ExecutionException, TimeoutException {
        testSubject.transferLeadership("demo").get(1, TimeUnit.SECONDS);
    }

    @Test
    public void prepareDeleteNodeFromReplicationGroup()
            throws InterruptedException, ExecutionException, TimeoutException {
        testSubject.prepareDeleteNodeFromReplicationGroup("demo", "node1").get(1, TimeUnit.SECONDS);
    }
}