package io.axoniq.axonserver.cluster.configuration;

import io.axoniq.axonserver.cluster.exception.RaftErrorMapping;
import io.axoniq.axonserver.cluster.exception.ServerTooSlowException;
import io.axoniq.axonserver.cluster.exception.UncommittedConfigException;
import io.axoniq.axonserver.grpc.cluster.ConfigChangeResult;
import io.axoniq.axonserver.grpc.cluster.ErrorMessage;
import io.axoniq.axonserver.grpc.cluster.Node;
import org.junit.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class LeaderConfigurationTest {

    @Test
    public void testAddFastNode() throws ExecutionException, InterruptedException {
        LeaderConfiguration leader = new LeaderConfiguration(operation -> completedFuture(null),
                                                             node -> completedFuture(null),
                                                             new RaftErrorMapping());
        Node node = Node.newBuilder().setNodeId("MyNode").setHost("localhost").setPort(1234).build();
        CompletableFuture<ConfigChangeResult> result = leader.addServer(node);
        ConfigChangeResult configChangeResult = result.get();
        assertTrue(configChangeResult.hasSuccess());
    }

    @Test
    public void testAddSlowServer() throws ExecutionException, InterruptedException {
        LeaderConfiguration leader = new LeaderConfiguration(operation -> completedFuture(null),
                                                             node -> failedFuture(new ServerTooSlowException("My message")),
                                                             new RaftErrorMapping());
        Node node = Node.newBuilder()
                        .setNodeId("MyNode")
                        .setHost("localhost")
                        .setPort(1234).build();
        CompletableFuture<ConfigChangeResult> result = leader.addServer(node);
        ConfigChangeResult configChangeResult = result.get();
        assertTrue(configChangeResult.hasFailure());
        ErrorMessage error = configChangeResult.getFailure().getError();
        assertEquals("AXONIQ-10001", error.getCode());
        assertEquals("My message", error.getMessage());
    }


    @Test
    public void testAddServerCommitError() throws ExecutionException, InterruptedException {
        LeaderConfiguration leader = new LeaderConfiguration(operation -> failedFuture(new UncommittedConfigException("My message")),
                                                             node ->  completedFuture(null),
                                                             new RaftErrorMapping());
        Node node = Node.newBuilder()
                        .setNodeId("MyNode")
                        .setHost("localhost")
                        .setPort(1234).build();
        CompletableFuture<ConfigChangeResult> result = leader.addServer(node);
        ConfigChangeResult configChangeResult = result.get();
        assertTrue(configChangeResult.hasFailure());
        ErrorMessage error = configChangeResult.getFailure().getError();
        assertEquals("AXONIQ-10002", error.getCode());
        assertEquals("My message", error.getMessage());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddInvalidNodeId() throws ExecutionException, InterruptedException {
        LeaderConfiguration leader = new LeaderConfiguration(operation -> completedFuture(null),
                                                             node -> failedFuture(new ServerTooSlowException("My message")),
                                                             new RaftErrorMapping());
        Node node = Node.newBuilder()
                        .setHost("localhost")
                        .setPort(1234).build();
        CompletableFuture<ConfigChangeResult> result = leader.addServer(node);
        result.get();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddInvalidHost() throws ExecutionException, InterruptedException {
        LeaderConfiguration leader = new LeaderConfiguration(operation -> completedFuture(null),
                                                             node -> failedFuture(new ServerTooSlowException("My message")),
                                                             new RaftErrorMapping());
        Node node = Node.newBuilder()
                        .setNodeId("myNode")
                        .setPort(1234).build();
        CompletableFuture<ConfigChangeResult> result = leader.addServer(node);
        result.get();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddInvalidPort() throws ExecutionException, InterruptedException {
        LeaderConfiguration leader = new LeaderConfiguration(operation -> completedFuture(null),
                                                             node -> failedFuture(new ServerTooSlowException("My message")),
                                                             new RaftErrorMapping());
        Node node = Node.newBuilder()
                        .setNodeId("myNode")
                        .setHost("localhost")
                        .build();
        CompletableFuture<ConfigChangeResult> result = leader.addServer(node);
        result.get();
    }

    @Test
    public void testRemoveNode() throws ExecutionException, InterruptedException {
        LeaderConfiguration leader = new LeaderConfiguration(operation -> completedFuture(null),
                                                             node -> completedFuture(null),
                                                             new RaftErrorMapping());
        CompletableFuture<ConfigChangeResult> result = leader.removeServer("myNode");
        ConfigChangeResult configChangeResult = result.get();
        assertTrue(configChangeResult.hasSuccess());
    }

    @Test
    public void testRemoveCommitError() throws ExecutionException, InterruptedException {
        LeaderConfiguration leader = new LeaderConfiguration(operation -> failedFuture(new UncommittedConfigException("My message")),
                                                             node ->  completedFuture(null),
                                                             new RaftErrorMapping());
        CompletableFuture<ConfigChangeResult> result = leader.removeServer("myNode");
        ConfigChangeResult configChangeResult = result.get();
        assertTrue(configChangeResult.hasFailure());
        ErrorMessage error = configChangeResult.getFailure().getError();
        assertEquals("AXONIQ-10002", error.getCode());
        assertEquals("My message", error.getMessage());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRemoveNullId() {
        LeaderConfiguration leader = new LeaderConfiguration(operation -> completedFuture(null),
                                                             node -> completedFuture(null),
                                                             new RaftErrorMapping());
        CompletableFuture<ConfigChangeResult> result = leader.removeServer(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRemoveEmptyId() {
        LeaderConfiguration leader = new LeaderConfiguration(operation -> completedFuture(null),
                                                             node -> completedFuture(null),
                                                             new RaftErrorMapping());
        CompletableFuture<ConfigChangeResult> result = leader.removeServer("");
    }

    private CompletableFuture<Void> failedFuture(Throwable error) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        result.completeExceptionally(error);
        return result;
    }
}