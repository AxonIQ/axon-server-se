package io.axoniq.axonserver.cluster.configuration;

import io.axoniq.axonserver.cluster.exception.ServerTooSlowException;
import io.axoniq.axonserver.grpc.cluster.Node;
import org.junit.*;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.junit.Assert.*;

/**
 * @author Sara Pellegrini
 */
public class RoundMonitorTest {

    private RoundUpdateAlgorithm roundMonitor;

    @Test
    public void testSuccess() throws ExecutionException, InterruptedException {
        AtomicLong lastIndex = new AtomicLong();
        AtomicLong currentTime = new AtomicLong();
        roundMonitor = new RoundUpdateAlgorithm(3, 300, lastIndex::get, currentTime::get,
                                                (node, target) -> {
                                                    lastIndex.addAndGet(10L);
                                                    currentTime.addAndGet(100L);
                                                    return CompletableFuture.completedFuture(null);
                                                });
        Node node = Node.newBuilder().build();
        CompletableFuture<Void> serverUpdated = roundMonitor.apply(node);
        serverUpdated.get();
        assertTrue(serverUpdated.isDone());
    }

    @Test
    public void testFailure(){

        AtomicLong lastIndex = new AtomicLong();
        AtomicLong currentTime = new AtomicLong();
        roundMonitor = new RoundUpdateAlgorithm(3, 300, lastIndex::get, currentTime::get,
                                                (node, target) -> {
                                                    lastIndex.addAndGet(10L);
                                                    currentTime.addAndGet(1000L);
                                                    return CompletableFuture.completedFuture(null);
                                                });
        Node node = Node.newBuilder().build();
        CompletableFuture<Void> serverUpdated = roundMonitor.apply(node);
        try {
            serverUpdated.get();
        } catch (Exception e){
            assertTrue(serverUpdated.isCompletedExceptionally());
            assertTrue(e.getCause() instanceof ServerTooSlowException);
        }

    }
}