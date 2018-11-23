package io.axoniq.axonserver.cluster;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static io.axoniq.axonserver.cluster.TestUtils.assertWithin;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class RaftClusterIntegrationTest {

    private final int clusterSize;
    private RaftClusterTestFixture fixture;

    public RaftClusterIntegrationTest(int clusterSize) {
        this.clusterSize = clusterSize;
    }

    @Parameterized.Parameters(name = "{0} nodes")
    public static Collection<?> data() {
        return Arrays.asList(3, 5, 7, 9);
    }

    @Before
    public void setUp() {
        String[] hostNames = new String[clusterSize];
        for (int i = 0; i < clusterSize; i++) {
            hostNames[i] = "node" + (i+1);
        }
        fixture = new RaftClusterTestFixture(hostNames);
    }

    @After
    public void tearDown() {
        fixture.shutdown();
    }

    @Test
    public void testClusterStart() {
        fixture.nodes().forEach(n -> n.appendEntry("mock", "Mock".getBytes()));
        fixture.startNodes();
    }

    @Test
    public void testClusterElectsLeader() throws InterruptedException {
        fixture.startNodes();

        assertWithin(5, SECONDS, () -> {
            long leaderCount = fixture.leaders().size();
            assertEquals(1, leaderCount);
        });
    }

    @Test
    public void testFollowersRejectCommands() throws Exception {
        fixture.startNodes();

        assertWithin(5, SECONDS, () -> {
            long leaderCount = fixture.leaders().size();
            assertEquals(1, leaderCount);
        });

        RaftNode follower = fixture.nodes().stream().filter(n -> !n.isLeader()).findFirst().orElseThrow(() -> new AssertionError("Expected at least one follower node"));
        CompletableFuture<Void> result = follower.appendEntry("mock", "Mock".getBytes());
        assertTrue(result.thenApply(r -> false).exceptionally(e -> true).get());
    }

    @Test
    public void testClusterRestartsElectionAfterNetworkPartition() throws InterruptedException {
        fixture.startNodes();

        assertWithin(5, SECONDS, () -> {
            long leaderCount = fixture.leaders().size();
            assertEquals(1, leaderCount);
        });

        String firstLeader = fixture.leaders().stream().findFirst().orElseThrow(() -> new AssertionError("Expected at least one leader"));
        fixture.createNetworkPartition(firstLeader);

        assertWithin(5, SECONDS, () -> {
            long leaderCount = fixture.leaders().size();
            assertEquals(1, leaderCount);
        });
    }

    @Test
    public void testLeaderStepsDownAfterReconnection() throws InterruptedException {
        fixture.startNodes();

        assertWithin(5, SECONDS, () -> {
            long leaderCount = fixture.leaders().size();
            assertEquals(1, leaderCount);
        });

        String firstLeader = fixture.leaders().stream().findFirst().orElseThrow(() -> new AssertionError("Expected at least one leader. Did it step down?"));
        fixture.createNetworkPartition(firstLeader);

        assertWithin(5, SECONDS, () -> {
            long leaderCount = fixture.leaders().stream().filter(n -> !firstLeader.equals(n)).count();
            assertEquals(1, leaderCount);
        });

        fixture.clearNetworkProblems();

        assertWithin(5, SECONDS, () -> assertFalse(fixture.getNode(firstLeader).isLeader()));
    }

    @Test
    public void testLeaderStepsDownAfterReconnection_SlowNetworkToNode2() throws InterruptedException {
        fixture.setCommunicationDelay("node1", "node2", 50, 100);
        fixture.setCommunicationDelay("node3", "node2", 50, 100);
        fixture.startNodes();

        assertWithin(5, SECONDS, () -> {
            long leaderCount = fixture.leaders().size();
            assertEquals(1, leaderCount);
        });

        String firstLeader = fixture.leaders().stream().findFirst().orElseThrow(() -> new AssertionError("Expected at least one leader. Did it step down?"));
        fixture.createNetworkPartition(firstLeader);

        assertWithin(5, SECONDS, () -> {
            long leaderCount = fixture.leaders().stream().filter(n -> !firstLeader.equals(n)).count();
            assertEquals(1, leaderCount);
        });

        fixture.clearNetworkProblems();

        assertWithin(1, SECONDS, () -> assertFalse(fixture.getNode(firstLeader).isLeader()));
    }
}
