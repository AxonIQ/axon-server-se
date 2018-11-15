package io.axoniq.axonserver.cluster;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static io.axoniq.axonserver.cluster.TestUtils.assertWithin;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

public class RaftClusterIntegrationTest {

    private RaftClusterTestFixture fixture;

    @Before
    public void setUp() throws Exception {
        fixture = new RaftClusterTestFixture("node1", "node2", "node3");
    }

    @After
    public void tearDown() throws Exception {
        fixture.shutdown();
    }

    @Ignore("Must be activate once follower state is implemented")
    @Test
    public void testClusterStart() {
        fixture.nodes().forEach(n -> n.appendEntry("mock", "Mock".getBytes()));
        fixture.startNodes();
    }

    @Ignore("Must be activate once leader election is implemented")
    @Test
    public void testClusterElectsLeader() throws InterruptedException {
        fixture.startNodes();

        assertWithin(5, SECONDS, () -> {
            long leaderCount = fixture.nodes().stream().map(RaftNode::isLeader).filter(Boolean::valueOf).count();
            assertEquals(1, leaderCount);
        });
    }
}
