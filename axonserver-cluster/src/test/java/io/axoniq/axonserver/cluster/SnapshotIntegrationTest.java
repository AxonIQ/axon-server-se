package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.scheduler.Scheduler;
import io.axoniq.axonserver.cluster.snapshot.FakeSnapshotManager;
import io.axoniq.axonserver.cluster.snapshot.SnapshotManager;
import org.junit.*;

/**
 * Tests install snapshot as a whole.
 *
 * @author Milan Savic
 */
public class SnapshotIntegrationTest {

    private Scheduler scheduler;
    private SnapshotManager snapshotManager;

    @Before
    public void setUp() {
        scheduler = new FakeScheduler();
        snapshotManager = new FakeSnapshotManager();
    }

    @Test
    public void testAddingNewNodeToWorkingCluster() {
//        RaftClusterTestFixture clusterFixture = new RaftClusterTestFixture(snapshotManager,
//                                                                           scheduler,
//                                                                           "node1",
//                                                                           "node2",
//                                                                           "node3");
//        clusterFixture.startNodes();

    }
}
