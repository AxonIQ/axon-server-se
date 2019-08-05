package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import org.junit.*;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class RaftLeaderProviderTest {

    private RaftLeaderProvider testSubject = new RaftLeaderProvider("me", 1000);

    @Test
    public void getLeader() {
        testSubject.on(new ClusterEvents.BecomeLeader("context", () -> null));
        assertEquals("me", testSubject.getLeader("context"));
        assertTrue(testSubject.isLeader("context"));
        testSubject.on(new ClusterEvents.LeaderStepDown("context", true));
        assertNull(testSubject.getLeader("context"));
        assertFalse(testSubject.isLeader("context"));
        testSubject.on(new ClusterEvents.LeaderConfirmation("context", "other", false));
        assertEquals("other", testSubject.getLeader("context"));
        assertFalse(testSubject.isLeader("context"));
    }

    @Test
    public void getLeaderOrWait() {
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.schedule(() -> testSubject
                                                  .on(new ClusterEvents.LeaderConfirmation("context", "other", false)),
                                          220, TimeUnit.MILLISECONDS);
        assertEquals("other", testSubject.getLeaderOrWait("context", true));
    }
}