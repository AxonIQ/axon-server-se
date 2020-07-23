package io.axoniq.axonserver.enterprise.replication;

import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import org.junit.*;
import org.springframework.context.ApplicationEventPublisher;

import java.util.Collections;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class RaftLeaderProviderTest {

    private ApplicationEventPublisher applicationEventPublisher = mock(ApplicationEventPublisher.class);
    private RaftLeaderProviderImpl testSubject = new RaftLeaderProviderImpl("me",
                                                                            Collections::singleton,
                                                                            applicationEventPublisher);

    @Test
    public void getLeader() {
        testSubject.on(new ClusterEvents.BecomeLeader("context", () -> null));
        assertEquals("me", testSubject.getLeader("context"));
        assertTrue(testSubject.isLeader("context"));
        testSubject.on(new ClusterEvents.LeaderStepDown("context"));
        assertNull(testSubject.getLeader("context"));
        assertFalse(testSubject.isLeader("context"));
        testSubject.on(new ClusterEvents.LeaderConfirmation("context", "other"));
        assertEquals("other", testSubject.getLeader("context"));
        assertFalse(testSubject.isLeader("context"));
    }

}