package io.axoniq.axonserver.cluster.util;

import io.axoniq.axonserver.cluster.FakeClock;
import io.axoniq.axonserver.cluster.FakeReplicatorPeer;
import io.axoniq.axonserver.cluster.ReplicatorPeerStatus;
import io.axoniq.axonserver.grpc.cluster.Role;
import org.junit.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class LeaderTimeoutCheckerTest {

    private LeaderTimeoutChecker testSubject;
    private List<ReplicatorPeerStatus> peers = new ArrayList<>();
    private FakeClock clock = new FakeClock(Instant.ofEpochMilli(100));

    @Before
    public void setUp() throws Exception {
        testSubject = new LeaderTimeoutChecker(peers, 100, clock);
    }

    @Test
    public void noFollowers() {
        assertTrue(testSubject.heardFromFollowers().result());
    }

    @Test
    public void primaryTooOld() {
        peers.addAll(Arrays.asList(new FakeReplicatorPeer(40, Role.PRIMARY),
                                   new FakeReplicatorPeer(70, Role.PRIMARY)));
        LeaderTimeoutChecker.CheckResult checkResult = testSubject.heardFromFollowers();
        assertTrue(checkResult.result());
        assertEquals(40, checkResult.nextCheckInterval());

        clock.plusMillis(60);
        checkResult = testSubject.heardFromFollowers();

        assertTrue(checkResult.result());
        assertEquals(10, checkResult.nextCheckInterval());
        clock.plusMillis(11);
        assertFalse(testSubject.heardFromFollowers().result());
    }

    @Test
    public void activeBackupsTimeout() {
        peers.addAll(Arrays.asList(new FakeReplicatorPeer(50, Role.PRIMARY),
                                   new FakeReplicatorPeer(70, Role.PRIMARY),
                                   new FakeReplicatorPeer(20, Role.ACTIVE_BACKUP),
                                   new FakeReplicatorPeer(40, Role.ACTIVE_BACKUP)));
        assertTrue(testSubject.heardFromFollowers().result());
        clock.plusMillis(22);
        assertTrue(testSubject.heardFromFollowers().result());
        clock.plusMillis(22);
        assertFalse(testSubject.heardFromFollowers().result());
    }

    @Test
    public void passiveBackupsHaveNoImpact() {
        peers.addAll(Arrays.asList(new FakeReplicatorPeer(50, Role.PRIMARY),
                                   new FakeReplicatorPeer(70, Role.PRIMARY),
                                   new FakeReplicatorPeer(20, Role.PASSIVE_BACKUP),
                                   new FakeReplicatorPeer(40, Role.PASSIVE_BACKUP)));
        assertTrue(testSubject.heardFromFollowers().result());
        clock.plusMillis(22);
        assertTrue(testSubject.heardFromFollowers().result());
        clock.plusMillis(22);
        assertTrue(testSubject.heardFromFollowers().result());
    }
}