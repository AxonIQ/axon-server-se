package io.axoniq.axonserver.cluster.replication;

import io.axoniq.axonserver.cluster.FakeReplicatorPeer;
import io.axoniq.axonserver.cluster.ReplicatorPeerStatus;
import io.axoniq.axonserver.grpc.cluster.Role;
import org.junit.*;

import java.time.Clock;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class MajorityMatchStrategyTest {
    private MajorityMatchStrategy testSubject;
    private List<ReplicatorPeerStatus> replicationPeers;
    private Clock clock = Clock.systemDefaultZone();


    @Before
    public void setup() {
        replicationPeers = Arrays.asList(new FakeReplicatorPeer(0, Role.PRIMARY),
                                         new FakeReplicatorPeer(0, Role.PRIMARY));
        testSubject = new MajorityMatchStrategy(() -> 10L, () -> replicationPeers.iterator());

    }

    @Test
    public void match() {
        ((FakeReplicatorPeer) replicationPeers.get(0)).setMatchIndex(7);
        ((FakeReplicatorPeer) replicationPeers.get(1)).setMatchIndex(8);
        assertTrue(testSubject.match(8));
    }

    @Test
    public void noMatchFromPeers() {
        ((FakeReplicatorPeer) replicationPeers.get(0)).setMatchIndex(7);
        ((FakeReplicatorPeer) replicationPeers.get(1)).setMatchIndex(7);
        assertFalse( testSubject.match(8));
    }

    @Test
    public void noMatchFromLeader() {
        ((FakeReplicatorPeer) replicationPeers.get(0)).setMatchIndex(11);
        ((FakeReplicatorPeer) replicationPeers.get(1)).setMatchIndex(8);
        assertFalse(testSubject.match(11));
    }

    @Test
    public void matchFromLeader() {
        ((FakeReplicatorPeer) replicationPeers.get(0)).setMatchIndex(11);
        ((FakeReplicatorPeer) replicationPeers.get(1)).setMatchIndex(11);
        assertTrue(testSubject.match(11));
    }

    @Test
    public void matchWithSecondary() {
        FakeReplicatorPeer follower1 = new FakeReplicatorPeer(0, Role.PRIMARY);
        FakeReplicatorPeer follower2 = new FakeReplicatorPeer(0, Role.PRIMARY);

        FakeReplicatorPeer activeBackup1 = new FakeReplicatorPeer(0, Role.ACTIVE_BACKUP);
        FakeReplicatorPeer activeBackup2 = new FakeReplicatorPeer(0, Role.ACTIVE_BACKUP);
        replicationPeers = Arrays.asList(follower1, follower2, activeBackup1, activeBackup2);
        follower1.setMatchIndex(11);
        follower2.setMatchIndex(8);
        activeBackup1.setMatchIndex(11);
        activeBackup2.setMatchIndex(8);

        assertTrue(testSubject.match(10));
    }


    @Test
    public void matchWithPrimaryOnly() {
        FakeReplicatorPeer follower1 = new FakeReplicatorPeer(0, Role.PRIMARY);
        FakeReplicatorPeer follower2 = new FakeReplicatorPeer(0, Role.PRIMARY);

        FakeReplicatorPeer activeBackup1 = new FakeReplicatorPeer(0, Role.ACTIVE_BACKUP);
        FakeReplicatorPeer activeBackup2 = new FakeReplicatorPeer(0, Role.ACTIVE_BACKUP);
        replicationPeers = Arrays.asList(follower1, follower2, activeBackup1, activeBackup2);
        follower1.setMatchIndex(11);
        follower2.setMatchIndex(11);
        activeBackup1.setMatchIndex(8);
        activeBackup2.setMatchIndex(8);

        assertFalse(testSubject.match(10));
    }

    @Test
    public void matchWithoutSecondaryMajorityFails() {
        FakeReplicatorPeer follower1 = new FakeReplicatorPeer(0, Role.PRIMARY);
        FakeReplicatorPeer follower2 = new FakeReplicatorPeer(0, Role.PRIMARY);

        FakeReplicatorPeer activeBackup1 = new FakeReplicatorPeer(0, Role.ACTIVE_BACKUP);
        FakeReplicatorPeer activeBackup2 = new FakeReplicatorPeer(0, Role.ACTIVE_BACKUP);

        replicationPeers = Arrays.asList(follower1, follower2, activeBackup1, activeBackup2);
        follower1.setMatchIndex(11);
        follower2.setMatchIndex(8);
        activeBackup1.setMatchIndex(8);
        activeBackup2.setMatchIndex(8);

        assertFalse(testSubject.match(10));
    }
}