package io.axoniq.axonserver.cluster.replication;

import io.axoniq.axonserver.cluster.ReplicatorPeer;
import org.junit.*;

import java.time.Clock;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Author: marc
 */
public class MajorityMatchStrategyTest {
    private MajorityMatchStrategy testSubject;
    private List<FakeReplicationPeer> replicationPeers;
    private Clock clock = Clock.systemDefaultZone();


    @Before
    public void setup() {
        testSubject = new MajorityMatchStrategy(() -> 10L);
        replicationPeers = Arrays.asList(new FakeReplicationPeer(), new FakeReplicationPeer());

    }

    @Test
    public void match() {
        replicationPeers.get(0).setMatchIndex(7);
        replicationPeers.get(1).setMatchIndex(8);
        assertTrue(testSubject.match(8, replicationPeers));
    }

    @Test
    public void noMatchFromPeers() {
        replicationPeers.get(0).setMatchIndex(7);
        replicationPeers.get(1).setMatchIndex(7);
        assertFalse( testSubject.match(8, replicationPeers));
    }

    @Test
    public void noMatchFromLeader() {
        replicationPeers.get(0).setMatchIndex(11);
        replicationPeers.get(1).setMatchIndex(8);
        assertFalse(testSubject.match(11, replicationPeers));
    }

    @Test
    public void matchFromLeader() {
        replicationPeers.get(0).setMatchIndex(11);
        replicationPeers.get(1).setMatchIndex(11);
        assertTrue(testSubject.match(11, replicationPeers));
    }

    private class FakeReplicationPeer extends ReplicatorPeer {

        private long matchIndex;

        public FakeReplicationPeer() {
            super(null, i -> {}, clock, null, null,
                  (term,reason) -> {});
        }

        public void setMatchIndex(long matchIndex) {
            this.matchIndex = matchIndex;
        }

        @Override
        public long matchIndex() {
            return matchIndex;
        }
    }
}