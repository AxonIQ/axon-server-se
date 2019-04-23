package io.axoniq.axonserver.cluster.replication;

import io.axoniq.axonserver.cluster.RaftConfiguration;
import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.ReplicatorPeer;
import io.axoniq.axonserver.cluster.election.ElectionStore;
import org.junit.*;

import java.time.Clock;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class MajorityMatchStrategyTest {
    private MajorityMatchStrategy testSubject;
    private List<ReplicatorPeer> replicationPeers;
    private Clock clock = Clock.systemDefaultZone();


    @Before
    public void setup() {
        RaftConfiguration raftConfiguration = mock(RaftConfiguration.class);
        when(raftConfiguration.groupId()).thenReturn("mockGroup");

        ElectionStore electionStore = mock(ElectionStore.class);
        when(electionStore.currentTerm()).thenReturn(1L);

        RaftGroup raftGroup = mock(RaftGroup.class);
        when(raftGroup.raftConfiguration()).thenReturn(raftConfiguration);
        when(raftGroup.localElectionStore()).thenReturn(electionStore);

        replicationPeers = Arrays.asList(new FakeReplicationPeer(raftGroup), new FakeReplicationPeer(raftGroup));
        testSubject = new MajorityMatchStrategy(() -> 10L, () -> replicationPeers.iterator());

    }

    @Test
    public void match() {
        ((FakeReplicationPeer)replicationPeers.get(0)).setMatchIndex(7);
        ((FakeReplicationPeer)replicationPeers.get(1)).setMatchIndex(8);
        assertTrue(testSubject.match(8));
    }

    @Test
    public void noMatchFromPeers() {
        ((FakeReplicationPeer)replicationPeers.get(0)).setMatchIndex(7);
        ((FakeReplicationPeer)replicationPeers.get(1)).setMatchIndex(7);
        assertFalse( testSubject.match(8));
    }

    @Test
    public void noMatchFromLeader() {
        ((FakeReplicationPeer)replicationPeers.get(0)).setMatchIndex(11);
        ((FakeReplicationPeer)replicationPeers.get(1)).setMatchIndex(8);
        assertFalse(testSubject.match(11));
    }

    @Test
    public void matchFromLeader() {
        ((FakeReplicationPeer)replicationPeers.get(0)).setMatchIndex(11);
        ((FakeReplicationPeer)replicationPeers.get(1)).setMatchIndex(11);
        assertTrue(testSubject.match(11));
    }

    private class FakeReplicationPeer extends ReplicatorPeer {

        private long matchIndex;

        public FakeReplicationPeer(RaftGroup raftGroup) {
            super(null, i -> {}, clock, raftGroup , null,
                  (term,reason) -> {}, () -> 1L);
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