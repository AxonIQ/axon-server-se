package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.FakeStateFactory.FakeState;
import io.axoniq.axonserver.cluster.election.ElectionStore;
import io.axoniq.axonserver.cluster.election.InMemoryElectionStore;
import io.axoniq.axonserver.cluster.replication.InMemoryLogEntryStore;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import org.junit.*;

import static io.axoniq.axonserver.cluster.TestUtils.assertWithin;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class CandidateStateTest {

    private static final long MIN_ELECTION_TIMEOUT = 150L;
    private static final long MAX_ELECTION_TIMEOUT = 300L;

    private long electionTimeout = 160L;
    private RaftGroup raftGroup;
    private FakeRaftConfiguration raftConfiguration;
    private ElectionStore electionStore;
    private FakeScheduler fakeScheduler;
    private FakeTransitionHandler transitionHandler;
    private CandidateState candidateState;
    private FakeRaftPeer node0 = new FakeRaftPeer("node0");
    private FakeRaftPeer node1 = new FakeRaftPeer("node1");
    private FakeRaftPeer node2 = new FakeRaftPeer("node2");

    @Before
    public void setUp() throws Exception {
        raftConfiguration = new FakeRaftConfiguration("defaultGroup");
        RaftNode localNode = mock(RaftNode.class);
        when(localNode.nodeId()).thenReturn("node0");

        electionStore = new InMemoryElectionStore();
        electionStore.updateCurrentTerm(0);

        raftGroup = mock(RaftGroup.class);
        when(raftGroup.localLogEntryStore()).thenReturn(new InMemoryLogEntryStore());
        when(raftGroup.localElectionStore()).thenReturn(electionStore);
        when(raftGroup.raftConfiguration()).thenReturn(raftConfiguration);
        when(raftGroup.localNode()).thenReturn(localNode);

        transitionHandler = new FakeTransitionHandler();

        addClusterNode("node0", node0);
        addClusterNode("node1", node1);
        addClusterNode("node2", node2);

        fakeScheduler = new FakeScheduler();
        candidateState = CandidateState.builder()
                                       .raftGroup(raftGroup)
                                       .transitionHandler(transitionHandler)
                                       .scheduler(fakeScheduler)
                                       .randomValueSupplier((min, max) -> electionTimeout)
                                       .stateFactory(new FakeStateFactory()).build();
    }

    private void addClusterNode(String nodeId, FakeRaftPeer peer){
        Node node = Node.newBuilder().setNodeId(nodeId).build();
        raftConfiguration.addNode(node);
        when(raftGroup.peer(nodeId)).thenReturn(peer);
        peer.setTerm(1);
    }

    @After
    public void tearDown() throws Exception {
        candidateState.stop();
    }

    @Test
    public void requestVoteSameTerm() {
        candidateState.start();
        RequestVoteRequest request = RequestVoteRequest.newBuilder().setTerm(1).build();
        RequestVoteResponse response = candidateState.requestVote(request);
        assertFalse(response.getVoteGranted());
    }

    @Test
    public void requestVoteGreaterTerm() {
        candidateState.start();
        RequestVoteRequest request = RequestVoteRequest.newBuilder().setTerm(10).build();
        RequestVoteResponse response = candidateState.requestVote(request);
        MembershipState membershipState = transitionHandler.lastTransition();
        assertTrue(membershipState instanceof FakeState);
        FakeState fakeState = (FakeState) membershipState;
        assertEquals("follower", fakeState.name());
        assertEquals("requestVote", fakeState.lastMethodCalled());
    }

    @Test
    public void requestVoteLowerTerm() {
        candidateState.start();
        RequestVoteRequest request = RequestVoteRequest.newBuilder().setTerm(0).build();
        RequestVoteResponse response = candidateState.requestVote(request);
        assertFalse(response.getVoteGranted());
    }

    @Test
    public void appendEntriesSameTerm() {
        candidateState.start();
        AppendEntriesRequest request = AppendEntriesRequest.newBuilder().setTerm(1).build();
        AppendEntriesResponse response = candidateState.appendEntries(request);
        MembershipState membershipState = transitionHandler.lastTransition();
        assertTrue(membershipState instanceof FakeState);
        FakeState fakeState = (FakeState) membershipState;
        assertEquals("follower", fakeState.name());
        assertEquals("appendEntries", fakeState.lastMethodCalled());
    }

    @Test
    public void appendEntriesGreaterTerm() {
        candidateState.start();
        AppendEntriesRequest request = AppendEntriesRequest.newBuilder().setTerm(10).build();
        AppendEntriesResponse response = candidateState.appendEntries(request);
        MembershipState membershipState = transitionHandler.lastTransition();
        assertTrue(membershipState instanceof FakeState);
        FakeState fakeState = (FakeState) membershipState;
        assertEquals("follower", fakeState.name());
        assertEquals("appendEntries", fakeState.lastMethodCalled());
    }

    @Test
    public void appendEntriesLowerTerm() {
        candidateState.start();
        AppendEntriesRequest request = AppendEntriesRequest.newBuilder().setTerm(0).build();
        AppendEntriesResponse response = candidateState.appendEntries(request);
        assertTrue(response.hasFailure());
    }

    @Test
    public void installSnapshotSameTerm() {
        candidateState.start();
        InstallSnapshotRequest request = InstallSnapshotRequest.newBuilder().setTerm(1).build();
        InstallSnapshotResponse response = candidateState.installSnapshot(request);
        assertTrue(response.hasFailure());
    }

    @Test
    public void installSnapshotGreaterTerm() {
        candidateState.start();
        InstallSnapshotRequest request = InstallSnapshotRequest.newBuilder().setTerm(10).build();
        InstallSnapshotResponse response = candidateState.installSnapshot(request);
        MembershipState membershipState = transitionHandler.lastTransition();
        assertTrue(membershipState instanceof FakeState);
        FakeState fakeState = (FakeState) membershipState;
        assertEquals("follower", fakeState.name());
        assertEquals("installSnapshot", fakeState.lastMethodCalled());
    }

    @Test
    public void installSnapshotLowerTerm() {
        candidateState.start();
        InstallSnapshotRequest request = InstallSnapshotRequest.newBuilder().setTerm(0).build();
        InstallSnapshotResponse response = candidateState.installSnapshot(request);
        assertTrue(response.hasFailure());
    }

    @Test
    public void electionWon() throws InterruptedException {
        node1.setTerm(1);
        node1.setVoteGranted(true);
        candidateState.start();
        assertWithin(50, MILLISECONDS,() -> assertTrue(transitionHandler.lastTransition() instanceof FakeState));
        MembershipState membershipState = transitionHandler.lastTransition();
        FakeState fakeState = (FakeState) membershipState;
        assertEquals("leader", fakeState.name());
    }

    @Test
    public void electionRescheduled() throws InterruptedException{
        candidateState.start();
        node1.setTerm(2);
        node2.setTerm(2);
        Thread.sleep(50);
        fakeScheduler.timeElapses(electionTimeout+1);
        Thread.sleep(50);
        node1.setTerm(3);
        node2.setTerm(3);
        node1.setVoteGranted(true);
        fakeScheduler.timeElapses(electionTimeout+1);
        assertWithin(50, MILLISECONDS,() -> assertTrue(transitionHandler.lastTransition() instanceof FakeState));
        MembershipState membershipState = transitionHandler.lastTransition();
        FakeState fakeState = (FakeState) membershipState;
        assertEquals("leader", fakeState.name());

    }



}