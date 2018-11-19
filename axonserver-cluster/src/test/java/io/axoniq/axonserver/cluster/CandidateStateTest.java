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

    private RaftGroup raftGroup;
    private FakeRaftConfiguration raftConfiguration;
    private ElectionStore electionStore;
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
        candidateState = CandidateState.builder()
                                       .raftGroup(raftGroup)
                                       .transitionHandler(transitionHandler)
                                       .stateFactory(new FakeStateFactory()).build();
        candidateState.start();
    }

    private void addClusterNode(String nodeId, RaftPeer peer){
        Node node = Node.newBuilder().setNodeId(nodeId).build();
        raftConfiguration.addNode(node);
        when(raftGroup.peer(nodeId)).thenReturn(peer);
    }

    @After
    public void tearDown() throws Exception {
        candidateState.stop();
    }

    @Test
    public void requestVoteSameTerm() {
        RequestVoteRequest request = RequestVoteRequest.newBuilder().setTerm(1).build();
        RequestVoteResponse response = candidateState.requestVote(request);
        assertFalse(response.getVoteGranted());
    }

    @Test
    public void requestVoteGreaterTerm() {
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
        RequestVoteRequest request = RequestVoteRequest.newBuilder().setTerm(0).build();
        RequestVoteResponse response = candidateState.requestVote(request);
        assertFalse(response.getVoteGranted());
    }

    @Test
    public void appendEntriesSameTerm() {
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
        AppendEntriesRequest request = AppendEntriesRequest.newBuilder().setTerm(0).build();
        AppendEntriesResponse response = candidateState.appendEntries(request);
        assertTrue(response.hasFailure());
    }

    @Test
    public void installSnapshotSameTerm() {
        InstallSnapshotRequest request = InstallSnapshotRequest.newBuilder().setTerm(1).build();
        InstallSnapshotResponse response = candidateState.installSnapshot(request);
        assertTrue(response.hasFailure());
    }

    @Test
    public void installSnapshotGreaterTerm() {
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
        InstallSnapshotRequest request = InstallSnapshotRequest.newBuilder().setTerm(0).build();
        InstallSnapshotResponse response = candidateState.installSnapshot(request);
        assertTrue(response.hasFailure());
    }

    @Test
    public void electionWon() throws InterruptedException {
        node1.setTerm(2);
        node1.setVoteGranted(true);
        Thread.sleep(60);
        assertWithin(50, MILLISECONDS,() -> assertTrue(transitionHandler.lastTransition() instanceof FakeState));
        MembershipState membershipState = transitionHandler.lastTransition();
        FakeState fakeState = (FakeState) membershipState;
        assertEquals("leader", fakeState.name());
    }



}