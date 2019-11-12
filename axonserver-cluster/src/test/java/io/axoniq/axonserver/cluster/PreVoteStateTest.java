package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.FakeStateFactory.FakeState;
import io.axoniq.axonserver.cluster.election.ElectionStore;
import io.axoniq.axonserver.cluster.election.InMemoryElectionStore;
import io.axoniq.axonserver.cluster.replication.InMemoryLogEntryStore;
import io.axoniq.axonserver.cluster.snapshot.FakeSnapshotManager;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import org.junit.*;

import java.util.Collections;
import java.util.function.BiConsumer;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class PreVoteStateTest {

    private int electionTimeout = 160;
    private RaftGroup raftGroup;
    private FakeRaftConfiguration raftConfiguration;
    private FakeScheduler fakeScheduler;
    private FakeTransitionHandler transitionHandler;
    private PreVoteState preVoteState;
    private FakeRaftPeer node0;
    private FakeRaftPeer node1;
    private FakeRaftPeer node2;

    @Before
    public void setUp() throws Exception {
        raftConfiguration = new FakeRaftConfiguration("defaultGroup", "node0");
        RaftNode localNode = mock(RaftNode.class);
        when(localNode.nodeId()).thenReturn("node0");

        ElectionStore electionStore = new InMemoryElectionStore();
        electionStore.updateCurrentTerm(0);

        raftGroup = mock(RaftGroup.class);
        when(raftGroup.localLogEntryStore()).thenReturn(new InMemoryLogEntryStore("Test"));
        when(raftGroup.localElectionStore()).thenReturn(electionStore);
        when(raftGroup.raftConfiguration()).thenReturn(raftConfiguration);
        when(raftGroup.createIterator()).thenReturn(Collections.emptyIterator());
        LogEntryProcessor logEntryProcessor = mock(LogEntryProcessor.class);
        when(raftGroup.logEntryProcessor()).thenReturn(logEntryProcessor);
        when(raftGroup.localNode()).thenReturn(localNode);

        transitionHandler = new FakeTransitionHandler();
        fakeScheduler = new FakeScheduler();
        node0 = new FakeRaftPeer(fakeScheduler, "node0");
        node1 = new FakeRaftPeer(fakeScheduler, "node1");
        node2 = new FakeRaftPeer(fakeScheduler, "node2");
        addClusterNode("node0", node0);
        addClusterNode("node1", node1);
        addClusterNode("node2", node2);

        CurrentConfiguration currentConfiguration = mock(CurrentConfiguration.class);
        when(currentConfiguration.groupMembers()).thenReturn(asList(node("node0"),
                                                                    node("node1"),
                                                                    node("node2")));

        BiConsumer<Long, String> termUpdateHandler = (term, cause) -> electionStore.updateCurrentTerm(term);
        preVoteState = PreVoteState.builder()
                                   .raftGroup(raftGroup)
                                   .transitionHandler(transitionHandler)
                                   .termUpdateHandler(termUpdateHandler)
                                   .snapshotManager(new FakeSnapshotManager())
                                   .schedulerFactory(() -> fakeScheduler)
                                   .randomValueSupplier((min, max) -> electionTimeout)
                                   .currentConfiguration(currentConfiguration)
                                   .registerConfigurationListenerFn(l -> () -> {
                                   })
                                   .stateFactory(new FakeStateFactory()).build();
    }

    private void addClusterNode(String nodeId, FakeRaftPeer peer) {
        Node node = Node.newBuilder().setNodeId(nodeId).build();
        raftConfiguration.addNode(node);
        when(raftGroup.peer(node)).thenReturn(peer);
        peer.setTerm(1);
    }

    @After
    public void tearDown() throws Exception {
        preVoteState.stop();
    }

    @Test
    public void requestVoteSameTerm() {
        preVoteState.start();
        RequestVoteRequest request = RequestVoteRequest.newBuilder().setTerm(0).build();
        RequestVoteResponse response = preVoteState.requestVote(request);
        assertFalse(response.getVoteGranted());
    }

    @Test
    public void requestVoteGreaterTerm() {
        preVoteState.start();
        RequestVoteRequest request = RequestVoteRequest.newBuilder()
                                                       .setCandidateId("node1")
                                                       .setTerm(10)
                                                       .build();
        RequestVoteResponse response = preVoteState.requestVote(request);
        MembershipState membershipState = transitionHandler.lastTransition();
        assertTrue(membershipState instanceof FakeState);
        FakeState fakeState = (FakeState) membershipState;
        assertEquals("follower", fakeState.name());
        assertEquals("requestVote", fakeState.lastMethodCalled());
    }

    @Test
    public void requestVoteLowerTerm() {
        preVoteState.start();
        RequestVoteRequest request = RequestVoteRequest.newBuilder().setTerm(0).build();
        RequestVoteResponse response = preVoteState.requestVote(request);
        assertFalse(response.getVoteGranted());
    }

    @Test
    public void appendEntriesSameTerm() {
        preVoteState.start();
        AppendEntriesRequest request = AppendEntriesRequest.newBuilder().setTerm(1).build();
        AppendEntriesResponse response = preVoteState.appendEntries(request);
        MembershipState membershipState = transitionHandler.lastTransition();
        assertTrue(membershipState instanceof FakeState);
        FakeState fakeState = (FakeState) membershipState;
        assertEquals("follower", fakeState.name());
        assertEquals("appendEntries", fakeState.lastMethodCalled());
    }

    @Test
    public void appendEntriesGreaterTerm() {
        preVoteState.start();
        AppendEntriesRequest request = AppendEntriesRequest.newBuilder().setTerm(10).build();
        AppendEntriesResponse response = preVoteState.appendEntries(request);
        MembershipState membershipState = transitionHandler.lastTransition();
        assertTrue(membershipState instanceof FakeState);
        FakeState fakeState = (FakeState) membershipState;
        assertEquals("follower", fakeState.name());
        assertEquals("appendEntries", fakeState.lastMethodCalled());
    }

    @Test
    public void appendEntriesLowerTerm() {
        preVoteState.start();
        AppendEntriesRequest request = AppendEntriesRequest.newBuilder().setTerm(-1).build();
        AppendEntriesResponse response = preVoteState.appendEntries(request);
        assertTrue(response.hasFailure());
    }

    @Test
    public void installSnapshotSameTerm() {
        preVoteState.start();
        InstallSnapshotRequest request = InstallSnapshotRequest.newBuilder().setTerm(0).build();
        InstallSnapshotResponse response = preVoteState.installSnapshot(request);
        assertTrue(response.hasFailure());
    }

    @Test
    public void installSnapshotGreaterTerm() {
        preVoteState.start();
        InstallSnapshotRequest request = InstallSnapshotRequest.newBuilder().setTerm(10).build();
        InstallSnapshotResponse response = preVoteState.installSnapshot(request);
        MembershipState membershipState = transitionHandler.lastTransition();
        assertTrue(membershipState instanceof FakeState);
        FakeState fakeState = (FakeState) membershipState;
        assertEquals("follower", fakeState.name());
        assertEquals("installSnapshot", fakeState.lastMethodCalled());
    }

    @Test
    public void installSnapshotLowerTerm() {
        preVoteState.start();
        InstallSnapshotRequest request = InstallSnapshotRequest.newBuilder().setTerm(0).build();
        InstallSnapshotResponse response = preVoteState.installSnapshot(request);
        assertTrue(response.hasFailure());
    }

    @Test
    public void electionWon() {
        node2.setTerm(0);
        node1.setTerm(0);
        node1.setVoteGranted(true);
        preVoteState.start();
        fakeScheduler.timeElapses(30);
        assertTrue(transitionHandler.lastTransition() instanceof FakeState);
        MembershipState membershipState = transitionHandler.lastTransition();
        FakeState fakeState = (FakeState) membershipState;
        assertEquals("candidate", fakeState.name());
    }

    @Test
    public void electionRescheduled() {
        preVoteState.start();
        node2.setTerm(1);
        node1.setTerm(1);
        node1.setVoteGranted(true);
        fakeScheduler.timeElapses(electionTimeout + 1);
        fakeScheduler.timeElapses(30);
        assertTrue(transitionHandler.lastTransition() instanceof FakeState);
        MembershipState membershipState = transitionHandler.lastTransition();
        FakeState fakeState = (FakeState) membershipState;
        assertEquals("candidate", fakeState.name());
    }

    private Node node(String id) {
        return Node.newBuilder()
                   .setNodeId(id)
                   .build();
    }
}