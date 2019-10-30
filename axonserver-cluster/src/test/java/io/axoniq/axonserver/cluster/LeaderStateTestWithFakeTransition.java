package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.election.ElectionStore;
import io.axoniq.axonserver.cluster.election.InMemoryElectionStore;
import io.axoniq.axonserver.cluster.replication.InMemoryLogEntryStore;
import io.axoniq.axonserver.cluster.snapshot.FakeSnapshotManager;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.ConfigChangeResult;
import io.axoniq.axonserver.grpc.cluster.ErrorMessage;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import io.axoniq.axonserver.grpc.cluster.Role;
import org.junit.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class LeaderStateTestWithFakeTransition {

    private RaftGroup raftGroup;
    private FakeRaftConfiguration raftConfiguration;
    private LeaderState leaderState;
    private FakeTransitionHandler transitionHandler;
    private FakeScheduler scheduler;

    @Before
    public void setUp() {
        raftConfiguration = new FakeRaftConfiguration("defaultGroup", "node0");
        ElectionStore electionStore = new InMemoryElectionStore();
        electionStore.updateCurrentTerm(1);
        RaftNode localNode = mock(RaftNode.class);
        when(localNode.nodeId()).thenReturn("node0");
        raftGroup = mock(RaftGroup.class);
        when(raftGroup.raftConfiguration()).thenReturn(raftConfiguration);
        when(raftGroup.localLogEntryStore()).thenReturn(new InMemoryLogEntryStore("Test"));
        when(raftGroup.localElectionStore()).thenReturn(electionStore);
        when(raftGroup.logEntryProcessor()).thenReturn(mock(LogEntryProcessor.class));
        when(raftGroup.localNode()).thenReturn(localNode);
        transitionHandler = new FakeTransitionHandler();
        scheduler = new FakeScheduler();

        FakeRaftPeer node0 = new FakeRaftPeer(scheduler, "node0");
        FakeRaftPeer node1 = new FakeRaftPeer(scheduler, "node1");
        FakeRaftPeer node2 = new FakeRaftPeer(scheduler, "node2");

        addClusterNode(node("node0"), node0);
        addClusterNode(node("node1"), node1);
        addClusterNode(node("node2"), node2);

        CurrentConfiguration currentConfiguration = mock(CurrentConfiguration.class);
        when(currentConfiguration.groupMembers()).thenReturn(asList(node("node0"),
                                                                    node("node1"),
                                                                    node("node2")));

        leaderState = LeaderState.builder()
                                 .raftGroup(raftGroup)
                                 .transitionHandler(transitionHandler)
                                 .schedulerFactory(() -> scheduler)
                                 .snapshotManager(new FakeSnapshotManager())
                                 .stateFactory(new FakeStateFactory())
                                 .termUpdateHandler((l, s) -> {
                                 })
                                 .currentConfiguration(currentConfiguration)
                                 .registerConfigurationListenerFn(l -> () -> {
                                 })
                                 .matchStrategy(nextCommitCandidate -> true)
                                 .build();
    }

    private void addClusterNode(Node node, FakeRaftPeer peer) {
        raftConfiguration.addNode(node);
        when(raftGroup.peer(node)).thenReturn(peer);
        peer.setTerm(1);
    }


    @After
    public void tearDown() {
        leaderState.stop();
    }

    @Test
    public void testRequestVote() {
        leaderState.start();
        RequestVoteRequest request = RequestVoteRequest.newBuilder()
                                                       .setCandidateId("node1")
                                                       .build();
        RequestVoteResponse response = leaderState.requestVote(request);
        assertFalse(response.getVoteGranted());
    }

    @Test
    public void testRequestVoteWhenCandidateIsNotAMember() {
        leaderState.start();
        RequestVoteRequest request = RequestVoteRequest.newBuilder()
                                                       .setCandidateId("node4")
                                                       .build();
        RequestVoteResponse response = leaderState.requestVote(request);
        assertFalse(response.getVoteGranted());
        assertTrue(response.getGoAway());
    }

    @Test
    public void testAppendEntriesSameTerm() {
        leaderState.start();
        AppendEntriesRequest request = AppendEntriesRequest.newBuilder().setTerm(1).build();
        AppendEntriesResponse response = leaderState.appendEntries(request);
        assertFalse(response.hasFailure());
        FakeStateFactory.FakeState fakeState = (FakeStateFactory.FakeState) transitionHandler.lastTransition();
        assertEquals("follower", fakeState.name());
    }

    @Test
    public void testAppendEntriesLowerTerm() {
        leaderState.start();
        AppendEntriesRequest request = AppendEntriesRequest.newBuilder().setTerm(0).build();
        AppendEntriesResponse response = leaderState.appendEntries(request);
        assertTrue(response.hasFailure());
    }

    @Test
    public void testInstallSnapshotSameTerm() {
        leaderState.start();
        InstallSnapshotRequest request = InstallSnapshotRequest.newBuilder().setTerm(1).build();
        InstallSnapshotResponse response = leaderState.installSnapshot(request);
        assertTrue(response.hasFailure());
    }

    @Test
    public void testInstallSnapshotLowerTerm() {
        leaderState.start();
        InstallSnapshotRequest request = InstallSnapshotRequest.newBuilder().setTerm(0).build();
        InstallSnapshotResponse response = leaderState.installSnapshot(request);
        assertTrue(response.hasFailure());
    }

    private Node node(String id) {
        return Node.newBuilder()
                   .setNodeId(id)
                   .build();
    }

    @Test
    public void addNewNodeTwiceTest() throws InterruptedException, ExecutionException {
        Node nodeA = Node
                .newBuilder()
                .setNodeId("nodeId-1")
                .setNodeName("nodeName")
                .setHost("host")
                .setPort(1234)
                .build();
        FakeRaftPeer raftPeerA = new FakeRaftPeer(scheduler, "nodeId-1", "nodeName", Role.PRIMARY);

        addClusterNode(nodeA, raftPeerA);
        Node nodeB = nodeA.toBuilder()
                          .setNodeId("nodeId-2")
                          .build();
        addClusterNode(nodeB, new FakeRaftPeer(scheduler, "nodeId-2", "nodeName", Role.PRIMARY));
        leaderState.start();
        CompletableFuture<ConfigChangeResult> futureA = leaderState.addServer(nodeA);
        CompletableFuture<ConfigChangeResult> futureB = leaderState.addServer(nodeB);

        ConfigChangeResult result = futureB.get();
        assertTrue(result.hasFailure());
        ErrorMessage error = result.getFailure().getError();
        assertEquals("Replicators already contain the node nodeName", error.getMessage());
    }
}
