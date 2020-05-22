package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.election.ElectionStore;
import io.axoniq.axonserver.cluster.election.InMemoryElectionStore;
import io.axoniq.axonserver.cluster.replication.InMemoryLogEntryStore;
import io.axoniq.axonserver.cluster.replication.MajorityMatchStrategy;
import io.axoniq.axonserver.cluster.snapshot.FakeSnapshotManager;
import io.axoniq.axonserver.grpc.cluster.ConfigChangeResult;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.Role;
import org.junit.*;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Test failed configuration changes when no entry has been committed during current term.
 *
 * @author Sara Pellegrini
 */
public class LeaderStateChangeConfigTest {

    private FakeRaftConfiguration raftConfiguration;
    private RaftGroup raftGroup;
    private FakeTransitionHandler transitionHandler;
    private FakeScheduler scheduler;
    private ElectionStore electionStore;
    private RaftNode localNode;
    private MajorityMatchStrategy matchStrategy;
    private LogEntryProcessor logEntryProcessor;

    @Before
    public void setUp() throws Exception {
        raftConfiguration = new FakeRaftConfiguration("defaultGroup", "node0");
        transitionHandler = new FakeTransitionHandler();
        raftGroup = mock(RaftGroup.class);
        electionStore = new InMemoryElectionStore();
        electionStore.updateCurrentTerm(1);
        when(raftGroup.localLogEntryStore()).thenReturn(new InMemoryLogEntryStore("Test"));
        when(raftGroup.localElectionStore()).thenReturn(electionStore);
        logEntryProcessor = mock(LogEntryProcessor.class);
        when(raftGroup.logEntryProcessor()).thenReturn(logEntryProcessor);
        when(raftGroup.createIterator()).thenReturn(Collections.emptyIterator());
        when(raftGroup.raftConfiguration()).thenReturn(raftConfiguration);
        localNode = mock(RaftNode.class);
        when(localNode.nodeId()).thenReturn("node0");
        when(raftGroup.localNode()).thenReturn(localNode);
        scheduler = new FakeScheduler();


        FakeRaftPeer node0 = new FakeRaftPeer(scheduler, "node0", "Node-Name-0", Role.PRIMARY);
        FakeRaftPeer node1 = new FakeRaftPeer(scheduler, "node1", "Node-Name-1", Role.PRIMARY);
        FakeRaftPeer node2 = new FakeRaftPeer(scheduler, "node2", "Node-Name-2", Role.PRIMARY);

        addClusterNode("node0", node0);
        addClusterNode("node1", node1);
        addClusterNode("node2", node2);

        matchStrategy = new MajorityMatchStrategy(() -> raftGroup.localLogEntryStore().lastLogIndex(),
                                                  () -> raftGroup.localNode().replicatorPeers(),
                                                  () -> raftGroup.raftConfiguration().minActiveBackups());

    }

    private void addClusterNode(String nodeId, FakeRaftPeer peer){
        Node node = Node.newBuilder().setNodeId(nodeId).build();
        raftConfiguration.addNode(node);
        when(raftGroup.peer(node)).thenReturn(peer);
        peer.setTerm(2);
    }

    @Test
    public void test() throws ExecutionException, InterruptedException {
        LeaderState leaderState = LeaderState.builder()
                                             .raftGroup(raftGroup)
                                             .transitionHandler(transitionHandler)
                                             .termUpdateHandler((term, cause) -> electionStore.updateCurrentTerm(term))
                                             .schedulerFactory(() -> scheduler)
                                             .snapshotManager(new FakeSnapshotManager())
                                             .stateFactory(new FakeStateFactory())
                                             .matchStrategy(matchStrategy)
                                             .build();
        when(localNode.replicatorPeers()).thenReturn(leaderState.replicatorPeers());
        when(logEntryProcessor.commitTerm()).thenReturn(0L);
        leaderState.start();

        CompletableFuture<ConfigChangeResult> changeResult = leaderState.removeServer("node2");
        ConfigChangeResult result = changeResult.get();
        assertTrue(result.hasFailure());
        assertEquals("AXONIQ-10007",result.getFailure().getError().getCode());
    }

}