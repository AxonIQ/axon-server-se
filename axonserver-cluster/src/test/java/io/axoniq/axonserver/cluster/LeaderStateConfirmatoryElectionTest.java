package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.election.Election;
import io.axoniq.axonserver.cluster.election.ElectionStore;
import io.axoniq.axonserver.cluster.election.InMemoryElectionStore;
import io.axoniq.axonserver.cluster.replication.InMemoryLogEntryStore;
import io.axoniq.axonserver.cluster.replication.MajorityMatchStrategy;
import io.axoniq.axonserver.cluster.snapshot.FakeSnapshotManager;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.Role;
import org.junit.*;
import reactor.core.publisher.Mono;

import java.util.Collections;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Sara Pellegrini
 * @since 4.1
 */
public class LeaderStateConfirmatoryElectionTest {

    private FakeRaftConfiguration raftConfiguration;
    private RaftGroup raftGroup;
    private FakeTransitionHandler transitionHandler;
    private FakeScheduler scheduler;
    private ElectionStore electionStore;
    private MajorityMatchStrategy matchStrategy;
    private RaftNode localNode;

    @Before
    public void setUp() throws Exception {
        raftConfiguration = new FakeRaftConfiguration("defaultGroup", "node0");
        transitionHandler = new FakeTransitionHandler();
        raftGroup = mock(RaftGroup.class);
        electionStore = new InMemoryElectionStore();
        electionStore.updateCurrentTerm(1);
        when(raftGroup.localLogEntryStore()).thenReturn(new InMemoryLogEntryStore("Test"));
        when(raftGroup.localElectionStore()).thenReturn(electionStore);
        when(raftGroup.logEntryProcessor()).thenReturn(mock(LogEntryProcessor.class));
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
                                                  () -> raftGroup.minActiveBackups());
    }

    private void addClusterNode(String nodeId, FakeRaftPeer peer){
        Node node = Node.newBuilder().setNodeId(nodeId).build();
        raftConfiguration.addNode(node);
        when(raftGroup.peer(node)).thenReturn(peer);
        peer.setTerm(2);
    }

    @Test
    public void testLeaderNotConfirmed() {

        LeaderState leaderState = LeaderState.builder()
                                             .raftGroup(raftGroup)
                                             .transitionHandler(transitionHandler)
                                             .termUpdateHandler((term, cause) -> electionStore.updateCurrentTerm(term))
                                             .electionFactory((disruptLeader) -> () -> Mono.just(lost()))
                                             .schedulerFactory(() -> scheduler)
                                             .snapshotManager(new FakeSnapshotManager())
                                             .stateFactory(new FakeStateFactory())
                                             .matchStrategy(matchStrategy)
                                             .build();

        leaderState.start();
        when(localNode.replicatorPeers()).thenReturn(leaderState.replicatorPeers());
        scheduler.timeElapses(50);
        assertEquals("follower", ((FakeStateFactory.FakeState)transitionHandler.lastTransition()).name());
    }

    Election.Result won(){
        return new Election.Result() {
            @Override
            public boolean won() {
                return true;
            }

            @Override
            public String cause() {
                return "Test case: election won!";
            }
        };
    }

    Election.Result lost(){
        return new Election.Result() {
            @Override
            public boolean won() {
                return false;
            }

            @Override
            public String cause() {
                return "Test case: election lost!";
            }
        };
    }
}