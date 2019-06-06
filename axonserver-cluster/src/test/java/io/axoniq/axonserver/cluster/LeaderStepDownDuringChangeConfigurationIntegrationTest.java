package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.election.InMemoryElectionStore;
import io.axoniq.axonserver.cluster.replication.EntryIterator;
import io.axoniq.axonserver.cluster.replication.LogEntryStore;
import io.axoniq.axonserver.cluster.scheduler.DefaultScheduler;
import io.axoniq.axonserver.cluster.snapshot.SnapshotManager;
import io.axoniq.axonserver.grpc.cluster.ConfigChangeResult;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.cluster.LeaderElected;
import io.axoniq.axonserver.grpc.cluster.Node;
import org.junit.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static io.axoniq.axonserver.cluster.TestUtils.assertWithin;
import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.*;

/**
 * @author Sara Pellegrini
 * @since 4.1.5
 */
public class LeaderStepDownDuringChangeConfigurationIntegrationTest {

    private RaftNode raftNode;

    @Before
    public void setUp() {
        Entry lastEntry = Entry.newBuilder().setIndex(100).setTerm(0).build();
        Entry leaderElected = Entry.newBuilder().setIndex(101).setTerm(1).build();
        LogEntryStore logEntryStore = mock(LogEntryStore.class);
        when(logEntryStore.lastLog()).thenReturn(new TermIndex(0, 100));
        when(logEntryStore.lastLogIndex()).thenReturn(100L);
        when(logEntryStore.createEntry(anyLong(), any(LeaderElected.class))).thenReturn(completedFuture(leaderElected));
        when(logEntryStore.getEntry(100)).thenReturn(lastEntry);

        RaftConfiguration raftConfiguration = mock(RaftConfiguration.class);
        when(raftConfiguration.isLogCompactionEnabled()).thenReturn(true);
        when(raftConfiguration.minElectionTimeout()).thenReturn(150);
        when(raftConfiguration.maxElectionTimeout()).thenReturn(300);
        when(raftConfiguration.maxReplicationRound()).thenReturn(10);
        when(raftConfiguration.groupId()).thenReturn("myGroupId");
        RaftGroup raftGroup = mock(RaftGroup.class);
        when(raftGroup.createIterator()).thenReturn(mock(EntryIterator.class));
        when(raftGroup.raftConfiguration()).thenReturn(raftConfiguration);
        when(raftGroup.localLogEntryStore()).thenReturn(logEntryStore);
        when(raftGroup.logEntryProcessor()).thenReturn(new LogEntryProcessor(new InMemoryProcessorStore()));
        when(raftGroup.localElectionStore()).thenReturn(new InMemoryElectionStore());
        RaftPeer raftPeer = mock(RaftPeer.class);
        when(raftPeer.nodeId()).thenReturn(newNode().getNodeId());
        when(raftGroup.peer(newNode())).thenReturn(raftPeer);

        raftNode = new RaftNode("myNode", raftGroup, new DefaultScheduler(), mock(SnapshotManager.class));
        when(raftGroup.localNode()).thenReturn(raftNode);
        when(raftConfiguration.groupMembers()).thenReturn(singletonList(Node.newBuilder().setNodeId("myNode").build()));
    }

    @Test
    public void testLeaderStepDownWhenAddingNewNode() throws InterruptedException, ExecutionException {
        raftNode.start();
        assertWithin(3, TimeUnit.SECONDS, () -> assertTrue(raftNode.isLeader()));
        CompletableFuture<ConfigChangeResult> result = raftNode.addNode(newNode());
        Thread.sleep(1000);
        raftNode.stepdown();
        ConfigChangeResult configChangeResult = result.get();
        assertTrue(configChangeResult.hasFailure());
        assertEquals("AXONIQ-10004", configChangeResult.getFailure().getError().getCode());
    }

    private Node newNode() {
        return Node.newBuilder().setHost("host").setPort(123).setNodeId("otherNode").build();
    }
}