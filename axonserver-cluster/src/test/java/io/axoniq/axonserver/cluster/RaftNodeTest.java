package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.election.InMemoryElectionStore;
import io.axoniq.axonserver.cluster.replication.EntryIterator;
import io.axoniq.axonserver.cluster.replication.LogEntryStore;
import io.axoniq.axonserver.cluster.replication.file.FileSegmentLogEntryStore;
import io.axoniq.axonserver.cluster.replication.file.PrimaryEventStoreFactory;
import io.axoniq.axonserver.cluster.replication.file.PrimaryLogEntryStore;
import io.axoniq.axonserver.cluster.snapshot.SnapshotManager;
import io.axoniq.axonserver.grpc.cluster.Node;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link RaftNode}.
 *
 * @author Sara Pellegrini
 * @since 4.1
 */
public class RaftNodeTest {

    private RaftNode testSubject;

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    @Ignore
    public void rescheduleLogCompaction() {
        SnapshotManager snapshotManager = mock(SnapshotManager.class);
        LogEntryStore logEntryStore = mock(LogEntryStore.class);
        when(logEntryStore.lastLog()).thenReturn(new TermIndex(0,0));
        when(logEntryStore.createIterator(anyLong())).thenReturn(mock(EntryIterator.class));
        RaftConfiguration raftConfiguration = mock(RaftConfiguration.class);
        when(raftConfiguration.isLogCompactionEnabled()).thenReturn(true);
        when(raftConfiguration.minElectionTimeout()).thenReturn(150);
        when(raftConfiguration.maxElectionTimeout()).thenReturn(300);
        when(raftConfiguration.groupId()).thenReturn("myGroupId");
        RaftGroup raftGroup = mock(RaftGroup.class);
        when(raftGroup.raftConfiguration()).thenReturn(raftConfiguration);
        when(raftGroup.localLogEntryStore()).thenReturn(logEntryStore);
        when(raftGroup.logEntryProcessor()).thenReturn(new LogEntryProcessor(new InMemoryProcessorStore()));
        when(raftGroup.localElectionStore()).thenReturn(new InMemoryElectionStore());
        FakeScheduler scheduler = new FakeScheduler();
        testSubject = new RaftNode("myNode", raftGroup, scheduler, snapshotManager);
        when(raftGroup.localNode()).thenReturn(testSubject);
        when(raftConfiguration.groupMembers()).thenReturn(asList(Node.newBuilder().setNodeId("myNode").build()));

        testSubject.start();
        scheduler.timeElapses(115, TimeUnit.MINUTES);
        verify(logEntryStore, times(1)).clearOlderThan(anyLong(),any(),any());
        testSubject.restartLogCleaning();
        scheduler.timeElapses(55, TimeUnit.MINUTES);
        verify(logEntryStore, times(1)).clearOlderThan(anyLong(),any(),any());
        testSubject.stop();
    }


    @Test
    public void gracefulShutdown() {
        SnapshotManager snapshotManager = mock(SnapshotManager.class);

        PrimaryLogEntryStore primaryLogEntryStore = PrimaryEventStoreFactory.create(tempFolder.getRoot().getAbsolutePath() + "/" + UUID.randomUUID().toString());
        LogEntryStore logEntryStore = new FileSegmentLogEntryStore("test", primaryLogEntryStore, () -> 0L);

        RaftConfiguration raftConfiguration = mock(RaftConfiguration.class);
        when(raftConfiguration.maxElectionTimeout()).thenReturn(100);
        RaftGroup raftGroup = mock(RaftGroup.class);
        when(raftGroup.raftConfiguration()).thenReturn(raftConfiguration);
        when(raftGroup.localLogEntryStore()).thenReturn(logEntryStore);
        when(raftGroup.logEntryProcessor()).thenReturn(new LogEntryProcessor(new InMemoryProcessorStore()));
        when(raftGroup.localElectionStore()).thenReturn(new InMemoryElectionStore());

        FakeScheduler scheduler = new FakeScheduler();
        testSubject = new RaftNode("myNode", raftGroup, scheduler, snapshotManager);
        when(raftGroup.localNode()).thenReturn(testSubject);

        testSubject.start();

        assertFalse(primaryLogEntryStore.isClosed());

        testSubject.stop();

        assertTrue(primaryLogEntryStore.isClosed());

    }

}