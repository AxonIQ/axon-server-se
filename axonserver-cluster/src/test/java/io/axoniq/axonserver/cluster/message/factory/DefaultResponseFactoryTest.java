package io.axoniq.axonserver.cluster.message.factory;

import io.axoniq.axonserver.cluster.LogEntryProcessor;
import io.axoniq.axonserver.cluster.RaftConfiguration;
import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.cluster.election.ElectionStore;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import org.junit.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link DefaultResponseFactory}
 *
 * @author Sara Pellegrini
 * @since 4.1.5
 */
public class DefaultResponseFactoryTest {

    private final long currentTerm = 5L;
    private final String localNodeId = "ME";
    private final String requestId = "requestId";
    private final String failureCause = "My cause";
    private final long lastAppliedIndex = 100L;
    private final long lastAppliedEventSequence = 30L;
    private final long lastAppliedSnapshotSequence = 8L;
    private final String groupId = "My group";
    private RaftGroup raftGroup = mock(RaftGroup.class);
    private final DefaultResponseFactory testSubject = new DefaultResponseFactory(raftGroup);

    @Before
    public void setUp() {
        ElectionStore electionStore = mock(ElectionStore.class);
        when(raftGroup.localElectionStore()).thenReturn(electionStore);
        LogEntryProcessor logEntryProcessor = mock(LogEntryProcessor.class);
        when(raftGroup.logEntryProcessor()).thenReturn(logEntryProcessor);
        RaftNode localNode = mock(RaftNode.class);
        RaftConfiguration conf = mock(RaftConfiguration.class);
        when(raftGroup.raftConfiguration()).thenReturn(conf);

        when(conf.groupId()).thenReturn(groupId);
        when(raftGroup.localNode()).thenReturn(localNode);
        when(electionStore.currentTerm()).thenReturn(currentTerm);
        when(localNode.nodeId()).thenReturn(localNodeId);
        when(raftGroup.lastAppliedEventSequence()).thenReturn(lastAppliedEventSequence);
        when(raftGroup.lastAppliedSnapshotSequence()).thenReturn(lastAppliedSnapshotSequence);
        when(logEntryProcessor.lastAppliedIndex()).thenReturn(lastAppliedIndex);
    }

    @Test
    public void testAppendEntryFailure() {
        AppendEntriesResponse response = testSubject.appendEntriesFailure(requestId, failureCause);
        assertEquals(currentTerm, response.getTerm());
        assertEquals(localNodeId, response.getResponseHeader().getNodeId());
        assertEquals(requestId, response.getResponseHeader().getRequestId());
        assertEquals(failureCause, response.getFailure().getCause());
        assertEquals(lastAppliedIndex, response.getFailure().getLastAppliedIndex());
        assertEquals(lastAppliedEventSequence, response.getFailure().getLastAppliedEventSequence());
        assertEquals(lastAppliedSnapshotSequence, response.getFailure().getLastAppliedSnapshotSequence());
        assertEquals(groupId, response.getGroupId());
        assertFalse(response.hasSuccess());
    }


    @Test
    public void testAppendEntrySuccess() {
        AppendEntriesResponse response = testSubject.appendEntriesSuccess(requestId, 100L);
        assertEquals(currentTerm, response.getTerm());
        assertEquals(localNodeId, response.getResponseHeader().getNodeId());
        assertEquals(requestId, response.getResponseHeader().getRequestId());
        assertEquals(100L, response.getSuccess().getLastLogIndex());
        assertEquals(groupId, response.getGroupId());
        assertFalse(response.hasFailure());
    }

    @Test
    public void testRequestVoteFailure() {
        RequestVoteResponse response = testSubject.voteResponse(requestId, false, true);
        assertEquals(currentTerm, response.getTerm());
        assertEquals(localNodeId, response.getResponseHeader().getNodeId());
        assertEquals(requestId, response.getResponseHeader().getRequestId());
        assertEquals(groupId, response.getGroupId());
        assertFalse(response.getVoteGranted());
        assertTrue(response.getGoAway());
    }


    @Test
    public void testRequestVoteSuccess() {
        RequestVoteResponse response = testSubject.voteResponse(requestId, true, false);
        assertEquals(currentTerm, response.getTerm());
        assertEquals(localNodeId, response.getResponseHeader().getNodeId());
        assertEquals(requestId, response.getResponseHeader().getRequestId());
        assertEquals(groupId, response.getGroupId());
        assertFalse(response.getGoAway());
        assertTrue(response.getVoteGranted());
    }

    @Test
    public void testInstallSnapshotFailure() {
        InstallSnapshotResponse response = testSubject.installSnapshotFailure(requestId, failureCause);
        assertEquals(currentTerm, response.getTerm());
        assertEquals(localNodeId, response.getResponseHeader().getNodeId());
        assertEquals(requestId, response.getResponseHeader().getRequestId());
        assertEquals(failureCause, response.getFailure().getCause());
        assertEquals(groupId, response.getGroupId());
        assertFalse(response.hasSuccess());
    }

    @Test
    public void testInstallSnapshotSuccess() {
        InstallSnapshotResponse response = testSubject.installSnapshotSuccess(requestId, 10);
        assertEquals(currentTerm, response.getTerm());
        assertEquals(localNodeId, response.getResponseHeader().getNodeId());
        assertEquals(requestId, response.getResponseHeader().getRequestId());
        assertEquals(10, response.getSuccess().getLastReceivedOffset());
        assertEquals(groupId, response.getGroupId());
        assertFalse(response.hasFailure());
    }
}