package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.election.ElectionStore;
import io.axoniq.axonserver.cluster.election.InMemoryElectionStore;
import io.axoniq.axonserver.cluster.replication.InMemoryLogEntryStore;
import io.axoniq.axonserver.cluster.replication.LogEntryStore;
import io.axoniq.axonserver.cluster.snapshot.SnapshotManager;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.Config;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import org.junit.*;

import java.io.IOException;
import java.util.function.Consumer;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link FollowerState}.
 *
 * @author Milan Savic
 */
public class FollowerStateTest {

    private static final int MIN_ELECTION_TIMEOUT = 150;
    private static final int MAX_ELECTION_TIMEOUT = 300;
    private static final long LAST_APPLIED_EVENT_SEQUENCE = 2L;

    private int electionTimeout = 160;
    private Consumer<MembershipState> transitionHandler;
    private FakeScheduler fakeScheduler;
    private FollowerState followerState;
    private LogEntryStore logEntryStore;
    private ElectionStore electionStore;
    private RaftConfiguration raftConfiguration;
    private SnapshotManager snapshotManager;
    private LogEntryProcessor logEntryProcessor;

    @Before
    public void setup() {
        transitionHandler = mock(Consumer.class);

        logEntryStore = spy(new InMemoryLogEntryStore("Test"));
        electionStore = spy(new InMemoryElectionStore());
        logEntryProcessor = spy( new LogEntryProcessor(new InMemoryProcessorStore()));

        raftConfiguration = mock(RaftConfiguration.class);
        when(raftConfiguration.groupId()).thenReturn("defaultGroup");
        when(raftConfiguration.minElectionTimeout()).thenReturn(MIN_ELECTION_TIMEOUT);
        when(raftConfiguration.maxElectionTimeout()).thenReturn(MAX_ELECTION_TIMEOUT);

        RaftGroup raftGroup = mock(RaftGroup.class);
        when(raftGroup.lastAppliedEventSequence()).thenReturn(LAST_APPLIED_EVENT_SEQUENCE);
        when(raftGroup.localLogEntryStore()).thenReturn(logEntryStore);
        when(raftGroup.localElectionStore()).thenReturn(electionStore);
        when(raftGroup.logEntryProcessor()).thenReturn(logEntryProcessor);
        when(raftGroup.raftConfiguration()).thenReturn(raftConfiguration);
        RaftNode localNode = mock(RaftNode.class);
        when(localNode.nodeId()).thenReturn("mockNode");
        when(raftGroup.localNode()).thenReturn(localNode);

        fakeScheduler = new FakeScheduler();

        snapshotManager = mock(SnapshotManager.class);

        followerState = spy(FollowerState.builder()
                                         .transitionHandler(transitionHandler)
                                         .raftGroup(raftGroup)
                                         .scheduler(fakeScheduler)
                                         .randomValueSupplier((min, max) -> electionTimeout)
                                         .snapshotManager(snapshotManager)
                                         .stateFactory(new DefaultStateFactory(raftGroup, transitionHandler))
                                         .build());
        followerState.start();
    }

    @After
    public void tearDown() {
        followerState.stop();
    }

    @Test
    public void testTransitionToCandidateState() {
        fakeScheduler.timeElapses(electionTimeout + 1);
        verify(transitionHandler).accept(any(CandidateState.class));
    }

    @Test
    public void testRequestVoteGranted() {
        RequestVoteResponse response = followerState.requestVote(RequestVoteRequest.newBuilder()
                                                                                   .setGroupId("defaultGroup")
                                                                                   .setTerm(1)
                                                                                   .build());
        assertTrue(response.getVoteGranted());
        assertEquals(1L, response.getTerm());
        assertEquals("defaultGroup", response.getGroupId());
    }

    @Test
    public void testRequestVoteGrantedAfterAppendAndAfterMinElectionTimeoutHasPassed() {
        followerState.appendEntries(firstAppend());

        // wait min election timeout to pass in order to have vote granted
        fakeScheduler.timeElapses(MIN_ELECTION_TIMEOUT + 1);

        RequestVoteResponse response = followerState.requestVote(RequestVoteRequest.newBuilder()
                                                                                   .setGroupId("defaultGroup")
                                                                                   .setLastLogTerm(0L)
                                                                                   .setLastLogIndex(1L)
                                                                                   .setTerm(1)
                                                                                   .build());

        assertTrue(response.getVoteGranted());
        assertEquals(1L, response.getTerm());
        assertEquals("defaultGroup", response.getGroupId());
    }

    @Test
    public void testRequestVoteNotGrantedAfterAppendAndMinElectionTimeoutHasNotPassed() {
        followerState.appendEntries(firstAppend());

        fakeScheduler.timeElapses(MIN_ELECTION_TIMEOUT - 1);

        RequestVoteResponse response = followerState.requestVote(RequestVoteRequest.newBuilder()
                                                                                   .setGroupId("defaultGroup")
                                                                                   .setLastLogTerm(0L)
                                                                                   .setLastLogIndex(1L)
                                                                                   .setTerm(1)
                                                                                   .build());

        assertFalse(response.getVoteGranted());
        assertEquals(0L, response.getTerm());
        assertEquals("defaultGroup", response.getGroupId());
    }

    @Test
    public void testRequestVoteNotGrantedAfterMinElectionTimeoutHasPassedAndLogIsNotUpToDate() {
        followerState.appendEntries(firstAppend());

        // wait min election timeout to pass in order to have vote granted
        fakeScheduler.timeElapses(MIN_ELECTION_TIMEOUT + 1);

        RequestVoteResponse response = followerState.requestVote(RequestVoteRequest.newBuilder()
                                                                                   .setGroupId("defaultGroup")
                                                                                   .setLastLogTerm(0L)
                                                                                   .setLastLogIndex(0L)
                                                                                   .setTerm(1)
                                                                                   .build());

        assertFalse(response.getVoteGranted());
        assertEquals(1L, response.getTerm());
        assertEquals("defaultGroup", response.getGroupId());
    }

    @Test
    public void testRequestVoteNotGrantedAfterMinElectionTimeoutHasPassedAndTermIsOld() {
        followerState.appendEntries(firstAppend(1L));

        // wait min election timeout to pass in order to have vote granted
        fakeScheduler.timeElapses(MIN_ELECTION_TIMEOUT + 1);

        RequestVoteResponse response = followerState.requestVote(RequestVoteRequest.newBuilder()
                                                                                   .setGroupId("defaultGroup")
                                                                                   .setLastLogTerm(0L)
                                                                                   .setLastLogIndex(2L)
                                                                                   .setTerm(0L)
                                                                                   .build());

        assertFalse(response.getVoteGranted());
        assertEquals(1L, response.getTerm());
        assertEquals("defaultGroup", response.getGroupId());
    }

    @Test
    public void testFirstAppend() throws IOException {
        AppendEntriesRequest request = firstAppend();
        AppendEntriesResponse response = followerState.appendEntries(request);

        assertEquals("defaultGroup", response.getGroupId());
        assertEquals(0L, response.getTerm());
        assertEquals(1L, response.getSuccess().getLastLogIndex());
        verify(logEntryStore).appendEntry(request.getEntriesList());
    }

    @Test
    public void testAppendFailsDueToOldTerm() throws IOException {
        when(electionStore.currentTerm()).thenReturn(1L);

        AppendEntriesResponse response = followerState.appendEntries(firstAppend());

        assertEquals("defaultGroup", response.getGroupId());
        assertEquals(1L, response.getTerm());
        assertEquals(LAST_APPLIED_EVENT_SEQUENCE, response.getFailure().getLastAppliedEventSequence());
        assertEquals(0L, response.getFailure().getLastAppliedIndex());
        verify(logEntryStore, times(0)).appendEntry(any());
    }

    @Test
    public void testAppendFailsDueToNonMatchingPrevLogs() throws IOException {
        AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                                                           .setTerm(0L)
                                                           .setCommitIndex(1L)
                                                           .setPrevLogIndex(1L)
                                                           .setPrevLogTerm(0L)
                                                           .setLeaderId("node1")
                                                           .setGroupId("defaultGroup")
                                                           .addEntries(Entry.newBuilder()
                                                                            .setIndex(2L)
                                                                            .setTerm(0L)
                                                                            .build())
                                                           .build();

        AppendEntriesResponse response = followerState.appendEntries(request);

        assertEquals("defaultGroup", response.getGroupId());
        assertEquals(0L, response.getTerm());
        assertEquals(LAST_APPLIED_EVENT_SEQUENCE, response.getFailure().getLastAppliedEventSequence());
        assertEquals(0L, response.getFailure().getLastAppliedIndex());
        verify(logEntryStore, times(0)).appendEntry(any());
        assertEquals(0L, logEntryProcessor.commitIndex());
    }

    @Test
    public void testAppendFailsDueToIOException() throws IOException {
        doThrow(new IOException("oops")).when(logEntryStore).appendEntry(any());

        AppendEntriesResponse response = followerState.appendEntries(firstAppend());

        assertEquals("defaultGroup", response.getGroupId());
        assertEquals(0L, response.getTerm());
        assertEquals(LAST_APPLIED_EVENT_SEQUENCE, response.getFailure().getLastAppliedEventSequence());
        assertEquals(0L, response.getFailure().getLastAppliedIndex());
        assertEquals(0L, logEntryProcessor.commitIndex());
        verify(followerState).stop();
    }

    @Test
    public void testCommitIndexUpdated() {
        followerState.appendEntries(firstAppend());

        AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                                                           .setTerm(0L)
                                                           .setCommitIndex(1L)
                                                           .setPrevLogIndex(1L)
                                                           .setPrevLogTerm(0L)
                                                           .setLeaderId("node1")
                                                           .setGroupId("defaultGroup")
                                                           .addEntries(Entry.newBuilder()
                                                                            .setIndex(2L)
                                                                            .setTerm(0L)
                                                                            .build())
                                                           .build();

        AppendEntriesResponse response = followerState.appendEntries(request);
        assertEquals("defaultGroup", response.getGroupId());
        assertEquals(0L, response.getTerm());
        assertEquals(2L, response.getSuccess().getLastLogIndex());
        assertEquals(1L, logEntryProcessor.commitIndex());
    }

    @Test
    public void testInstallSnapshotFailWhenTermIsOld() {
        when(electionStore.currentTerm()).thenReturn(1L);

        InstallSnapshotResponse response = followerState
                .installSnapshot(InstallSnapshotRequest.newBuilder()
                                                       .setLeaderId("node1")
                                                       .setGroupId("defaultGroup")
                                                       .setTerm(0L)
                                                       .build());

        assertEquals("defaultGroup", response.getGroupId());
        assertEquals(1L, response.getTerm());
        assertTrue(response.hasFailure());
    }

    @Test
    public void testSuccessfulInstallSnapshot() {
        InstallSnapshotRequest request = InstallSnapshotRequest.newBuilder()
                                                               .setLeaderId("node1")
                                                               .setGroupId("defaultGroup")
                                                               .setTerm(0L)
                                                               .setOffset(0)
                                                               .setLastIncludedIndex(2L)
                                                               .setLastIncludedTerm(0L)
                                                               .setLastConfig(Config.newBuilder().build())
                                                               .addData(SerializedObject.newBuilder().build())
                                                               .build();

        InstallSnapshotResponse response = followerState.installSnapshot(request);

        assertEquals("defaultGroup", response.getGroupId());
        assertEquals(0L, response.getTerm());
        assertTrue(response.hasSuccess());
        verify(logEntryStore).clear(2L, 0L);
        verify(raftConfiguration).update(request.getLastConfig().getNodesList());
        verify(snapshotManager).applySnapshotData(request.getDataList());

        fakeScheduler.timeElapses(electionTimeout + 1);
        verify(transitionHandler).accept(any(CandidateState.class));
    }

    private AppendEntriesRequest firstAppend() {
        return firstAppend(0L);
    }

    private AppendEntriesRequest firstAppend(long term) {
        return AppendEntriesRequest.newBuilder()
                                   .setTerm(term)
                                   .setCommitIndex(0L)
                                   .setPrevLogIndex(0L)
                                   .setPrevLogTerm(0L)
                                   .setLeaderId("node1")
                                   .setGroupId("defaultGroup")
                                   .addEntries(Entry.newBuilder()
                                                    .setIndex(1L)
                                                    .setTerm(0L)
                                                    .build())
                                   .build();
    }
}
