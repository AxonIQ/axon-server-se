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
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import org.junit.*;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.UUID;
import java.util.function.BiConsumer;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyLong;
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
    private StateTransitionHandler transitionHandler;
    private FakeScheduler fakeScheduler;
    private FollowerState followerState;
    private LogEntryStore logEntryStore;
    private ElectionStore electionStore;
    private RaftConfiguration raftConfiguration;
    private SnapshotManager snapshotManager;
    private LogEntryProcessor logEntryProcessor;

    @Before
    public void setup() {
        transitionHandler = mock(StateTransitionHandler.class);

        logEntryStore = spy(new InMemoryLogEntryStore("Test"));
        electionStore = spy(new InMemoryElectionStore());
        logEntryProcessor = spy(new LogEntryProcessor(new InMemoryProcessorStore()));

        BiConsumer<Long, String> termUpdateHandler =
                (term, cause) -> electionStore.updateCurrentTerm(Math.max(term, electionStore.currentTerm()));

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
        when(snapshotManager.applySnapshotData(anyList())).thenReturn(Mono.empty());

        CurrentConfiguration currentConfiguration = mock(CurrentConfiguration.class);
        when(currentConfiguration.groupMembers()).thenReturn(asList(node("node1"),
                                                                    node("node2"),
                                                                    node("node3")));

        followerState = spy(FollowerState.builder()
                                         .transitionHandler(transitionHandler)
                                         .termUpdateHandler(termUpdateHandler)
                                         .raftGroup(raftGroup)
                                         .schedulerFactory(() -> fakeScheduler)
                                         .randomValueSupplier((min, max) -> electionTimeout)
                                         .snapshotManager(snapshotManager)
                                         .currentConfiguration(currentConfiguration)
                                         .registerConfigurationListenerFn(l -> () -> {
                                         })
                                         .stateFactory(new DefaultStateFactory(raftGroup,
                                                                               transitionHandler,
                                                                               termUpdateHandler,
                                                                               snapshotManager))
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
        verify(transitionHandler).updateState(any(), any(CandidateState.class), any());
    }

    @Test
    public void testRequestVoteGranted() {
        RequestVoteResponse response = followerState.requestVote(RequestVoteRequest.newBuilder()
                                                                                   .setCandidateId("node1")
                                                                                   .setGroupId("defaultGroup")
                                                                                   .setTerm(1)
                                                                                   .build());
        assertTrue(response.getVoteGranted());
        assertEquals(1L, response.getTerm());
        assertEquals("defaultGroup", response.getGroupId());
        assertFalse(response.getGoAway());
    }

    @Test
    public void testRequestVoteGrantedAfterAppendAndAfterMinElectionTimeoutHasPassed() {
        followerState.appendEntries(firstAppend());

        // wait min election timeout to pass in order to have vote granted
        fakeScheduler.timeElapses(MIN_ELECTION_TIMEOUT + 1);

        RequestVoteResponse response = followerState.requestVote(RequestVoteRequest.newBuilder()
                                                                                   .setRequestId(UUID.randomUUID()
                                                                                                     .toString())
                                                                                   .setCandidateId("node2")
                                                                                   .setGroupId("defaultGroup")
                                                                                   .setLastLogTerm(0L)
                                                                                   .setLastLogIndex(1L)
                                                                                   .setTerm(1)
                                                                                   .build());

        assertTrue(response.getVoteGranted());
        assertEquals(1L, response.getTerm());
        assertEquals("defaultGroup", response.getGroupId());
        assertFalse(response.getGoAway());
    }

    @Test
    public void testRequestVoteNotGrantedAfterAppendAndMinElectionTimeoutHasNotPassed() {
        followerState.appendEntries(firstAppend());

        fakeScheduler.timeElapses(MIN_ELECTION_TIMEOUT - 1);

        RequestVoteResponse response = followerState.requestVote(RequestVoteRequest.newBuilder()
                                                                                   .setRequestId(UUID.randomUUID()
                                                                                                     .toString())
                                                                                   .setGroupId("defaultGroup")
                                                                                   .setLastLogTerm(0L)
                                                                                   .setLastLogIndex(1L)
                                                                                   .setTerm(1)
                                                                                   .build());

        assertFalse(response.getVoteGranted());
        assertEquals(0L, response.getTerm());
        assertEquals("defaultGroup", response.getGroupId());
        assertFalse(response.getGoAway());
    }

    @Test
    public void testRequestVoteNotGrantedAfterMinElectionTimeoutHasPassedAndLogIsNotUpToDate() {
        followerState.appendEntries(firstAppend());

        // wait min election timeout to pass in order to have vote granted
        fakeScheduler.timeElapses(MIN_ELECTION_TIMEOUT + 1);

        RequestVoteResponse response = followerState.requestVote(RequestVoteRequest.newBuilder()
                                                                                   .setRequestId(UUID.randomUUID()
                                                                                                     .toString())
                                                                                   .setCandidateId("node3")
                                                                                   .setGroupId("defaultGroup")
                                                                                   .setLastLogTerm(0L)
                                                                                   .setLastLogIndex(0L)
                                                                                   .setTerm(1)
                                                                                   .build());

        assertFalse(response.getVoteGranted());
        assertEquals(1L, response.getTerm());
        assertEquals("defaultGroup", response.getGroupId());
        assertFalse(response.getGoAway());
    }

    @Test
    public void testRequestVoteNotGrantedAfterMinElectionTimeoutHasPassedAndTermIsOld() {
        followerState.appendEntries(firstAppend(1L));

        // wait min election timeout to pass in order to have vote granted
        fakeScheduler.timeElapses(MIN_ELECTION_TIMEOUT + 1);

        RequestVoteResponse response = followerState.requestVote(RequestVoteRequest.newBuilder()
                                                                                   .setRequestId(UUID.randomUUID()
                                                                                                     .toString())
                                                                                   .setCandidateId("node1")
                                                                                   .setGroupId("defaultGroup")
                                                                                   .setLastLogTerm(0L)
                                                                                   .setLastLogIndex(2L)
                                                                                   .setTerm(0L)
                                                                                   .build());

        assertFalse(response.getVoteGranted());
        assertEquals(1L, response.getTerm());
        assertEquals("defaultGroup", response.getGroupId());
        assertFalse(response.getGoAway());
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
                                                           .setRequestId(UUID.randomUUID().toString())
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
    public void testAppendSuccessAfterSnapshotInstalled() {
        InstallSnapshotRequest installSnapshotRequest = InstallSnapshotRequest.newBuilder()
                                                                              .setRequestId(UUID.randomUUID()
                                                                                                .toString())
                                                                              .setLeaderId("node1")
                                                                              .setGroupId("defaultGroup")
                                                                              .setTerm(0L)
                                                                              .setOffset(0)
                                                                              .setDone(true)
                                                                              .setLastIncludedIndex(2L)
                                                                              .setLastIncludedTerm(3L)
                                                                              .setLastConfig(Config.newBuilder()
                                                                                                   .build())
                                                                              .addData(SerializedObject.newBuilder()
                                                                                                       .build())
                                                                              .build();
        followerState.installSnapshot(installSnapshotRequest);
        AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                                                           .setRequestId(UUID.randomUUID().toString())
                                                           .setTerm(3L)
                                                           .setCommitIndex(1L)
                                                           .setPrevLogIndex(2L)
                                                           .setPrevLogTerm(3L)
                                                           .setLeaderId("node1")
                                                           .setGroupId("defaultGroup")
                                                           .addEntries(Entry.newBuilder()
                                                                            .setIndex(3L)
                                                                            .setTerm(3L)
                                                                            .build())
                                                           .build();

        AppendEntriesResponse response = followerState.appendEntries(request);
        assertTrue(response.hasSuccess());
        assertEquals(3, response.getSuccess().getLastLogIndex());
        assertEquals(3, response.getTerm());
    }

    @Test
    public void testAppendFailsDueToIOException() throws IOException {
        doThrow(new IOException("oops")).when(logEntryStore).appendEntry(any());

        AppendEntriesResponse response = followerState.appendEntries(firstAppend());

//        assertEquals("defaultGroup", response.getGroupId());
//        assertEquals(0L, response.getTerm());
//        assertEquals(LAST_APPLIED_EVENT_SEQUENCE, response.getFailure().getLastAppliedEventSequence());
//        assertEquals(0L, response.getFailure().getLastAppliedIndex());
//        assertEquals(0L, logEntryProcessor.commitIndex());
//        verify(followerState).stop();
    }

    @Test
    public void testCommitIndexUpdated() {
        followerState.appendEntries(firstAppend());

        AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                                                           .setRequestId(UUID.randomUUID().toString())
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
                                                               .setRequestId(UUID.randomUUID().toString())
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
        verify(logEntryStore).clear(anyLong());
        verify(raftConfiguration).update(request.getLastConfig().getNodesList());
        verify(snapshotManager).applySnapshotData(request.getDataList());

        fakeScheduler.timeElapses(electionTimeout + 1);
        verify(transitionHandler).updateState(any(), any(CandidateState.class), any());
    }

    @Test
    public void testApplyingLastUpdatedIndexOnLastSnapshotChunk() {
        InstallSnapshotRequest request = InstallSnapshotRequest.newBuilder()
                                                               .setRequestId(UUID.randomUUID().toString())
                                                               .setLeaderId("node1")
                                                               .setGroupId("defaultGroup")
                                                               .setTerm(0L)
                                                               .setOffset(0)
                                                               .setLastIncludedIndex(2L)
                                                               .setLastIncludedTerm(0L)
                                                               .setDone(true)
                                                               .build();

        InstallSnapshotResponse response = followerState.installSnapshot(request);

        assertEquals("defaultGroup", response.getGroupId());
        assertEquals(0L, response.getTerm());
        assertTrue(response.hasSuccess());
        verify(snapshotManager).applySnapshotData(request.getDataList());
        verify(logEntryProcessor).updateLastApplied(2L, 0L);
        verify(logEntryProcessor).markCommitted(2L, 0L);
    }

    private AppendEntriesRequest firstAppend() {
        return firstAppend(0L);
    }

    private AppendEntriesRequest firstAppend(long term) {
        return AppendEntriesRequest.newBuilder()
                                   .setRequestId(UUID.randomUUID().toString())
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

    private Node node(String id) {
        return Node.newBuilder()
                   .setNodeId(id)
                   .build();
    }

    @Test
    public void installSnapshotMissingChunk() {
        InstallSnapshotRequest zero = InstallSnapshotRequest.newBuilder()
                                                            .setRequestId(UUID.randomUUID().toString())
                                                            .setLeaderId("node1")
                                                            .setGroupId("defaultGroup")
                                                            .setTerm(0L)
                                                            .setOffset(0)
                                                            .setLastIncludedIndex(2L)
                                                            .setLastIncludedTerm(0L)
                                                            .setDone(false)
                                                            .build();
        assertTrue(followerState.installSnapshot(zero).hasSuccess());
        InstallSnapshotRequest one = InstallSnapshotRequest.newBuilder(zero).setOffset(1).build();
        assertTrue(followerState.installSnapshot(one).hasSuccess());
        InstallSnapshotRequest three = InstallSnapshotRequest.newBuilder(zero).setOffset(3).build();
        InstallSnapshotResponse response = followerState.installSnapshot(three);
        assertEquals("defaultGroup", response.getGroupId());
        assertTrue(response.hasFailure());
    }

    @Test
    public void installSnapshotInterrupted() {
        InstallSnapshotRequest zero = InstallSnapshotRequest.newBuilder()
                                                            .setRequestId(UUID.randomUUID().toString())
                                                            .setLeaderId("node1")
                                                            .setGroupId("defaultGroup")
                                                            .setTerm(0L)
                                                            .setOffset(0)
                                                            .setLastIncludedIndex(2L)
                                                            .setLastIncludedTerm(0L)
                                                            .setDone(false)
                                                            .build();
        assertTrue(followerState.installSnapshot(zero).hasSuccess());
        InstallSnapshotRequest one = InstallSnapshotRequest.newBuilder(zero).setOffset(1).build();
        assertTrue(followerState.installSnapshot(one).hasSuccess());

        //the leader has been disrupted, and start again from the first chuck
        assertTrue(followerState.installSnapshot(zero).hasSuccess());

        //the chuck "two" must fail because a new install snapshot is started
        InstallSnapshotRequest two = InstallSnapshotRequest.newBuilder(zero).setOffset(2).build();
        InstallSnapshotResponse response = followerState.installSnapshot(two);

        assertEquals("defaultGroup", response.getGroupId());
        assertTrue(response.hasFailure());
    }

    @Test
    public void installSnapshotCompleted() {
        InstallSnapshotRequest zero = InstallSnapshotRequest.newBuilder()
                                                            .setRequestId(UUID.randomUUID().toString())
                                                            .setLeaderId("node1")
                                                            .setGroupId("defaultGroup")
                                                            .setTerm(0L)
                                                            .setOffset(0)
                                                            .setLastIncludedIndex(2L)
                                                            .setLastIncludedTerm(0L)
                                                            .setDone(false)
                                                            .build();
        assertTrue(followerState.installSnapshot(zero).hasSuccess());
        InstallSnapshotRequest one = InstallSnapshotRequest.newBuilder(zero).setOffset(1).build();
        assertTrue(followerState.installSnapshot(one).hasSuccess());
        InstallSnapshotRequest two = InstallSnapshotRequest.newBuilder(zero).setOffset(2).setDone(true).build();
        assertTrue(followerState.installSnapshot(two).hasSuccess());

        InstallSnapshotRequest three = InstallSnapshotRequest.newBuilder(zero).setOffset(3).build();
        InstallSnapshotResponse response = followerState.installSnapshot(three);

        //the chuck "three" must fail because install snapshot is completed with chunk "two"
        assertEquals("defaultGroup", response.getGroupId());
        assertTrue(response.hasFailure());
    }
}
