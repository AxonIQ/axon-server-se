package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.election.InMemoryElectionStore;
import io.axoniq.axonserver.cluster.replication.InMemoryLogEntryStore;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import org.junit.*;

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

    private Consumer<MembershipState> transitionHandler;
    private RaftConfiguration raftConfiguration;
    private RaftGroup raftGroup;
    private FakeScheduler fakeScheduler;
    private FollowerState followerState;

    @Before
    public void setup() {
        transitionHandler = mock(Consumer.class);

        raftConfiguration = mock(RaftConfiguration.class);
        when(raftConfiguration.groupId()).thenReturn("defaultGroup");
        when(raftConfiguration.minElectionTimeout()).thenReturn(150L);
        when(raftConfiguration.maxElectionTimeout()).thenReturn(300L);

        raftGroup = mock(RaftGroup.class);
        when(raftGroup.lastAppliedEventSequence()).thenReturn(2L);
        when(raftGroup.localLogEntryStore()).thenReturn(new InMemoryLogEntryStore());
        when(raftGroup.localElectionStore()).thenReturn(new InMemoryElectionStore());
        when(raftGroup.raftConfiguration()).thenReturn(raftConfiguration);

        fakeScheduler = new FakeScheduler();

        followerState = FollowerState.builder()
                                     .transitionHandler(transitionHandler)
                                     .raftGroup(raftGroup)
                                     .scheduler(fakeScheduler)
                                     .stateFactory(new DefaultStateFactory(raftGroup, transitionHandler))
                                     .build();
        followerState.start();
    }

    @Test
    public void testTransitionToCandidateState() {
        long firstSchedule = fakeScheduler.nextSchedule().toEpochMilli();
        long untilFirstSchedule = firstSchedule - fakeScheduler.getCurrentTime().toEpochMilli();
        fakeScheduler.timeElapses(untilFirstSchedule + 1);
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
        fakeScheduler.timeElapses(151);

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
        fakeScheduler.timeElapses(151);

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

    private AppendEntriesRequest firstAppend() {
        return AppendEntriesRequest.newBuilder()
                                   .setTerm(0L)
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
