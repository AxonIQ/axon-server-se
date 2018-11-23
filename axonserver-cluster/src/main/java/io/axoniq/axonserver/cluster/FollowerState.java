package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.Scheduler.ScheduledRegistration;
import io.axoniq.axonserver.cluster.replication.LogEntryStore;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.AppendEntrySuccess;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotSuccess;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.Math.min;

public class FollowerState extends AbstractMembershipState {

    private static final Logger logger = LoggerFactory.getLogger(FollowerState.class);

    private final AtomicReference<ScheduledRegistration> scheduledElection = new AtomicReference<>();
    private volatile boolean heardFromLeader;
    private final AtomicLong nextTimeout = new AtomicLong();
    private final AtomicLong lastMessage = new AtomicLong();
    private final Clock clock;

    protected FollowerState(Builder builder) {
        super(builder);
        clock = scheduler().clock();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void start() {
        cancelCurrentElectionTimeout();
        nextTimeout.set(clock.millis() + random(minElectionTimeout(), maxElectionTimeout()));
        scheduleNewElection();
        heardFromLeader = false;
    }

    @Override
    public void stop() {
        cancelCurrentElectionTimeout();
    }

    @Override
    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        lastMessage.set(clock.millis());
        nextTimeout.set(lastMessage.get() + random(minElectionTimeout(), maxElectionTimeout()));
        try {
            logger.trace("{}: received {}", me(), request);
            updateCurrentTerm(request.getTerm());

            //1. Reply false if term < currentTerm
            if (request.getTerm() < currentTerm()) {
                logger.warn("{}: term before current term {}", me(), currentTerm());
                return appendEntriesFailure();
            }

            heardFromLeader = true;
            //rescheduleElection(request.getTerm());
            LogEntryStore logEntryStore = raftGroup().localLogEntryStore();
            if( logger.isTraceEnabled() && request.getPrevLogIndex() == 0) {
                logger.trace("{} Received heartbeat, commitindex: {}", me(), request.getCommitIndex());
            }

            //2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
            if (!logEntryStore.contains(request.getPrevLogIndex(), request.getPrevLogTerm())) {
                logger.warn("{}: previous term/index missing {}/{} last log {}",
                            me(),
                            request.getPrevLogTerm(),
                            request.getPrevLogIndex(),
                            logEntryStore.lastLogIndex());
                return appendEntriesFailure();
            }

            //3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry
            // and all that follow it
            //4. Append any new entries not already in the log
            try {
                logEntryStore.appendEntry(request.getEntriesList());
            } catch (IOException e) {
                logger.warn("{}: append failed", me(), e);
                stop();
                return appendEntriesFailure();
            }

            //5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
            if (request.getCommitIndex() > logEntryStore.commitIndex()) {
                if (request.getEntriesCount() == 0) {
                    logEntryStore.markCommitted(request.getCommitIndex());
                } else {
                    logEntryStore.markCommitted(min(request.getCommitIndex(),
                                                    request.getEntries(request.getEntriesCount() - 1).getIndex()));
                }
            }

            logger.trace("{}: stored {}", me(), lastLogIndex());
            return AppendEntriesResponse.newBuilder()
                                        .setGroupId(groupId())
                                        .setTerm(currentTerm())
                                        .setSuccess(buildAppendEntrySuccess(lastLogIndex()))
                                        .setTerm(currentTerm())
                                        .build();
        } catch( Exception ex) {
            logger.error("{}: failed to append events", me(), ex);
            return appendEntriesFailure();
        }
    }

    @Override
    public RequestVoteResponse requestVote(RequestVoteRequest request) {
        long elapsedFromLastMessage = scheduledElection.get().getElapsed(TimeUnit.MILLISECONDS);

        // If a server receives a RequestVote within the minimum election timeout of hearing from a current leader, it
        // does not update its term or grant its vote
        if (heardFromLeader && clock.millis() - lastMessage.get() < minElectionTimeout()) {
            logger.debug("Request for vote received from {}. {} voted rejected", request.getCandidateId(), me());
            return requestVoteResponse(false);
        }

        updateCurrentTerm(request.getTerm());
        boolean voteGranted = voteGrantedFor(request);
        if (voteGranted){
            rescheduleElection(request.getTerm());
        }

        logger.debug("Request for vote received from {}. {} voted {}", request.getCandidateId(), me(), voteGranted);
        return requestVoteResponse(voteGranted);
    }

    @Override
    public InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request) {
        logger.debug("{}: received {}", me(), request);
        updateCurrentTerm(request.getTerm());

        // Reply immediately if term < currentTerm
        if (request.getTerm() < currentTerm()) {
            logger.warn("{}: term before current term {}", me(), currentTerm());
            return installSnapshotFailure();
        }

        rescheduleElection(request.getTerm());

        if (request.hasLastConfig()) {
            logger.debug("{}: applying config {}", me(), request.getLastConfig());
            raftGroup().raftConfiguration().update(request.getLastConfig().getNodesList());
        }

        if (request.getOffset() == 0) {
            // first segment
            raftGroup().localLogEntryStore().clear(request.getLastIncludedIndex(), request.getLastIncludedTerm());
        }

        snapshotManager().applySnapshotData(request.getDataList());

        return InstallSnapshotResponse.newBuilder()
                                      .setTerm(currentTerm())
                                      .setGroupId(groupId())
                                      .setSuccess(buildInstallSnapshotSuccess(request.getOffset()))
                                      .build();
    }

    private void cancelCurrentElectionTimeout() {
        Optional.ofNullable(scheduledElection.get())
                .ifPresent(Registration::cancel);
    }

    private void scheduleNewElection() {
            scheduledElection.set(scheduler().schedule(
                    this::checkMessageReceived,
                    minElectionTimeout()/10,
                    TimeUnit.MILLISECONDS));
    }

    private void checkMessageReceived() {
        long now = clock.millis();
        if( nextTimeout.get() < now) {
            changeStateTo(stateFactory().candidateState());
        } else {
            scheduleNewElection();
        }
    }

    private void rescheduleElection(long term) {
        if (term >= currentTerm()) {
            cancelCurrentElectionTimeout();
            scheduleNewElection();
        }
    }

    private AppendEntrySuccess buildAppendEntrySuccess(long lastLogIndex) {
        return AppendEntrySuccess.newBuilder()
                                 .setLastLogIndex(lastLogIndex)
                                 .build();
    }

    private InstallSnapshotSuccess buildInstallSnapshotSuccess(int offset) {
        return InstallSnapshotSuccess.newBuilder()
                                     .setLastReceivedOffset(offset)
                                     .build();
    }

    private boolean voteGrantedFor(RequestVoteRequest request) {
        //1. Reply false if term < currentTerm
        if (request.getTerm() < currentTerm()) {
            return false;
        }

        //2. If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant
        // vote
        String votedFor = votedFor();
        if (votedFor != null && !votedFor.equals(request.getCandidateId())) {
            return false;
        }

        if (request.getLastLogTerm() < lastLogTerm()) {
            return false;
        }

        if (request.getLastLogIndex() < lastLogIndex()) {
            return false;
        }

        markVotedFor(request.getCandidateId());
        return true;
    }

    public static class Builder extends AbstractMembershipState.Builder<Builder> {

        public FollowerState build() {
            return new FollowerState(this);
        }
    }
}
