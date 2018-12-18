package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.Scheduler.ScheduledRegistration;
import io.axoniq.axonserver.cluster.configuration.ClusterConfiguration;
import io.axoniq.axonserver.cluster.configuration.FollowerConfiguration;
import io.axoniq.axonserver.cluster.replication.LogEntryStore;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.AppendEntrySuccess;
import io.axoniq.axonserver.grpc.cluster.ConfigChangeResult;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotSuccess;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.Math.min;

public class FollowerState extends AbstractMembershipState {

    private static final Logger logger = LoggerFactory.getLogger(FollowerState.class);
    private final ClusterConfiguration clusterConfiguration;

    private final AtomicReference<ScheduledRegistration> scheduledElection = new AtomicReference<>();
    private volatile boolean heardFromLeader;
    private volatile String leader;
    private final AtomicLong nextTimeout = new AtomicLong();
    private final AtomicLong lastMessage = new AtomicLong();
    private final Clock clock;
    private AtomicReference<String> leaderId = new AtomicReference<>();

    protected FollowerState(Builder builder) {
        super(builder);
        clock = scheduler().clock();
        clusterConfiguration = new FollowerConfiguration(() -> leaderId.get());
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void start() {
        cancelElectionTimeoutChecker();
        nextTimeout.set(clock.millis() + random(minElectionTimeout(), maxElectionTimeout()));
        scheduleElectionTimeoutChecker();
        heardFromLeader = false;
    }

    @Override
    public void stop() {
        cancelElectionTimeoutChecker();
    }

    @Override
    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        try {
            updateCurrentTerm(request.getTerm());

            //1. Reply false if term < currentTerm
            if (request.getTerm() < currentTerm()) {
                logger.warn("{}: term before current term {}", groupId(), currentTerm());
                return appendEntriesFailure();
            }

            heardFromLeader = true;
            leader = request.getLeaderId();
            rescheduleElection(request.getTerm());
            LogEntryStore logEntryStore = raftGroup().localLogEntryStore();
            LogEntryProcessor logEntryProcessor = raftGroup().logEntryProcessor();
            if( logger.isTraceEnabled() && request.getPrevLogIndex() == 0) {
                logger.trace("{} Received heartbeat, commitindex: {}", me(), request.getCommitIndex());
            }

            //2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
            if (!logEntryStore.contains(request.getPrevLogIndex(), request.getPrevLogTerm())) {
                logger.warn("{}: previous term/index missing {}/{} last log {}",
                            groupId(),
                            request.getPrevLogTerm(),
                            request.getPrevLogIndex(),
                            logEntryStore.lastLogIndex());
                return appendEntriesFailure();
            }

            //3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry
            // and all that follow it
            //4. Append any new entries not already in the log
            try {
                if( request.getEntriesCount() > 0) {
                    logger.trace("{}: received {}", groupId(), request.getEntries(0).getIndex());
                }
                logEntryStore.appendEntry(request.getEntriesList());
                logger.trace("{}: stored {}", groupId(), lastLogIndex());
            } catch (IOException e) {
                logger.warn("{}: append failed", groupId(), e);
                stop();
                return appendEntriesFailure();
            }

            //5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
            if (request.getCommitIndex() > logEntryProcessor.commitIndex()) {
                if (request.getEntriesCount() == 0) {
                    logEntryProcessor.markCommitted(request.getCommitIndex());
                } else {
                    logEntryProcessor.markCommitted(min(request.getCommitIndex(),
                                                    request.getEntries(request.getEntriesCount() - 1).getIndex()));
                }
            }

            long last = lastLogIndex();
            if( request.getEntriesCount() == 0 && request.getPrevLogIndex()  > 0 && request.getPrevLogIndex() < last) {
                // this follower has more data than leader, sets matchIndex on leader to last common
                last = request.getPrevLogIndex();
                logger.trace("Updated last to {}", last);
            }
            return AppendEntriesResponse.newBuilder()
                                        .setGroupId(groupId())
                                        .setTerm(currentTerm())
                                        .setSuccess(buildAppendEntrySuccess(last))
                                        .setTerm(currentTerm())
                                        .build();
        } catch( Exception ex) {
            logger.error("{}: failed to append events", me(), ex);
            return appendEntriesFailure();
        }
    }

    @Override
    public RequestVoteResponse requestVote(RequestVoteRequest request) {
        // If a server receives a RequestVote within the minimum election timeout of hearing from a current leader, it
        // does not update its term or grant its vote
        if (heardFromLeader && clock.millis() - lastMessage.get() < minElectionTimeout()) {
            logger.debug("{}: Request for vote received from {}. {} voted rejected", groupId(), request.getCandidateId(), me());
            return requestVoteResponse(false);
        }

        updateCurrentTerm(request.getTerm());
        boolean voteGranted = voteGrantedFor(request);
        if (voteGranted){
            rescheduleElection(request.getTerm());
        }

        logger.debug("{}: Request for vote received from {}. {} voted {}", groupId(), request.getCandidateId(), me(), voteGranted);
        return requestVoteResponse(voteGranted);
    }

    @Override
    public InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request) {
        logger.debug("{}: received {}", me(), request);
        updateCurrentTerm(request.getTerm());

        // Reply immediately if term < currentTerm
        if (request.getTerm() < currentTerm()) {
            logger.warn("{}: term before current term {}", groupId(), currentTerm());
            return installSnapshotFailure();
        }

        rescheduleElection(request.getTerm());

        if (request.hasLastConfig()) {
            logger.debug("{}: applying config {}", groupId(), request.getLastConfig());
            raftGroup().raftConfiguration().update(request.getLastConfig().getNodesList());
        }

        if (request.getOffset() == 0) {
            // first segment
            raftGroup().localLogEntryStore().clear(request.getLastIncludedIndex(), request.getLastIncludedTerm());
        }

        snapshotManager().applySnapshotData(request.getDataList())
                         .block();

        return InstallSnapshotResponse.newBuilder()
                                      .setTerm(currentTerm())
                                      .setGroupId(groupId())
                                      .setSuccess(buildInstallSnapshotSuccess(request.getOffset()))
                                      .build();
    }

    @Override
    public String getLeader() {
        return leader;
    }

    private void cancelElectionTimeoutChecker() {
        Optional.ofNullable(scheduledElection.get())
                .ifPresent(Registration::cancel);
    }

    private void scheduleElectionTimeoutChecker() {
            scheduledElection.set(scheduler().schedule(
                    this::checkMessageReceived,
                    minElectionTimeout()/10,
                    TimeUnit.MILLISECONDS));
    }

    private void checkMessageReceived() {
        long now = clock.millis();
        if( nextTimeout.get() < now) {
            logger.warn("{}: Timeout in follower state: {}", groupId(), (now-nextTimeout.get()));
            changeStateTo(stateFactory().candidateState());
        } else {
            scheduleElectionTimeoutChecker();
        }
    }

    private void rescheduleElection(long term) {
        if (term >= currentTerm()) {
            lastMessage.set(clock.millis());
            nextTimeout.updateAndGet(old -> Math.max(old,lastMessage.get() + random(minElectionTimeout(), maxElectionTimeout())));
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

        TermIndex lastLog = lastLog();
        if (request.getLastLogTerm() < lastLog.getTerm()) {
            return false;
        }

        if (request.getLastLogIndex() < lastLog.getIndex()) {
            return false;
        }

        markVotedFor(request.getCandidateId());
        return true;
    }

    @Override
    public CompletableFuture<ConfigChangeResult> addServer(Node node) {
        return this.clusterConfiguration.addServer(node);
    }

    @Override
    public CompletableFuture<ConfigChangeResult> removeServer(String nodeId) {
        return this.clusterConfiguration.removeServer(nodeId);
    }

    public static class Builder extends AbstractMembershipState.Builder<Builder> {

        public FollowerState build() {
            return new FollowerState(this);
        }
    }
}
