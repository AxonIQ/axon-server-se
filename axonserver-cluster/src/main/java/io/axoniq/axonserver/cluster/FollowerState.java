package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.configuration.ClusterConfiguration;
import io.axoniq.axonserver.cluster.configuration.FollowerConfiguration;
import io.axoniq.axonserver.cluster.replication.LogEntryStore;
import io.axoniq.axonserver.cluster.scheduler.Scheduler;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.AppendEntrySuccess;
import io.axoniq.axonserver.grpc.cluster.ConfigChangeResult;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotSuccess;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.Math.min;

public class FollowerState extends AbstractMembershipState {

    private static final Logger logger = LoggerFactory.getLogger(FollowerState.class);
    private final ClusterConfiguration clusterConfiguration;

    private final AtomicReference<Scheduler> scheduler = new AtomicReference<>();
    private volatile boolean heardFromLeader;
    private volatile String leader;
    private final AtomicLong nextTimeout = new AtomicLong();
    private final AtomicLong lastMessage = new AtomicLong();
    private AtomicReference<String> leaderId = new AtomicReference<>();

    protected FollowerState(Builder builder) {
        super(builder);
        clusterConfiguration = new FollowerConfiguration(() -> leaderId.get());
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void start() {
        scheduler.set(schedulerFactory().get());
        nextTimeout.set(scheduler.get().clock().millis() + random(minElectionTimeout(), maxElectionTimeout()));
        scheduleElectionTimeoutChecker();
        heardFromLeader = false;
    }

    @Override
    public void stop() {
        if (scheduler.get() != null) {
            scheduler.getAndSet(null).shutdownNow();
        }
    }

    @Override
    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        try {
            updateCurrentTerm(request.getTerm());

            //1. Reply false if term < currentTerm
            if (request.getTerm() < currentTerm()) {
                logger.debug("{}: term before current term {}", groupId(), currentTerm());
                return appendEntriesFailure();
            }

            heardFromLeader = true;
            leader = request.getLeaderId();
            rescheduleElection(request.getTerm());
            LogEntryStore logEntryStore = raftGroup().localLogEntryStore();
            LogEntryProcessor logEntryProcessor = raftGroup().logEntryProcessor();
            if (logger.isTraceEnabled() && request.getPrevLogIndex() == 0) {
                logger.trace("{}: Received heartbeat, commitindex: {}", me(), request.getCommitIndex());
            }

            //2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
            if (!logEntryStore.contains(request.getPrevLogIndex(), request.getPrevLogTerm())) {
                logger.trace("{}: previous term/index missing {}/{} last log {}",
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
                if (logger.isTraceEnabled() && request.getEntriesCount() > 0) {
                    logger.trace("{}: received {}", groupId(), request.getEntries(0).getIndex());
                }
                logEntryStore.appendEntry(request.getEntriesList());
            } catch (IOException e) {
                logger.warn("{}: append failed", groupId(), e);
                stop();
                return appendEntriesFailure();
            }

            //5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
            if (request.getCommitIndex() > logEntryProcessor.commitIndex()) {
                long commit = min(request.getCommitIndex(), logEntryStore.lastLogIndex());
                Entry entry = logEntryStore.getEntry(commit);
                logEntryProcessor.markCommitted(entry.getIndex(), entry.getTerm());
            }

            long last = lastLogIndex();
            if (request.getEntriesCount() == 0 && request.getPrevLogIndex() > 0 && request.getPrevLogIndex() < last) {
                // this follower has more data than leader, sets matchIndex on leader to last common
                last = request.getPrevLogIndex();
                logger.trace("{}: Updated last to {}", groupId(), last);
            }
            return AppendEntriesResponse.newBuilder()
                                        .setGroupId(groupId())
                                        .setTerm(currentTerm())
                                        .setSuccess(buildAppendEntrySuccess(last))
                                        .setTerm(currentTerm())
                                        .build();
        } catch (Exception ex) {
            logger.error("{}: failed to append events", groupId(), ex);
            return appendEntriesFailure();
        }
    }

    @Override
    public RequestVoteResponse requestVote(RequestVoteRequest request) {
        // If a server receives a RequestVote within the minimum election timeout of hearing from a current leader, it
        // does not update its term or grant its vote
        if (heardFromLeader && scheduler.get().clock().millis() - lastMessage.get() < minElectionTimeout()) {
            logger.trace("{}: Request for vote received from {}. {} voted rejected",
                         groupId(),
                         request.getCandidateId(),
                         me());
            return requestVoteResponse(false);
        }

        updateCurrentTerm(request.getTerm());
        boolean voteGranted = voteGrantedFor(request);
        if (voteGranted) {
            rescheduleElection(request.getTerm());
        }
        return requestVoteResponse(voteGranted);
    }

    @Override
    public InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request) {
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
            raftGroup().localLogEntryStore().clear();
            snapshotManager().clear();
        }

        snapshotManager().applySnapshotData(request.getDataList())
                         .block();

        if (request.getDone()) {
            raftGroup().logEntryProcessor().updateLastApplied(request.getLastIncludedIndex(), request.getLastIncludedTerm());
        }

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

    private void scheduleElectionTimeoutChecker() {
        scheduler.get().schedule(
                this::checkMessageReceived,
                minElectionTimeout() / 10,
                TimeUnit.MILLISECONDS);
    }

    private void checkMessageReceived() {
        long now = scheduler.get().clock().millis();
        if (nextTimeout.get() < now) {
            logger.warn("{}: Timeout in follower state: {}", groupId(), (now - nextTimeout.get()));
            changeStateTo(stateFactory().candidateState());
        } else {
            scheduleElectionTimeoutChecker();
        }
    }

    @Override
    public void forceStepDown() {
        logger.warn("{}: Forced step down", groupId());
        changeStateTo(stateFactory().candidateState());
    }

    private void rescheduleElection(long term) {
        if (term >= currentTerm()) {
            lastMessage.set(scheduler.get().clock().millis());
            nextTimeout.set(lastMessage.get() + random(minElectionTimeout(), maxElectionTimeout()));
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
            logger.trace("{}: Vote not granted. Current term {} is greater than requested {}.",
                         groupId(),
                         currentTerm(),
                         request.getTerm());
            return false;
        }

        //2. If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant
        // vote
        String votedFor = votedFor();
        if (votedFor != null && !votedFor.equals(request.getCandidateId())) {
            logger.trace("{}: Vote not granted. Already voted for: {}.", groupId(), votedFor);
            return false;
        }

        TermIndex lastLog = lastLog();
        if (request.getLastLogTerm() < lastLog.getTerm()) {
            logger.trace("{}: Vote not granted. Requested last log term {}, my last log term {}.",
                         groupId(),
                         request.getLastLogTerm(),
                         lastLog.getTerm());
            return false;
        }

        if (request.getLastLogIndex() < lastLog.getIndex()) {
            logger.trace("{}: Vote not granted. Requested last log index {}, my last log index {}.",
                         groupId(),
                         request.getLastLogIndex(),
                         lastLog.getIndex());
            return false;
        }

        logger.trace("{}: Vote granted for {}.", groupId(), request.getCandidateId());
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
