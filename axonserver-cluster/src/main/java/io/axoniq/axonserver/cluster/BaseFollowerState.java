package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.configuration.ClusterConfiguration;
import io.axoniq.axonserver.cluster.configuration.FollowerConfiguration;
import io.axoniq.axonserver.cluster.replication.LogEntryStore;
import io.axoniq.axonserver.cluster.scheduler.Scheduler;
import io.axoniq.axonserver.cluster.util.RoleUtils;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.ConfigChangeResult;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.Math.min;
import static java.lang.String.format;

/**
 * Base state for Follower, Prospect and Secondary states.
 *
 * @author Milan Savic, Marc Gathier
 * @since 4.3
 */
public abstract class BaseFollowerState extends AbstractMembershipState {

    private static final Logger logger = LoggerFactory.getLogger(BaseFollowerState.class);
    private final ClusterConfiguration clusterConfiguration;

    protected final AtomicReference<Scheduler> scheduler = new AtomicReference<>();
    private final AtomicLong lastMessage = new AtomicLong();
    private final AtomicReference<String> leaderId = new AtomicReference<>();
    private final AtomicInteger lastSnapshotChunk = new AtomicInteger(-1);
    protected final AtomicBoolean processing = new AtomicBoolean();
    protected volatile boolean heardFromLeader;
    private long followerStateStated;

    protected BaseFollowerState(Builder builder) {
        super(builder);
        clusterConfiguration = new FollowerConfiguration(leaderId::get);
    }

    @Override
    public void start() {
        heardFromLeader = false;
        leaderId.set(null);
        // initialize lastMessage with current time to get a meaningful message in case of initial timeout
        scheduler.set(schedulerFactory().get());
        lastMessage.set(scheduler.get().clock().millis());
        followerStateStated = lastMessage.get();
        // initially the timeout is increased to prevent leader elections when node restarts
    }

    @Override
    public void stop() {
        if (scheduler.get() != null) {
            scheduler.getAndSet(null).shutdownNow();
        }
    }

    /**
     * Adds the provided entries to the raft log on this node. During this action the process will not check for
     * timeouts
     * in the follower state.
     *
     * @param request the entries to add
     * @return success or failure response
     */
    @Override
    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        processing.set(true);
        long before = startTimer();
        try {
            return doAppendEntries(request);
        } finally {
            long after = System.currentTimeMillis();

            if (after - before > raftGroup().raftConfiguration().heartbeatTimeout()) {
                logger.trace("{} in term {}: Append entries took {}ms", groupId(), currentTerm(), after - before);
            }
            processing.set(false);
        }
    }

    /**
     * logs a message if the time since last message received is less than twice the heartbeat time. Indicates that
     * the follower is not receiving messages at the expected speed.
     *
     * @return the current timestamp
     */
    private long startTimer() {
        long start = System.currentTimeMillis();
        if (logger.isTraceEnabled() && start - lastMessage.get() > 2 * raftGroup().raftConfiguration()
                                                                                  .heartbeatTimeout()) {
            logger.trace("{} in term {}: Not received any messages for  {}ms",
                         groupId(),
                         currentTerm(),
                         start - lastMessage.get());
        }
        return start;
    }

    private AppendEntriesResponse doAppendEntries(AppendEntriesRequest request) {

        try {
            if (!heardFromLeader) {
                logger.info("{} in term {}: {} received first AppendEntriesRequest after {}ms",
                            groupId(),
                            currentTerm(),
                            me(),
                            System.currentTimeMillis() - followerStateStated
                );
            }
            String cause = format("%s in term %s: %s received AppendEntriesRequest with term = %s from %s",
                                  groupId(),
                                  currentTerm(),
                                  me(),
                                  request.getTerm(),
                                  request.getLeaderId());
            updateCurrentTerm(request.getTerm(), cause);

            //1. Reply false if term < currentTerm
            if (request.getTerm() < currentTerm()) {
                String failureCause = String.format("%s in term %s: received term %s is before current term %s",
                                                    groupId(),
                                                    currentTerm(),
                                                    request.getTerm(),
                                                    currentTerm());
                logger.info(failureCause);
                return responseFactory().appendEntriesFailure(request.getRequestId(), failureCause);
            }

            heardFromLeader = true;
            if (!request.getLeaderId().equals(leaderId.get())) {
                leaderId.set(request.getLeaderId());
                logger.info("{} in term {}: Updated leader to {}", groupId(), currentTerm(), leaderId.get());
                raftGroup().localNode().notifyNewLeader(leaderId.get());
            }

            LogEntryStore logEntryStore = raftGroup().localLogEntryStore();
            LogEntryProcessor logEntryProcessor = raftGroup().logEntryProcessor();

            //2. Reply false if the prev term and index are not valid
            if (!validPrevTermIndex(request.getPrevLogIndex(), request.getPrevLogTerm())) {
                String failureCause = String.format("%s in term %s: previous term/index missing %s/%s last log %s",
                                                    groupId(),
                                                    currentTerm(),
                                                    request.getPrevLogTerm(),
                                                    request.getPrevLogIndex(),
                                                    logEntryStore.lastLogIndex());

                logger.info(failureCause);
                // Allow for extra time from leader, the current node is not up to date and should not move to candidate state too soon
                rescheduleElection(request.getTerm(), raftGroup().raftConfiguration().maxElectionTimeout());

                return responseFactory().appendEntriesFailure(request.getRequestId(), failureCause);
            }

            //3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry
            // and all that follow it
            //4. Append any new entries not already in the log
            try {
                logEntryStore.appendEntry(request.getEntriesList());
            } catch (IOException e) {
                String failureCause = String.format("%s in term %s: append failed critically with IOException: %s",
                                                    groupId(),
                                                    currentTerm(),
                                                    e.getMessage());
                logger.error(failureCause + ". Shutting down.", e);
                fatalShutdown(failureCause);
                return responseFactory().appendEntriesFailure(request.getRequestId(), failureCause);
            }

            //5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
            if (request.getCommitIndex() > logEntryProcessor.commitIndex()) {
                long commit = min(request.getCommitIndex(), logEntryStore.lastLogIndex());
                Entry entry = logEntryStore.getEntry(commit);
                if (entry == null) {
                    logger.warn("{} in term {}: cannot check commit index {}, missing log entry, first log entry {}",
                                groupId(), currentTerm(), request.getCommitIndex(), logEntryStore.firstLogIndex());
                } else {
                    logEntryProcessor.markCommitted(entry.getIndex(), entry.getTerm());
                }
            }

            long last = lastLogIndex();
            if (request.getEntriesCount() == 0 && request.getPrevLogIndex() > 0 && request.getPrevLogIndex() < last) {
                // this follower has more data than leader, sets matchIndex on leader to last common
                last = request.getPrevLogIndex();
                logger.trace("{} in term {}: Updated last to {}", groupId(), currentTerm(), last);
            }
            rescheduleElection(request.getTerm());
            return responseFactory().appendEntriesSuccess(request.getRequestId(), last);
        } catch (Exception ex) {
            String failureCause = String.format("%s in term %s: critical failure while attempting to append events: %s",
                                                groupId(),
                                                currentTerm(),
                                                ex.getMessage());
            logger.error(failureCause + ". Shutting down.", ex);
            fatalShutdown(failureCause);
            return responseFactory().appendEntriesFailure(request.getRequestId(), failureCause);
        }
    }

    protected void rescheduleElection(long term) {
        rescheduleElection(term, 0);
    }

    protected void rescheduleElection(long term, long extra) {

    }

    private void fatalShutdown(String failureCause) {
        changeStateTo(stateFactory().fatalState(), failureCause);
    }

    /**
     * Checks if the log contains an entry at prevLogIndex whose term matches prevLogTerm
     * or if the previous index and term are those included in the latest snapshot installed
     *
     * @param prevIndex the index of the previous log entry
     * @param prevTerm  the term of the previous log entry
     * @return true if prev index and term can be considered valid, false otherwise.
     */
    private boolean validPrevTermIndex(long prevIndex, long prevTerm) {
        LogEntryStore logEntryStore = raftGroup().localLogEntryStore();
        LogEntryProcessor logEntryProcessor = raftGroup().logEntryProcessor();
        return logEntryStore.contains(prevIndex, prevTerm) || logEntryProcessor.isLastApplied(prevIndex, prevTerm);
    }

    /**
     * Applies the provided entries. During this action the process will not check for timeouts
     * in the follower state.
     *
     * @param request the snapshot entries to apply
     * @return success or failure response
     */
    @Override
    public InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request) {
        processing.set(true);
        long before = startTimer();
        try {
            return doInstallSnapshot(request);
        } finally {
            long after = System.currentTimeMillis();

            if (after - before > raftGroup().raftConfiguration().heartbeatTimeout()) {
                logger.trace("{} in term {}: Install snapshot took {}ms", groupId(), currentTerm(), after - before);
            }
            processing.set(false);
        }
    }

    private InstallSnapshotResponse doInstallSnapshot(InstallSnapshotRequest request) {
        String cause = format("%s in term %s: %s received InstallSnapshotRequest with term = %s from %s",
                              groupId(),
                              currentTerm(),
                              me(),
                              request.getTerm(),
                              request.getLeaderId());
        updateCurrentTerm(request.getTerm(), cause);

        // Reply immediately if term < currentTerm
        if (request.getTerm() < currentTerm()) {
            String failureCause = String.format(
                    "%s in term %s: Install snapshot failed - term (%s) is before current term (%s)",
                    groupId(),
                    currentTerm(),
                    request.getTerm(),
                    currentTerm());
            logger.info(failureCause);
            return responseFactory().installSnapshotFailure(request.getRequestId(), failureCause);
        }

        // Allow for extra time from leader, the current node is not up to date and should not move to candidate state too soon
        rescheduleElection(request.getTerm(), raftGroup().raftConfiguration().maxElectionTimeout());

        //Install snapshot chunks must arrive in the correct order. If the chunk doesn't have the expected index it will be rejected.
        //The first chunk (index = 0) is always accepted in order to restore from a partial installation caused by a disrupted leader.
        if (request.getOffset() != 0 && (lastSnapshotChunk.get() + 1) != request.getOffset()) {
            String failureCause = format(
                    "%s in term %s: missing previous snapshot chunk. Received %s while expecting %s.",
                    groupId(),
                    currentTerm(),
                    request.getOffset(),
                    (lastSnapshotChunk.get() + 1)
            );
            logger.warn(failureCause);
            return responseFactory().installSnapshotFailure(request.getRequestId(), failureCause);
        }

        if (request.hasLastConfig()) {
            logger.trace("{} in term {}: applying config {}", groupId(), currentTerm(), request.getLastConfig());
            raftGroup().raftConfiguration().update(request.getLastConfig().getNodesList());
            raftGroup().localNode().notifyNewLeader(leaderId.get());
        }

        if (request.getOffset() == 0) {
            logger.info("{} in term {}: Install snapshot started.", groupId(), currentTerm());
            // first segment
            raftGroup().localLogEntryStore().clear(request.getLastIncludedIndex());
            raftGroup().logEntryProcessor().reset();
            snapshotManager().clear();
        }

        try {
            snapshotManager().applySnapshotData(request.getDataList())
                             .block();
        } catch (Exception ex) {
            String failureCause = String.format(
                    "%s in term %s: Install snapshot failed - %s",
                    groupId(),
                    currentTerm(),
                    ex.getMessage());
            logger.error(failureCause, ex);
            return responseFactory().installSnapshotFailure(request.getRequestId(), failureCause);
        }

        lastSnapshotChunk.set(request.getOffset());
        //When install snapshot request is completed, update commit and last applied indexes.
        if (request.getDone()) {
            long index = request.getLastIncludedIndex();
            long term = request.getLastIncludedTerm();
            raftGroup().logEntryProcessor().updateLastApplied(index, term);
            raftGroup().logEntryProcessor().markCommitted(index, term);
            lastSnapshotChunk.set(-1);
            logger.info(
                    "{} in term {}: Install snapshot finished. Last applied entry: {}, Last log entry {}, Commit Index: {}",
                    groupId(),
                    currentTerm(),
                    raftGroup().logEntryProcessor().lastAppliedIndex(),
                    raftGroup().localLogEntryStore().lastLogIndex(),
                    raftGroup().logEntryProcessor().commitIndex());
        }
        return responseFactory().installSnapshotSuccess(request.getRequestId(), request.getOffset());
    }

    @Override
    public String getLeader() {
        return leaderId.get();
    }


    @Override
    public void forceStepDown() {
        if (!RoleUtils.primaryNode(currentNode().getRole())) {
            logger.warn("{} in term {}: Request to start new election ignored, node is not a primary node.",
                        groupId(),
                        currentTerm());
            return;
        }
        String cause = format("%s in term %s: Forced transition from Follower to Candidate.", groupId(), currentTerm());
        logger.warn(cause);
        changeStateTo(((CandidateState) stateFactory().candidateState()).withDisruptAllowed(), cause);
    }

    @Override
    public CompletableFuture<ConfigChangeResult> addServer(Node node) {
        return this.clusterConfiguration.addServer(node);
    }

    @Override
    public CompletableFuture<ConfigChangeResult> removeServer(String nodeId) {
        return this.clusterConfiguration.removeServer(nodeId);
    }
}
