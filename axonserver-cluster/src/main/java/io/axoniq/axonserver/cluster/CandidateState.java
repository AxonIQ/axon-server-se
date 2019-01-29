package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.configuration.CandidateConfiguration;
import io.axoniq.axonserver.cluster.configuration.ClusterConfiguration;
import io.axoniq.axonserver.cluster.election.Election;
import io.axoniq.axonserver.cluster.election.MajorityElection;
import io.axoniq.axonserver.cluster.scheduler.Scheduler;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.ConfigChangeResult;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class CandidateState extends AbstractMembershipState {

    private static final Logger logger = LoggerFactory.getLogger(CandidateState.class);
    private final AtomicReference<Election> currentElection = new AtomicReference<>();
    private final ClusterConfiguration clusterConfiguration = new CandidateConfiguration();
    private final AtomicReference<Scheduler> scheduler = new AtomicReference<>();

    private CandidateState(Builder builder) {
        super(builder);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void start() {
        scheduler.set(schedulerFactory().get());
        startElection();
    }

    @Override
    public void stop() {
        if (scheduler.get() != null) {
            scheduler.getAndSet(null).shutdownNow();
        }
    }

    private  <R> R handleAsFollower(Function<MembershipState, R> handler, String cause) {
        MembershipState followerState = stateFactory().followerState();
        changeStateTo(followerState, cause);
        return handler.apply(followerState);
    }

    @Override
    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        if (request.getTerm() >= currentTerm()) {
            logger.info("{}: Received term {} which is greater or equals than mine {}. Moving to Follower...",
                         groupId(), request.getTerm(), currentTerm());
            String message = format("%s received AppendEntriesRequest with greater or equals term (%s >= %s) from %s",
                                    me(), request.getTerm(), currentTerm(), request.getLeaderId());
            return handleAsFollower(follower -> follower.appendEntries(request), message);
        }
        logger.trace("{}: Received term {} is smaller than mine {}. Rejecting the request.",
                     groupId(),
                     request.getTerm(),
                     currentTerm());
        return appendEntriesFailure(request.getRequestId(), "Request rejected because I'm a candidate");
    }

    @Override
    public RequestVoteResponse requestVote(RequestVoteRequest request) {
        if (request.getTerm() > currentTerm()) {
            String message = format("%s received RequestVoteRequest with greater term (%s > %s) from %s",
                                    me(), request.getTerm(), currentTerm(), request.getCandidateId());
            RequestVoteResponse vote = handleAsFollower(follower -> follower.requestVote(request), message);
            logger.info("{}: Request for vote received from {} in term {}. {} voted {} (handled as follower)",
                         groupId(),
                         request.getCandidateId(),
                         request.getTerm(),
                         me(),
                         vote != null && vote.getVoteGranted());
            return vote;
        }
        logger.info("{}: Request for vote received from {} in term {}. {} voted rejected",
                     groupId(),
                     request.getCandidateId(),
                     request.getTerm(),
                     me());
        return requestVoteResponse(request.getRequestId(), false);
    }

    @Override
    public InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request) {
        if (request.getTerm() > currentTerm()) {
            logger.trace(
                    "{}: Received install snapshot with term {} which is greater than mine {}. Moving to Follower...",
                    groupId(),
                    request.getTerm(),
                    currentTerm());
            String message = format("%s received InstallSnapshotRequest with greater term (%s > %s) from %s",
                                    me(), request.getTerm(), currentTerm(), request.getLeaderId());
            return handleAsFollower(follower -> follower.installSnapshot(request), message);
        }
        String cause = format("%s: Received term (%s) is smaller or equal than mine (%s). Rejecting the request.",
                                     groupId(), request.getTerm(), currentTerm());
        logger.trace(cause);
        return installSnapshotFailure(request.getRequestId(), cause);
    }

    @Override
    public CompletableFuture<ConfigChangeResult> addServer(Node node) {
        return clusterConfiguration.addServer(node);
    }

    @Override
    public CompletableFuture<ConfigChangeResult> removeServer(String nodeId) {
        return clusterConfiguration.removeServer(nodeId);
    }

    private void resetElectionTimeout() {
        int timeout = random(minElectionTimeout(), maxElectionTimeout() + 1);
        scheduler.get().schedule(this::startElection, timeout, MILLISECONDS);
    }

    private void startElection() {
        try {
            synchronized (this) {
                long newTerm = currentTerm() + 1;
                String cause = format("%s is starting a new election, so increases its term from %s to %s",
                                      me(), currentTerm(), newTerm);
                updateCurrentTerm(newTerm, cause);
                markVotedFor(me());
            }
            logger.info("{}: Starting election from {} in term {}", groupId(), me(), currentTerm());
            resetElectionTimeout();
            currentElection.set(new MajorityElection(this::clusterSize));
            currentElection.get().registerVoteReceived(me(), true);
            Collection<RaftPeer> raftPeers = otherPeers();
            if (raftPeers.isEmpty() && !currentConfiguration().isEmpty()) {
                currentElection.set(null);
                changeStateTo(stateFactory().leaderState(), "No other nodes in the raft group.");
            } else {
                raftPeers.forEach(node -> requestVote(requestVote(), node));
            }
        } catch (Exception ex) {
            logger.warn("Failed to start election", ex);
        }
    }

    private RequestVoteRequest requestVote() {
        TermIndex lastLog = lastLog();
        return RequestVoteRequest.newBuilder()
                                 .setRequestId(UUID.randomUUID().toString())
                                 .setGroupId(groupId())
                                 .setCandidateId(me())
                                 .setTerm(currentTerm())
                                 .setLastLogIndex(lastLog.getIndex())
                                 .setLastLogTerm(lastLog.getTerm())
                                 .build();
    }

    private void requestVote(RequestVoteRequest request, RaftPeer node) {
        node.requestVote(request).thenAccept(response -> {
            synchronized (raftGroup().localNode()){
                onVoteResponse(response);
            }
        });
    }

    private void onVoteResponse(RequestVoteResponse response) {
        String voter = response.getResponseHeader().getNodeId();
        logger.trace("{} - currentTerm {} VoteResponse {}", voter, currentTerm(), response);
        if (response.getTerm() > currentTerm()) {
            String message = format("%s received RequestVoteResponse with greater term (%s > %s) from %s",
                                    me(), response.getTerm(), currentTerm(), voter);
            updateCurrentTerm(response.getTerm(), message);
            changeStateTo(stateFactory().followerState(), message);
            return;
        }
        //The candidate can receive a response with lower term if the voter is receiving regular heartbeat from a leader.
        //In this case, the voter recognizes any request of vote as disruptive, refuses the vote and does't update its term.
        if (response.getTerm() < currentTerm()) {
            return;
        }
        Election election = this.currentElection.get();
        if (election != null) {
            election.registerVoteReceived(voter, response.getVoteGranted());
            if (election.isWon()) {
                this.currentElection.set(null);
                String message = format("%s won the election for context %s {%s}", me(), groupId(), election);
                changeStateTo(stateFactory().leaderState(), message);
            }
        }
    }

    public static class Builder extends AbstractMembershipState.Builder<Builder> {

        public CandidateState build() {
            return new CandidateState(this);
        }
    }
}
