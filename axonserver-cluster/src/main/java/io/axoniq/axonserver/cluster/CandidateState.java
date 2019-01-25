package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.configuration.CandidateConfiguration;
import io.axoniq.axonserver.cluster.configuration.ClusterConfiguration;
import io.axoniq.axonserver.cluster.election.MajorityElection;
import io.axoniq.axonserver.cluster.election.Election;
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

    @Override
    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        if (request.getTerm() >= currentTerm()) {
            logger.trace("{}: Received term {} which is greater or equals than mine {}. Moving to Follower...",
                         groupId(),
                         request.getTerm(),
                         currentTerm());
            return handleAsFollower(follower -> follower.appendEntries(request));
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
            RequestVoteResponse vote = handleAsFollower(follower -> follower.requestVote(request));
            logger.trace("{}: Request for vote received from {} in term {}. {} voted {}",
                         groupId(),
                         request.getCandidateId(),
                         request.getTerm(),
                         me(),
                         vote != null && vote.getVoteGranted());
            return vote;
        }
        logger.trace("{}: Request for vote received from {} in term {}. {} voted rejected",
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
            return handleAsFollower(follower -> follower.installSnapshot(request));
        }
        logger.trace("{}: Received term {} is smaller or equal than mine {}. Rejecting the request.",
                     groupId(),
                     request.getTerm(),
                     currentTerm());
        return installSnapshotFailure(request.getRequestId());
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
                updateCurrentTerm(currentTerm() + 1);
                markVotedFor(me());
            }
            logger.trace("{}: Starting election from {} in term {}", groupId(), me(), currentTerm());
            resetElectionTimeout();
            currentElection.set(new MajorityElection(this::clusterSize));
            currentElection.get().registerVoteReceived(me(), true);
            Collection<RaftPeer> raftPeers = otherPeers();
            if (raftPeers.isEmpty() && !currentConfiguration().isEmpty()) {
                currentElection.set(null);
                changeStateTo(stateFactory().leaderState());
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
            updateCurrentTerm(response.getTerm());
            changeStateTo(stateFactory().followerState());
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
                changeStateTo(stateFactory().leaderState());
            }
        }
    }

    public static class Builder extends AbstractMembershipState.Builder<Builder> {

        public CandidateState build() {
            return new CandidateState(this);
        }
    }
}
