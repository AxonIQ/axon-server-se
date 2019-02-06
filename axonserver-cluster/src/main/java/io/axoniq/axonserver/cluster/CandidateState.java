package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.configuration.CandidateConfiguration;
import io.axoniq.axonserver.cluster.configuration.ClusterConfiguration;
import io.axoniq.axonserver.cluster.election.Election.Result;
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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * @author Sara Pellegrini
 * @since 4.1
 */
public class CandidateState extends AbstractMembershipState {

    private static final Logger logger = LoggerFactory.getLogger(CandidateState.class);
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
        if (currentConfiguration().isEmpty()) return;
        newElection().result().subscribe(this::onElectionResult, error -> logger.warn("Failed to run election", error));
        resetElectionTimeout();
    }

    private void onElectionResult(Result result){
        if (result.won()) {
            changeStateTo(stateFactory().leaderState(), result.cause());
        } else if( result.goAway()) {
            changeStateTo(stateFactory().removedState(), result.cause());
        } else {
            changeStateTo(stateFactory().followerState(), result.cause());
        }
    }

    public static class Builder extends AbstractMembershipState.Builder<Builder> {

        public CandidateState build() {
            return new CandidateState(this);
        }
    }
}
