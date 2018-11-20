package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;

import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class CandidateState extends AbstractMembershipState {

    private final AtomicReference<Registration> nextElection = new AtomicReference<>();
    private final AtomicReference<Election> currentElection = new AtomicReference<>();

    public static class Builder extends AbstractMembershipState.Builder<Builder> {
        public CandidateState build() {
            return new CandidateState(this);
        }
    }

    private CandidateState(Builder builder) {
        super(builder);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void start() {
        startElection();
    }

    @Override
    public void stop() {
        Optional.ofNullable(nextElection.get()).ifPresent(Registration::cancel);
    }

    @Override
    public synchronized AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        if (request.getTerm() >= currentTerm()) {
            return handleAsFollower(follower -> follower.appendEntries(request));
        }
        return appendEntriesFailure();
    }

    @Override
    public synchronized RequestVoteResponse requestVote(RequestVoteRequest request) {
        if (request.getTerm() > currentTerm()) {
            return handleAsFollower(follower -> follower.requestVote(request));
        }
        return requestVoteResponse(false);
    }

    @Override
    public synchronized InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request) {
        if (request.getTerm() > currentTerm()) {
            return handleAsFollower(follower -> follower.installSnapshot(request));
        }
        return installSnapshotFailure();
    }

    private void resetElectionTimeout() {
        long timeout = random(minElectionTimeout(), maxElectionTimeout() + 1);
        Registration newTask = scheduler().schedule(this::startElection, timeout, MILLISECONDS);
        nextElection.set(newTask);
    }

    private void startElection() {
        synchronized (this) {
            updateCurrentTerm(currentTerm() + 1);
            markVotedFor(me());
        }
        resetElectionTimeout();
        currentElection.set(new CandidateElection(clusterSize()));
        currentElection.get().registerVoteReceived(me(), true);
        RequestVoteRequest request = requestVote();
        Iterable<RaftPeer> raftPeers = otherNodes();
        raftPeers.forEach(node -> requestVote(request, node));
    }

    private RequestVoteRequest requestVote() {
        return RequestVoteRequest.newBuilder()
                                 .setGroupId(groupId())
                                 .setCandidateId(me())
                                 .setTerm(currentTerm())
                                 .setLastLogIndex(lastLogIndex())
                                 .setLastLogTerm(lastLogTerm())
                                 .build();
    }

    private void requestVote(RequestVoteRequest request, RaftPeer node) {
        node.requestVote(request).thenAccept(response -> onVoteResponse(node.nodeId(), response));
    }

    private synchronized void onVoteResponse(String voter, RequestVoteResponse response) {
        if (response.getTerm() > currentTerm()) {
            changeStateTo(stateFactory().followerState());
            return;
        }
        //The candidate can receive a response with lower term if the voter is receiving regular heartbeat from a leader.
        //In this case, the voter recognizes any request of vote as disruptive, refuses the vote and does't update its term.
        if (response.getTerm() < currentTerm()) {
            return;
        }
        Election election = this.currentElection.get();
        election.registerVoteReceived(voter, response.getVoteGranted());
        if (election.isWon()) {
            changeStateTo(stateFactory().leaderState());
        }
    }
}
