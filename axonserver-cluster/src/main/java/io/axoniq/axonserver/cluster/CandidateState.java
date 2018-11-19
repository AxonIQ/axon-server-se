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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class CandidateState extends AbstractMembershipState {

    private final Scheduler scheduler;
    private final AtomicReference<Registration> nextElection = new AtomicReference<>();
    private final AtomicReference<Election> currentElection = new AtomicReference<>();

    public static class Builder extends AbstractMembershipState.Builder<Builder> {

        private Scheduler scheduler = new DefaultScheduler();

        public Builder scheduler(Scheduler scheduler) {
            this.scheduler = scheduler;
            return this;
        }

        public CandidateState build() {
            return new CandidateState(this);
        }
    }

    private CandidateState(Builder builder) {
        super(builder);
        this.scheduler = builder.scheduler;
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
            MembershipState followerState = stateFactory().followerState();
            changeStateTo(followerState);
            return followerState.appendEntries(request);
        } else {
            return appendEntriesFailure();
        }
    }

    @Override
    public synchronized RequestVoteResponse requestVote(RequestVoteRequest request) {
        if (request.getTerm() > currentTerm()) {
            MembershipState followerState = stateFactory().followerState();
            changeStateTo(followerState);
            return followerState.requestVote(request);
        }
        return requestVoteResponse(false);
    }

    @Override
    public synchronized InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request) {
        if (request.getTerm() > currentTerm()) {
            MembershipState followerState = stateFactory().followerState();
            changeStateTo(followerState);
            return followerState.installSnapshot(request);
        }
        return installSnapshotFailure();
    }

    private void resetElectionTimeout() {
        long timeout = ThreadLocalRandom.current().nextLong(minElectionTimeout(), maxElectionTimeout() + 1);
        Registration newTask = scheduler.schedule(this::startElection, timeout, MILLISECONDS);
        nextElection.set(newTask);
    }

    private void startElection() {
        synchronized (this) {
            updateCurrentTerm(currentTerm() + 1);
            markVotedFor(me());
        }
        resetElectionTimeout();
        long raftGroupSize = raftGroup().raftConfiguration().groupMembers().size();
        currentElection.set(new CandidateElection(raftGroupSize));
        currentElection.get().onVoteReceived(me(), true);
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
        if (response.getTerm() < currentTerm()) {
            return;
        }
        Election election = this.currentElection.get();
        election.onVoteReceived(voter, response.getVoteGranted());
        if (election.isWon()) {
            changeStateTo(stateFactory().leaderState());
        }
    }
}
