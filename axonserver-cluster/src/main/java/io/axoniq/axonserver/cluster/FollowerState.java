package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;

/**
 * Performs all actions when the node is in the Follower state.
 *
 * @author Milan Savic
 * @since 4.1
 */
public class FollowerState extends BaseFollowerState {

    private static final Logger logger = LoggerFactory.getLogger(FollowerState.class);

    private final AtomicLong nextTimeout = new AtomicLong();
    private final AtomicLong lastMessage = new AtomicLong();

    private FollowerState(Builder builder) {
        super(builder);
    }

    /**
     * Instantiates a new builder for the Follower State.
     *
     * @return a new builder for the Follower State
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void start() {
        super.start();
        // initially the timeout is increased to prevent leader elections when node restarts
        nextTimeout.set(currentTimeMillis() + initialElectionTimeout() + random(minElectionTimeout(), maxElectionTimeout()));
        scheduleElectionTimeoutChecker();
    }

    @Override
    public RequestVoteResponse requestVote(RequestVoteRequest request) {
        // If a server receives a RequestVote within the minimum election timeout of hearing from a current leader, it
        // does not update its term or grant its vote
        if (!request.getDisruptAllowed() && heardFromLeader && timeSinceLastMessage() < minElectionTimeout()) {
            logger.info(
                    "{} in term {}: Request for vote received from {}. Voted rejected since we have heard from the leader.",
                    groupId(),
                    currentTerm(),
                    request.getCandidateId());
            return responseFactory().voteRejected(request.getRequestId());
        }
        String cause = format("%s in term %s: %s received RequestVoteRequest with term = %s from %s",
                              groupId(),
                              currentTerm(),
                              me(),
                              request.getTerm(),
                              request.getCandidateId());
        updateCurrentTerm(request.getTerm(), cause);
        boolean voteGranted = voteGrantedFor(request, false);
        if (voteGranted) {
            rescheduleElection(request.getTerm());
        }
        return responseFactory().voteResponse(request.getRequestId(), voteGranted);
    }

    @Override
    public RequestVoteResponse requestPreVote(RequestVoteRequest request) {
        // If a server receives a pre-vote within the minimum election timeout of hearing from a current leader, it
        // does not update its term or grant its vote
        if (heardFromLeader && timeSinceLastMessage() < minElectionTimeout()) {
            logger.info(
                    "{} in term {}: Request for pre-vote received from {}. Voted rejected since we have heard from the leader.",
                    groupId(),
                    currentTerm(),
                    request.getCandidateId());
            return responseFactory().voteRejected(request.getRequestId());
        }
        return responseFactory().voteResponse(request.getRequestId(), voteGrantedFor(request, true));
    }

    private long timeSinceLastMessage() {
        return processing.get() ? 0 : currentTimeMillis() - lastMessage.get();
    }



    private void scheduleElectionTimeoutChecker() {
        schedule(s -> s.schedule(this::checkMessageReceived, minElectionTimeout() / 10, TimeUnit.MILLISECONDS));
    }

    /**
     * Checks if the follower is still in valid state. Follower changes its state to candidate if it has not received a message from leader
     * within a timeout (random value between minElectionTimeout and maxElectionTimeout).
     */
    private void checkMessageReceived() {
        long now = currentTimeMillis();
        if (!processing.get() && nextTimeout.get() < now) {
            String message = format("%s in term %s: Timeout in follower state: %s ms.",
                                    groupId(),
                                    currentTerm(),
                                    now - lastMessage.get());
            logger.info(message);
            changeStateTo(stateFactory().preVoteState(), message);
        } else {
            scheduleElectionTimeoutChecker();
        }
    }

    @Override
    protected void rescheduleElection(long term, long extra) {
        if (term >= currentTerm()) {
            long now = currentTimeMillis();
            lastMessage.set(now);
            nextTimeout.updateAndGet(currentTimeout -> {
                long newTimeout = now + extra + random(minElectionTimeout(), maxElectionTimeout());
                return Math.max(currentTimeout, newTimeout);
            });
        }
    }

    private boolean voteGrantedFor(RequestVoteRequest request, boolean preVote) {
        String type = preVote ? "Pre-vote" : "Vote";
        //1. Reply false if term < currentTerm
        if (request.getTerm() < currentTerm()) {
            logger.info("{} in term {}: {} not granted. Current term is greater than requested {}.",
                        groupId(),
                        currentTerm(),
                        type,
                        request.getTerm());
            return false;
        }

        //2. If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant
        // vote
        String votedFor = votedFor();
        // if this node has role active-backup it participates in the election, but always returns true, except when the
        // term in the request is before the current term
        if (!preVote && votedFor != null && !votedFor.equals(request.getCandidateId())) {
            logger.info("{} in term {}: Vote not granted. Already voted for: {}.", groupId(), currentTerm(), votedFor);
            return false;
        }

        TermIndex lastLog = lastLog();
        if (request.getLastLogTerm() < lastLog.getTerm()) {
            logger.info("{} in term {}: {} not granted. Requested last log term {}, my last log term {}.",
                        groupId(),
                        currentTerm(),
                        type,
                        request.getLastLogTerm(),
                        lastLog.getTerm());
            return false;
        }

        if (request.getLastLogIndex() < lastLog.getIndex()) {
            logger.info("{} in term {}: {}not granted. Requested last log index {}, my last log index {}.",
                        groupId(),
                        currentTerm(),
                        type,
                        request.getLastLogIndex(),
                        lastLog.getIndex());
            return false;
        }

        logger.info("{} in term {}: {} granted for {}.", groupId(), currentTerm(), type, request.getCandidateId());
        if (!preVote) {
            markVotedFor(request.getCandidateId());
        }
        return true;
    }

    /**
     * A Builder for {@link FollowerState}.
     *
     * @author Milan Savic
     * @since 4.1
     */
    public static class Builder extends AbstractMembershipState.Builder<Builder> {

        /**
         * Builds the Follower State.
         *
         * @return the Follower State
         */
        public FollowerState build() {
            return new FollowerState(this);
        }
    }
}
