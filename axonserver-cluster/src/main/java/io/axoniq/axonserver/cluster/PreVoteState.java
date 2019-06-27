package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.configuration.CandidateConfiguration;
import io.axoniq.axonserver.cluster.configuration.ClusterConfiguration;
import io.axoniq.axonserver.cluster.election.Election.Result;
import io.axoniq.axonserver.cluster.scheduler.Scheduler;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

import static java.lang.String.format;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Performs all actions when the node is in the Candidate state.
 *
 * @author Sara Pellegrini
 * @since 4.1
 */
public class PreVoteState extends AbstractMembershipState {

    private static final Logger logger = LoggerFactory.getLogger(PreVoteState.class);
    private final ClusterConfiguration clusterConfiguration = new CandidateConfiguration();
    private final AtomicReference<Scheduler> scheduler = new AtomicReference<>();
    private volatile boolean disruptAllowed;

    private PreVoteState(Builder builder) {
        super(builder);
    }

    /**
     * Instantiates a new builder for the Candidate State.
     *
     * @return a new builder for the Candidate State
     */
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
        disruptAllowed = false;
    }

    @Override
    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        if (request.getTerm() >= currentTerm()) {
            logger.info("{} in term {}: Received term {} which is greater or equals than mine. Moving to Follower...",
                        groupId(),
                        currentTerm(),
                        request.getTerm());
            String message = format("%s received AppendEntriesRequest with greater or equals term (%s >= %s) from %s",
                                    me(),
                                    request.getTerm(),
                                    currentTerm(),
                                    request.getLeaderId());
            return handleAsFollower(follower -> follower.appendEntries(request), message);
        }
        logger.info("{} in term {}: Received term {} is smaller than mine. Rejecting the request.",
                    groupId(),
                    currentTerm(),
                    request.getTerm());
        return responseFactory().appendEntriesFailure(request.getRequestId(),
                                                      "Request rejected because term is smaller than mine");
    }

    private void resetElectionTimeout() {
        int timeout = random(minElectionTimeout(), maxElectionTimeout() + 1);
        ofNullable(scheduler.get()).ifPresent(s -> s.schedule(this::startElection, timeout, MILLISECONDS));
    }

    private void startElection() {
        if (currentConfiguration().isEmpty()) {
            logger.info("{} in term {}: Not able to start election. Current configuration is empty.",
                        groupId(),
                        currentTerm());
            return;
        }
        newPreVote().result().subscribe(this::onElectionResult,
                                        error -> logger.warn("{} in term {}: Failed to run election. {}",
                                                             groupId(),
                                                             currentTerm(),
                                                             error));
        resetElectionTimeout();
    }

    private void onElectionResult(Result result) {
        if (result.won()) {
            changeStateTo(stateFactory().candidateState(), result.cause());
        } else if (result.goAway()) {
            changeStateTo(stateFactory().removedState(), result.cause());
        } else {
            changeStateTo(stateFactory().followerState(), result.cause());
        }
    }

    /**
     * A Builder for {@link PreVoteState}.
     *
     * @author Sara Pellegrini
     * @since 4.1
     */
    public static class Builder extends AbstractMembershipState.Builder<Builder> {

        /**
         * Builds the Candidate State.
         *
         * @return the Candidate State
         */
        public PreVoteState build() {
            return new PreVoteState(this);
        }
    }
}
