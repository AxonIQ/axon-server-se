package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.election.Election.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performs all actions when the node is in the Candidate state.
 *
 * @author Sara Pellegrini
 * @since 4.1
 */
public class CandidateState extends VotingState {

    private static final Logger logger = LoggerFactory.getLogger(CandidateState.class);
    private volatile boolean disruptAllowed;

    private CandidateState(Builder builder) {
        super(builder, logger);
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
    public void stop() {
        super.stop();
        disruptAllowed = false;
    }


    protected void startElection() {
        if (currentConfiguration().isEmpty()) {
            logger.info("{} in term {}: Not able to start election. Current configuration is empty.",
                        groupId(),
                        currentTerm());
            return;
        }
        newElection(disruptAllowed).result().subscribe(this::onElectionResult,
                                         error -> logger.warn("{} in term {}: Failed to run election. {}",
                                                              groupId(),
                                                              currentTerm(),
                                                              error));
        resetElectionTimeout();
    }

    private void onElectionResult(Result result) {
        if (result.won()) {
            changeStateTo(stateFactory().leaderState(), result.cause());
        } else if (result.goAway()) {
            changeStateTo(stateFactory().removedState(), result.cause());
        } else {
            changeStateTo(stateFactory().followerState(), result.cause());
        }
    }

    public MembershipState withDisruptAllowed() {
        disruptAllowed = true;
        return this;
    }

    /**
     * A Builder for {@link CandidateState}.
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
        public CandidateState build() {
            return new CandidateState(this);
        }
    }
}
