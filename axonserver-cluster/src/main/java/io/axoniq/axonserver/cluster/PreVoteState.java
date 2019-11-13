package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.election.Election.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performs all actions when the node is in the Pre-Vote state.
 *
 * @author Marc Gathier
 * @since 4.2
 */
public class PreVoteState extends VotingState {
    private static final Logger logger = LoggerFactory.getLogger(PreVoteState.class);

    private PreVoteState(Builder builder) {
        super(builder, logger);
    }

    /**
     * Instantiates a new builder for the Pre-Vote State.
     *
     * @return a new builder for the Pre-Vote State
     */
    public static Builder builder() {
        return new Builder();
    }


    protected void startElection() {
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
        if (!running) {
            return;
        }
        if (result.won()) {
            changeStateTo(stateFactory().candidateState(), result.cause());
        } else {
            changeStateTo(stateFactory().followerState(), result.cause());
        }
    }

    /**
     * A Builder for {@link PreVoteState}.
     *
     * @author Marc Gathier
     * @since 4.2
     */
    public static class Builder extends AbstractMembershipState.Builder<Builder> {

        /**
         * Builds the Pre-Vote State.
         *
         * @return the Pre-Vote State
         */
        public PreVoteState build() {
            return new PreVoteState(this);
        }
    }
}
