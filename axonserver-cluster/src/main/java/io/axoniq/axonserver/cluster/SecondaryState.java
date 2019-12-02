package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;

/**
 * @author Marc Gathier
 * @since 4.3
 */
public class SecondaryState extends BaseFollowerState {

    protected SecondaryState(Builder builder) {
        super(builder);
    }

    @Override
    public RequestVoteResponse requestPreVote(RequestVoteRequest request) {
        return responseFactory().voteResponse(request.getRequestId(), true);
    }

    @Override
    public RequestVoteResponse requestVote(RequestVoteRequest request) {
        return responseFactory().voteResponse(request.getRequestId(), true);
    }


    /**
     * A Builder for {@link SecondaryState}.
     *
     * @author Marc Gathier
     * @since 4.3
     */
    public static class Builder extends AbstractMembershipState.Builder<Builder> {

        /**
         * Builds the Secondary State.
         *
         * @return the Secondary State
         */
        public SecondaryState build() {
            return new SecondaryState(this);
        }
    }

    /**
     * Instantiates a new builder for the Secondary State.
     *
     * @return a new builder for the Secondary State
     */
    public static Builder builder() {
        return new Builder();
    }
}
