package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import io.axoniq.axonserver.grpc.cluster.Role;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Marc Gathier
 * @since 4.3
 */
public class ProspectState extends BaseFollowerState {

    private static final Logger logger = LoggerFactory.getLogger(ProspectState.class);

    protected ProspectState(Builder builder) {
        super(builder);
    }

    @Override
    public RequestVoteResponse requestPreVote(RequestVoteRequest request) {
        return responseFactory().voteRejected(request.getRequestId());
    }

    @Override
    public RequestVoteResponse requestVote(RequestVoteRequest request) {
        return responseFactory().voteRejected(request.getRequestId());
    }


    @Override
    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        currentConfiguration().refresh();
        Node currentNode = currentNode();
        if (currentNode == null) {
            logger.info("{} in term {}: Current node is empty", groupId(), currentTerm());
            return super.appendEntries(request);
        }
        logger.trace("{} in term {}: Role: {}", groupId(), currentTerm(), currentNode().getRole());
        if (currentNode.getRole().equals(Role.PRIMARY)) {
            return handleAsFollower(f -> f.appendEntries(request), "Found node as primary");
        }

        return handleAsSecondary(f -> f.appendEntries(request), "Found node as " + currentNode.getRole());
    }

    @Override
    public InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request) {
        if (Role.PRIMARY.equals(request.getPeerRole())) {
            return handleAsFollower(f -> f.installSnapshot(request), "Received node as primary");
        }
        return handleAsSecondary(f -> f.installSnapshot(request), "Found node as " + request.getPeerRole());
    }

    /**
     * A Builder for {@link ProspectState}.
     *
     * @author Marc Gathier
     * @since 4.3
     */
    public static class Builder extends AbstractMembershipState.Builder<Builder> {

        /**
         * Builds the Prospect State.
         *
         * @return the Prospect State
         */
        public ProspectState build() {
            return new ProspectState(this);
        }
    }

    /**
     * Instantiates a new builder for the Prospect State.
     *
     * @return a new builder for the Prospect State
     */
    public static Builder builder() {
        return new Builder();
    }
}
