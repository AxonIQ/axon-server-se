package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;

/**
 * Interface responsible for the creation of the responses to RAFT apis.
 *
 * @author Sara Pellegrini
 * @since 4.1.5
 */
public interface RaftResponseFactory {

    /**
     * Builds an {@link AppendEntriesResponse} to confirm that
     * the {@link io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest} has been successfully processed.
     *
     * @param requestId    the AppendEntriesRequest identifier
     * @param lastLogIndex the last log index
     * @return the successful {@link AppendEntriesResponse}
     */
    AppendEntriesResponse appendEntriesSuccess(String requestId, long lastLogIndex);

    /**
     * Builds an {@link AppendEntriesResponse} to notify that
     * the {@link io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest} has not been correctly processed.
     *
     * @param requestId    the AppendEntriesRequest identifier
     * @param failureCause the cause of the failure
     * @return the failure {@link AppendEntriesResponse}
     */
    default AppendEntriesResponse appendEntriesFailure(String requestId, String failureCause) {
        return appendEntriesFailure(requestId, failureCause, false);
    }

    AppendEntriesResponse appendEntriesFailure(String requestId, String failureCause, boolean fatal);
    /**
     * Builds an {@link InstallSnapshotResponse} to confirm that
     * the {@link io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest} has been successfully processed.
     *
     * @param requestId the InstallSnapshotRequest identifier
     * @param offset    the InstallSnapshotRequest offset
     * @return the successful {@link InstallSnapshotResponse}
     */
    InstallSnapshotResponse installSnapshotSuccess(String requestId, int offset);

    /**
     * Builds an {@link InstallSnapshotResponse} to notify that
     * the {@link io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest} has not been correctly processed.
     *
     * @param requestId    the InstallSnapshotRequest identifier
     * @param failureCause the cause of the failure
     * @return the failure {@link InstallSnapshotResponse}
     */
    InstallSnapshotResponse installSnapshotFailure(String requestId, String failureCause);

    /**
     * Builds a {@link RequestVoteResponse} with the specified parameters.
     *
     * @param requestId   the RequestVoteRequest identifier
     * @param voteGranted true if the vote is granted, false otherwise
     * @param goAway      true to avoid future requests from this node
     * @return the {@link RequestVoteResponse}
     */
    RequestVoteResponse voteResponse(String requestId, boolean voteGranted, boolean goAway);

    /**
     * Builds a {@link RequestVoteResponse} with the specified parameters.
     *
     * @param requestId   the RequestVoteRequest identifier
     * @param voteGranted true if the vote is granted, false otherwise
     * @return the {@link RequestVoteResponse}
     */
    default RequestVoteResponse voteResponse(String requestId, boolean voteGranted) {
        return voteResponse(requestId, voteGranted, false);
    }

    /**
     * Builds a {@link RequestVoteResponse} that reject the request for vote.
     *
     * @param requestId the RequestVoteRequest identifier
     * @return the {@link RequestVoteResponse}
     */
    default RequestVoteResponse voteRejected(String requestId) {
        return voteRejected(requestId, false);
    }

    /**
     * Builds a {@link RequestVoteResponse} that reject the request for vote.
     *
     * @param requestId the RequestVoteRequest identifier
     * @param goAway    true to avoid future requests from this node
     * @return the {@link RequestVoteResponse}
     */
    default RequestVoteResponse voteRejected(String requestId, boolean goAway) {
        return voteResponse(requestId, false, goAway);
    }
}
