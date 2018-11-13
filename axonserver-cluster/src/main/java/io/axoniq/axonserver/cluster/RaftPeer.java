package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;

import java.util.concurrent.Future;

/**
 * Author: marc
 */
public interface RaftPeer {
    Future<RequestVoteResponse> requestVote(RequestVoteRequest request);

}
