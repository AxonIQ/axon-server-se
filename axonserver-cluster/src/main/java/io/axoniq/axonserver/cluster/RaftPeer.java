package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public interface RaftPeer {

    int sendNextEntries();

    void send(AppendEntriesRequest heartbeat);

    CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request);

    String nodeId();

    Registration registerMatchIndexListener(Consumer<Long> matchIndexListener);

    long getMatchIndex();
}
