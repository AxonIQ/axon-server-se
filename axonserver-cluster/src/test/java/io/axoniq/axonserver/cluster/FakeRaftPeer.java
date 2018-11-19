package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.AppendEntryFailure;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotFailure;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Sara Pellegrini
 * @since
 */
public class FakeRaftPeer implements RaftPeer {

    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(2);
    private final String nodeId;
    private long term;
    private boolean voteGranted;

    public FakeRaftPeer(String nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest request) {
        CompletableFuture<AppendEntriesResponse> result = new CompletableFuture<>();
        AppendEntriesResponse response = AppendEntriesResponse.newBuilder()
                                                              .setTerm(term)
                                                              .setFailure(AppendEntryFailure.newBuilder().build())
                                                              .build();
        executorService.schedule(() -> result.complete(response), 10, TimeUnit.MILLISECONDS);
        return result;
    }

    @Override
    public CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest request) {
        CompletableFuture<InstallSnapshotResponse> result = new CompletableFuture<>();
        InstallSnapshotResponse response = InstallSnapshotResponse.newBuilder()
                                                                  .setTerm(term)
                                                                  .setFailure(InstallSnapshotFailure.newBuilder().build())
                                                                  .build();
        executorService.schedule(() -> result.complete(response), 10, TimeUnit.MILLISECONDS);
        return result;
    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
        CompletableFuture<RequestVoteResponse> result = new CompletableFuture<>();
        RequestVoteResponse response = RequestVoteResponse.newBuilder()
                                                          .setTerm(term)
                                                          .setVoteGranted(voteGranted)
                                                          .build();
        executorService.schedule(() -> result.complete(response), 10, TimeUnit.MILLISECONDS);
        return result;
    }

    @Override
    public String nodeId() {
        return nodeId;
    }

    public long term() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }

    public void setVoteGranted(boolean voteGranted) {
        this.voteGranted = voteGranted;
    }
}
