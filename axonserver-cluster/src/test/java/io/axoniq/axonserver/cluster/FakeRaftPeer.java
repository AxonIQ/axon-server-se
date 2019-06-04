package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.scheduler.Scheduler;
import io.axoniq.axonserver.grpc.cluster.*;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * @author Sara Pellegrini
 */
public class FakeRaftPeer implements RaftPeer {

    private final Scheduler scheduler;
    private final String nodeId;
    private long term;
    private boolean voteGranted;
    private Consumer<AppendEntriesResponse> appendEntriesResponseConsumer = response -> {
    };
    private Consumer<InstallSnapshotResponse> installSnapshotResponseConsumer = response -> {
    };

    public FakeRaftPeer(Scheduler scheduler, String nodeId) {
        this.scheduler = scheduler;
        this.nodeId = nodeId;
    }

    @Override
    public void appendEntries(AppendEntriesRequest request) {
        AppendEntriesResponse response;
        if (request.getEntriesCount() > 0) {
            response = AppendEntriesResponse.newBuilder()
                                            .setResponseHeader(responseHeader(request.getRequestId()))
                                            .setTerm(term)
                                            .setSuccess(
                                                    AppendEntrySuccess
                                                            .newBuilder()
                                                            .setLastLogIndex(
                                                                    request.getEntries(request.getEntriesCount() - 1).getIndex())
                                                            .build())
                                            .build();
        } else {
            response = AppendEntriesResponse.newBuilder()
                                            .setResponseHeader(responseHeader(request.getRequestId()))
                                            .setTerm(term)
                                            .setFailure(AppendEntryFailure.newBuilder().build())
                                            .build();
        }
        scheduler.schedule(() -> appendEntriesResponseConsumer.accept(response), 10, TimeUnit.MILLISECONDS);
    }

    @Override
    public void installSnapshot(InstallSnapshotRequest request) {
        InstallSnapshotResponse response = InstallSnapshotResponse.newBuilder()
                                                                  .setResponseHeader(responseHeader(request.getRequestId()))
                                                                  .setTerm(term)
                                                                  .setFailure(InstallSnapshotFailure.newBuilder()
                                                                                                    .build())
                                                                  .build();
        scheduler.schedule(() -> installSnapshotResponseConsumer.accept(response), 10, TimeUnit.MILLISECONDS);
    }

    private ResponseHeader responseHeader(String requestId) {
        return ResponseHeader.newBuilder()
                             .setRequestId(requestId)
                             .setResponseId(UUID.randomUUID().toString())
                             .setNodeId(nodeId)
                             .build();
    }

    @Override
    public Registration registerAppendEntriesResponseListener(Consumer<AppendEntriesResponse> listener) {
        appendEntriesResponseConsumer = listener;
        return () -> appendEntriesResponseConsumer = response -> {
        };
    }

    @Override
    public Registration registerInstallSnapshotResponseListener(Consumer<InstallSnapshotResponse> listener) {
        installSnapshotResponseConsumer = listener;
        return () -> installSnapshotResponseConsumer = response -> {
        };
    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
        CompletableFuture<RequestVoteResponse> result = new CompletableFuture<>();
        RequestVoteResponse response = RequestVoteResponse.newBuilder()
                                                          .setResponseHeader(responseHeader(request.getRequestId()))
                                                          .setTerm(term)
                                                          .setVoteGranted(voteGranted)
                                                          .build();
        scheduler.schedule(() -> result.complete(response), 10, TimeUnit.MILLISECONDS);
        return result;
    }

    @Override
    public String nodeId() {
        return nodeId;
    }

    @Override
    public void sendTimeoutNow() {

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
