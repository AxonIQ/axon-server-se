package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.*;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

public class GrpcRaftPeer implements RaftPeer {
    public final Node node;
    private final AtomicReference<AppendEntriesStream> appendEntiesStreamRef = new AtomicReference<>();

    public GrpcRaftPeer(Node node) {
        this.node = node;
    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
        CompletableFuture<RequestVoteResponse> response = new CompletableFuture<>();
        LeaderElectionServiceGrpc.LeaderElectionServiceStub stub = createLeaderElectionStub(node);
        stub.requestVote(request, new StreamObserver<RequestVoteResponse>() {
            @Override
            public void onNext(RequestVoteResponse requestVoteResponse) {
                response.complete(requestVoteResponse);
            }

            @Override
            public void onError(Throwable cause) {
                response.completeExceptionally(cause);

            }

            @Override
            public void onCompleted() {
                if(! response.isDone()) {
                    response.completeExceptionally(new Throwable("Request closed without result"));
                }
            }
        });
        return response;
    }

    private LeaderElectionServiceGrpc.LeaderElectionServiceStub createLeaderElectionStub(Node node) {
        return null;
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest request) {
        CompletableFuture<AppendEntriesResponse> response = new CompletableFuture<>();
        try {
            AppendEntriesStream appendEntriesStream = getAppendEntriesStream();
            appendEntriesStream.onNext(request, response);
        } catch (RuntimeException ex) {
            response.completeExceptionally(ex);
        }
        return response;
    }

    private AppendEntriesStream getAppendEntriesStream() {
        appendEntiesStreamRef.compareAndSet(null, new AppendEntriesStream());
        return appendEntiesStreamRef.get();
    }

    @Override
    public CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest request) {
        return null;
    }

    @Override
    public String nodeId() {
        return node.getNodeId();
    }

    private class AppendEntriesStream {
        public void onNext(AppendEntriesRequest request, CompletableFuture<AppendEntriesResponse> response) {

        }
    }
}
