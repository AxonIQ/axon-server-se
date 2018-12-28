package io.axoniq.axonserver.cluster.grpc;

import io.axoniq.axonserver.cluster.RaftPeer;
import io.axoniq.axonserver.cluster.Registration;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.LeaderElectionServiceGrpc;
import io.axoniq.axonserver.grpc.cluster.LogReplicationServiceGrpc;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class GrpcRaftPeer implements RaftPeer {
    private static final Logger logger = LoggerFactory.getLogger(GrpcRaftPeer.class);
    public final Node node;
    private final AtomicReference<AppendEntriesStream> appendEntiesStreamRef = new AtomicReference<>();
    private final AtomicReference<Consumer<AppendEntriesResponse>> appendEntriesResponseListener = new AtomicReference<>();

    private final AtomicReference<InstallSnapshotStream> installSnapshotStreamRef = new AtomicReference<>();
    private final AtomicReference<Consumer<InstallSnapshotResponse>> installSnapshotResponseListener = new AtomicReference<>();

    public GrpcRaftPeer(Node node) {
        this.node = node;
    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
        logger.debug("{} Send: {}", node.getNodeId(), request);
        CompletableFuture<RequestVoteResponse> response = new CompletableFuture<>();
        LeaderElectionServiceGrpc.LeaderElectionServiceStub stub = createLeaderElectionStub(node);
        stub.requestVote(request, new StreamObserver<RequestVoteResponse>() {
            @Override
            public void onNext(RequestVoteResponse requestVoteResponse) {
                logger.debug("{} received: {}", node.getNodeId(), requestVoteResponse);
                response.complete(requestVoteResponse);
            }

            @Override
            public void onError(Throwable cause) {
                logger.warn( "{}: Received error on vote - {}", node, cause.getMessage());
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
        return LeaderElectionServiceGrpc.newStub(getManagedChannel(node));
    }

    @Override
    public void appendEntries(AppendEntriesRequest request) {
        AppendEntriesStream appendEntriesStream = getAppendEntriesStream();
        appendEntriesStream.onNext(request);
    }

    private AppendEntriesStream getAppendEntriesStream() {
        appendEntiesStreamRef.compareAndSet(null, new AppendEntriesStream());
        return appendEntiesStreamRef.get();
    }

    @Override
    public void installSnapshot(InstallSnapshotRequest request) {
        InstallSnapshotStream installSnapshotStream = getInstallSnapshotStream();
        installSnapshotStream.onNext(request);
    }

    private InstallSnapshotStream getInstallSnapshotStream() {
        installSnapshotStreamRef.compareAndSet(null, new InstallSnapshotStream());
        return installSnapshotStreamRef.get();
    }

    @Override
    public Registration registerAppendEntriesResponseListener(Consumer<AppendEntriesResponse> listener) {
        appendEntriesResponseListener.set(listener);
        return () -> appendEntriesResponseListener.set(null);
    }

    @Override
    public Registration registerInstallSnapshotResponseListener(Consumer<InstallSnapshotResponse> listener) {
        installSnapshotResponseListener.set(listener);
        return () -> installSnapshotResponseListener.set(null);
    }

    @Override
    public String nodeId() {
        return node.getNodeId();
    }

    private class InstallSnapshotStream {

        private final AtomicReference<StreamObserver<InstallSnapshotRequest>> requestStreamRef = new AtomicReference<>();

        public void onNext(InstallSnapshotRequest request) {
            logger.trace("{} Send {}", node.getNodeId(), request);
            requestStreamRef.compareAndSet(null, initStreamObserver());
            StreamObserver<InstallSnapshotRequest> stream = requestStreamRef.get();
//            synchronized (requestStreamRef.get()) {
            if( stream != null) {
                stream.onNext(request);
            }
//            }
        }

        private StreamObserver<InstallSnapshotRequest> initStreamObserver() {
            LogReplicationServiceGrpc.LogReplicationServiceStub stub = LogReplicationServiceGrpc.newStub(
                    getManagedChannel(node));
            return stub.installSnapshot(new StreamObserver<InstallSnapshotResponse>() {
                @Override
                public void onNext(InstallSnapshotResponse installSnapshotResponse) {
                    if( installSnapshotResponse.hasFailure()) {
                        requestStreamRef.get().onCompleted();
                        requestStreamRef.set(null);
                    }
                    logger.trace("{}: Received {}", node.getNodeId(), installSnapshotResponse);
                    if( installSnapshotResponseListener.get() != null) {
                        installSnapshotResponseListener.get().accept(installSnapshotResponse);
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    requestStreamRef.set(null);
                }

                @Override
                public void onCompleted() {
                    if( requestStreamRef.get() != null) {
                        requestStreamRef.get().onCompleted();
                        requestStreamRef.set(null);
                    }
                }
            });
        }
    }


    private class AppendEntriesStream {

        private final AtomicReference<StreamObserver<AppendEntriesRequest>> requestStreamRef = new AtomicReference<>();

        public void onNext(AppendEntriesRequest request) {
            logger.trace("{} Send {}", node.getNodeId(), request);
            requestStreamRef.updateAndGet(current -> current == null ?  initStreamObserver(): current);

            StreamObserver<AppendEntriesRequest> stream = requestStreamRef.get();
//            synchronized (requestStreamRef.get()) {
            if( stream != null) {
                stream.onNext(request);
            }
//            }
        }

        private StreamObserver<AppendEntriesRequest> initStreamObserver() {
            LogReplicationServiceGrpc.LogReplicationServiceStub stub = LogReplicationServiceGrpc.newStub(
                    getManagedChannel(node));
            logger.info("initStreamObserver {}", requestStreamRef);
            return stub.appendEntries(new StreamObserver<AppendEntriesResponse>() {
                @Override
                public void onNext(AppendEntriesResponse appendEntriesResponse) {
                    if( appendEntriesResponse.hasFailure()) {
                        requestStreamRef.get().onCompleted();
                        requestStreamRef.set(null);
                    }
                    logger.trace("{}: Received {}", node.getNodeId(), appendEntriesResponse);
                    if( appendEntriesResponseListener.get() != null) {
                        appendEntriesResponseListener.get().accept(appendEntriesResponse);
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    requestStreamRef.set(null);
                }

                @Override
                public void onCompleted() {
                    if( requestStreamRef.get() != null) {
                        requestStreamRef.get().onCompleted();
                        requestStreamRef.set(null);
                    }
                }
            });
        }
    }

    private static final Map<String, ManagedChannel> channelMap = new ConcurrentHashMap<>();

    private static ManagedChannel getManagedChannel(Node node) {
        return channelMap.computeIfAbsent(node.getNodeId(), n -> NettyChannelBuilder.forAddress(node.getHost(), node.getPort()).usePlaintext().directExecutor().build());
    }

    @Override
    public Node toNode() {
        return node;
    }
}
