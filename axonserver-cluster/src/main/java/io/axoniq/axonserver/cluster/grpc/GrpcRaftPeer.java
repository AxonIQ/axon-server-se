package io.axoniq.axonserver.cluster.grpc;

import io.axoniq.axonserver.cluster.RaftPeer;
import io.axoniq.axonserver.cluster.Registration;
import io.axoniq.axonserver.cluster.exception.ErrorCode;
import io.axoniq.axonserver.cluster.exception.LogException;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.LeaderElectionServiceGrpc;
import io.axoniq.axonserver.grpc.cluster.LogReplicationServiceGrpc;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import io.axoniq.axonserver.grpc.cluster.TimeoutNowRequest;
import io.axoniq.axonserver.grpc.cluster.TimeoutNowResponse;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * @author Marc Gathier
 * @since 4.1
 */
public class GrpcRaftPeer implements RaftPeer {
    private static final Logger logger = LoggerFactory.getLogger(GrpcRaftPeer.class);
    private final String raftGroup;
    public final Node node;
    private final GrpcRaftClientFactory clientFactory;
    private final long idleConnectionTimeout;
    private final AtomicReference<AppendEntriesStream> appendEntiesStreamRef = new AtomicReference<>();
    private final AtomicReference<Consumer<AppendEntriesResponse>> appendEntriesResponseListener = new AtomicReference<>();

    private final AtomicReference<InstallSnapshotStream> installSnapshotStreamRef = new AtomicReference<>();
    private final AtomicReference<Consumer<InstallSnapshotResponse>> installSnapshotResponseListener = new AtomicReference<>();

    /**
     * Constuctor for a {@link RaftPeer} that communicates over gRPC.
     * @param raftGroup the name of the raft group
     * @param node the node information of the remote peer
     */
    public GrpcRaftPeer(String raftGroup, Node node) {
        this(raftGroup, node, new DefaultGrpcRaftClientFactory(), 5000);
    }

    /**
     * Constuctor for a {@link RaftPeer} that communicates over gRPC.
     * @param raftGroup the name of the raft group
     * @param node the node information of the remote peer
     * @param clientFactory factory to create client stubs
     * @param idleConnectionTimeout timeout to mark stream as idle
     */
    public GrpcRaftPeer(String raftGroup, Node node, GrpcRaftClientFactory clientFactory, long idleConnectionTimeout) {
        this.raftGroup = raftGroup;
        this.node = node;
        this.clientFactory = clientFactory;
        this.idleConnectionTimeout = idleConnectionTimeout;
    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
        logger.debug("{} Send: {}", node.getNodeId(), request);
        CompletableFuture<RequestVoteResponse> response = new CompletableFuture<>();
        LeaderElectionServiceGrpc.LeaderElectionServiceStub stub = clientFactory.createLeaderElectionStub(node);
        stub.requestVote(request, completableStreamObserver(response));
        return response;
    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestPreVote(RequestVoteRequest request) {
        CompletableFuture<RequestVoteResponse> response = new CompletableFuture<>();
        LeaderElectionServiceGrpc.LeaderElectionServiceStub stub = clientFactory.createLeaderElectionStub(node);
        stub.requestPreVote(request, completableStreamObserver(response));
        return response;
    }

    @NotNull
    private <T> StreamObserver<T> completableStreamObserver(
            CompletableFuture<T> response) {
        return new StreamObserver<T>() {
            @Override
            public void onNext(T requestVoteResponse) {
                logger.debug("{} received: {}", node.getNodeId(), requestVoteResponse);
                response.complete(requestVoteResponse);
            }

            @Override
            public void onError(Throwable cause) {
                logger.warn("{}: Received error on vote - {}", node.getNodeId(), cause.getMessage());
                response.completeExceptionally(cause);
            }

            @Override
            public void onCompleted() {
                if (!response.isDone()) {
                    response.completeExceptionally(new Throwable("Request closed without result"));
                }
            }
        };
    }

    @Override
    public void sendTimeoutNow() {
        LogReplicationServiceGrpc.LogReplicationServiceStub stub = clientFactory.createLogReplicationServiceStub(node);
        stub.timeoutNow(TimeoutNowRequest.newBuilder()
                                         .setGroupId(raftGroup)
                                         .build(), new StreamObserver<TimeoutNowResponse>() {
            @Override
            public void onNext(TimeoutNowResponse value) {

            }

            @Override
            public void onError(Throwable cause) {
                logger.warn( "{}: Received error on timeoutNow - {}", node.getNodeId(), cause.getMessage());
            }

            @Override
            public void onCompleted() {

            }
        });
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

    @Override
    public String nodeName() {
        return node.getNodeName();
    }

    @Override
    public boolean isReadyForAppendEntries() {
        return appendEntiesStreamRef.get() == null || appendEntiesStreamRef.get().isReady();
    }

    @Override
    public boolean isReadyForSnapshot() {
        return installSnapshotStreamRef.get() == null || installSnapshotStreamRef.get().isReady();
    }

    private class InstallSnapshotStream {

        private final AtomicReference<CallStreamObserver<InstallSnapshotRequest>> requestStreamRef = new AtomicReference<>();

        public void onNext(InstallSnapshotRequest request) {
            logger.trace("{} Send InstallSnapshot {}", node.getNodeId(), request.getOffset());
            requestStreamRef.compareAndSet(null, initStreamObserver());
            StreamObserver<InstallSnapshotRequest> stream = requestStreamRef.get();
            if( stream != null) {
                synchronized (requestStreamRef) {
                    send(stream, request);
                }
            }
        }

        private CallStreamObserver<InstallSnapshotRequest> initStreamObserver() {
            LogReplicationServiceGrpc.LogReplicationServiceStub stub = clientFactory.createLogReplicationServiceStub(node);
            return (CallStreamObserver<InstallSnapshotRequest>) stub.installSnapshot(new ClientResponseObserver<InstallSnapshotRequest,InstallSnapshotResponse>() {
                @Override
                public void beforeStart(ClientCallStreamObserver<InstallSnapshotRequest> clientCallStreamObserver) {
                    clientCallStreamObserver.setOnReadyHandler(() -> logger.trace("{}: install snapshot ready", node.getNodeId()));
                }

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
                    logger.debug("Error on InstallSnapshot stream", throwable);
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

        public boolean isReady() {
            return requestStreamRef.get() == null || requestStreamRef.get().isReady();
        }
    }


    private class AppendEntriesStream {

        private final AtomicReference<CallStreamObserver<AppendEntriesRequest>> requestStreamRef = new AtomicReference<>();
        private final AtomicLong lastMessageReceived = new AtomicLong();

        public void onNext(AppendEntriesRequest request) {
            logger.trace("{} Send appendEntries - {}", node.getNodeId(), request.getPrevLogIndex() + 1);
            requestStreamRef.updateAndGet(current -> current == null || noMessagesReceived() ?  initStreamObserver(): current);

            StreamObserver<AppendEntriesRequest> stream = requestStreamRef.get();
            if( stream != null) {
                synchronized (requestStreamRef) {
                    send(stream, request);
                }
            } else {
                logger.warn("{}: Not sending AppendEntriesRequest {}", node.getNodeId(), request.getPrevLogIndex() + 1);
            }
        }

        private boolean noMessagesReceived() {
            return lastMessageReceived.get() < System.currentTimeMillis() - idleConnectionTimeout;
        }


        private CallStreamObserver<AppendEntriesRequest> initStreamObserver() {
            lastMessageReceived.set(System.currentTimeMillis());
            LogReplicationServiceGrpc.LogReplicationServiceStub stub = clientFactory.createLogReplicationServiceStub(node);
            return (CallStreamObserver<AppendEntriesRequest>) stub.appendEntries(new ClientResponseObserver<AppendEntriesRequest, AppendEntriesResponse>() {

                @Override
                public void beforeStart(ClientCallStreamObserver<AppendEntriesRequest> clientCallStreamObserver) {
                    clientCallStreamObserver.setOnReadyHandler(() -> logger.trace("{}: append entries ready", node.getNodeId()));
                }

                @Override
                public void onNext(AppendEntriesResponse appendEntriesResponse) {
                    lastMessageReceived.set(System.currentTimeMillis());
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
                    lastMessageReceived.set(0);
                    logger.debug("Error on AppendEntries stream", throwable);
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

        public boolean isReady() {
            return requestStreamRef.get() == null || requestStreamRef.get().isReady();
        }
    }

    /**
     * Sends next message on a stream. As gRPC stream onNext is not thread-safe needs to be synchronized over the stream
     * @param stream the stream to send the message
     * @param request the message to send
     * @param <T> type of message
     */
    private <T> void send(StreamObserver<T> stream, T request) {
        try {
            synchronized (stream) {
                stream.onNext(request);
            }
        } catch( Throwable e) {
            logger.warn("Error while sending message: {}", e.getMessage(), e);
            try {
                // Cancel RPC
                stream.onError(e);
            } catch (Throwable ex) {
                // Ignore further exception on cancelling the RPC
            }
            throw new LogException(ErrorCode.SENDING_FAILED, e.getMessage());
        }
    }
}
