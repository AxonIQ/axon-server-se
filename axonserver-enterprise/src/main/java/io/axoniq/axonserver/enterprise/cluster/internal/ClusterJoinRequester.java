package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.manager.EventStoreManager;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.internal.ConnectResponse;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * Author: marc
 */
@Component
public class ClusterJoinRequester {
    private final ClusterController clusterController;
    private final EventStoreManager eventStoreManager;
    private final StubFactory stubFactory;
    private static final Logger logger = LoggerFactory.getLogger(ClusterJoinRequester.class);

    public ClusterJoinRequester(ClusterController clusterController,
                                Optional<EventStoreManager> eventStoreManager,
                                StubFactory stubFactory) {
        this.clusterController = clusterController;
        this.eventStoreManager = eventStoreManager.orElse(null);
        this.stubFactory = stubFactory;
    }

    public Future<ConnectResponse> addNode(String host, int port, NodeInfo nodeInfo) {
        CompletableFuture<ConnectResponse> future = new CompletableFuture<>();
        logger.debug("Connecting to: {}:{}", host, port);
        try {
            InetAddress.getAllByName(host);
        } catch (UnknownHostException e) {
            future.completeExceptionally(new MessagingPlatformException(ErrorCode.UNKNOWN_HOST, "Unknown host: " + e.getMessage(), e));
            return future;
        }
        eventStoreManager.stopAllContexts();
        MessagingClusterServiceInterface stub = stubFactory.messagingClusterServiceStub(
                    host,
                    port);
        logger.debug("Sending join request: {}", nodeInfo);
        stub.join(nodeInfo, new StreamObserver<ConnectResponse>() {
            private ConnectResponse connectResponse;
                @Override
                public void onNext(ConnectResponse connectResponse) {
                    this.connectResponse = connectResponse;
                }

                @Override
                public void onError(Throwable throwable) {
                    logger.warn("Error connecting to {}:{} - {}", host, port, processMessage(throwable));
                    eventStoreManager.start();
                    future.completeExceptionally(new MessagingPlatformException(ErrorCode.OTHER, "Error processing join request on " + host + ":" + port + ": " + processMessage(throwable), throwable));
                }

                @Override
                public void onCompleted() {
                    stub.closeChannel();
                    connectResponse.getNodesList().forEach(nodeInfo ->
                                                           {
                                                               if( ! nodeInfo.getNodeName().equals(clusterController.getName())) {
                                                                   clusterController.addConnection(nodeInfo,
                                                                                                   connectResponse
                                                                                                           .getGeneration());
                                                               } else {
                                                                   clusterController.setMyContexts(nodeInfo.getContextsList());
                                                               }
                                                           });
                    clusterController.setGeneration(connectResponse.getGeneration());
                    eventStoreManager.start();
                    future.complete(connectResponse);
                }
            });
        return future;
    }

    private String processMessage(Throwable throwable) {
        if( throwable instanceof StatusRuntimeException) {
            StatusRuntimeException statusRuntimeException = (StatusRuntimeException)throwable;
            switch (statusRuntimeException.getStatus().getCode()) {
                case UNAVAILABLE:
                    if( "UNAVAILABLE: Network closed for unknown reason".equals(statusRuntimeException.getMessage()) ) {
                        return "Wrong port. Send join request to internal GRPC port (default 8224)";
                    }
                    if( statusRuntimeException.getCause() != null)
                        return statusRuntimeException.getCause().getMessage();

                    break;
                case UNIMPLEMENTED:
                    return "Wrong port. Send join request to internal GRPC port (default 8224)";

                default:
            }
        }
        return throwable.getMessage();
    }
}
