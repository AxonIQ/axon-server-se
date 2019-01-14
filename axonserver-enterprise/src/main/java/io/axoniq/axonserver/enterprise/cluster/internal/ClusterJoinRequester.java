package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.manager.EventStoreManager;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
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
 * @author Marc Gathier
 */
@Component
public class ClusterJoinRequester {
    private final ClusterController clusterController;
    private final MessagingPlatformConfiguration messagingPlatformConfiguration;
    private final EventStoreManager eventStoreManager;
    private final StubFactory stubFactory;
    private static final Logger logger = LoggerFactory.getLogger(ClusterJoinRequester.class);

    public ClusterJoinRequester(ClusterController clusterController,
                                MessagingPlatformConfiguration messagingPlatformConfiguration,
                                Optional<EventStoreManager> eventStoreManager,
                                StubFactory stubFactory) {
        this.clusterController = clusterController;
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
        this.eventStoreManager = eventStoreManager.orElse(null);
        this.stubFactory = stubFactory;
    }

    public Future<Void> addNode(String host, int port) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        logger.debug("Connecting to: {}:{}", host, port);
        try {
            InetAddress.getAllByName(host);
        } catch (UnknownHostException e) {
            future.completeExceptionally(new MessagingPlatformException(ErrorCode.UNKNOWN_HOST, "Unknown host: " + e.getMessage(), e));
            return future;
        }
        eventStoreManager.stop();
        MessagingClusterServiceInterface stub = stubFactory.messagingClusterServiceStub(
                    messagingPlatformConfiguration,
                    host,
                    port);
        logger.debug("Sending join request: {}", clusterController.getMe().toNodeInfo());
        stub.join(clusterController.getMe().toNodeInfo(), new StreamObserver<NodeInfo>() {
                @Override
                public void onNext(NodeInfo nodeInfo) {
                    if (!messagingPlatformConfiguration.getName().equals(nodeInfo.getNodeName())) {
                        clusterController.addConnection(nodeInfo, true);
                        eventStoreManager.start();
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    logger.warn("Error connecting to {}:{} - {}", host, port, processMessage(throwable));
                    eventStoreManager.start();
                    future.completeExceptionally(new MessagingPlatformException(ErrorCode.OTHER, "Error processing join request on " + host + ":" + port + ": " + processMessage(throwable), throwable));
                }

                @Override
                public void onCompleted() {
                    future.complete(null);
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
