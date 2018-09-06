package io.axoniq.axonhub.grpc.internal;

import io.axoniq.axonhub.cluster.ClusterController;
import io.axoniq.axonhub.config.MessagingPlatformConfiguration;
import io.axoniq.axonhub.grpc.StubFactory;
import io.axoniq.axonhub.internal.grpc.NodeInfo;
import io.axoniq.axonhub.message.event.EventStoreManager;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * Author: marc
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
                                EventStoreManager eventStoreManager,
                                StubFactory stubFactory) {
        this.clusterController = clusterController;
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
        this.eventStoreManager = eventStoreManager;
        this.stubFactory = stubFactory;
    }

    public Future<Void> addNode(String host, int port) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        logger.debug("Connecting to: {}:{}", host, port);
        try {
            InetAddress.getAllByName(host);
        } catch (UnknownHostException e) {
            future.completeExceptionally(e);
            return future;
        }
        eventStoreManager.stop();
        MessagingClusterServiceInterface stub = stubFactory.messagingClusterServiceStub(messagingPlatformConfiguration, host, port);
        logger.warn("Sending join request: {}", clusterController.getMe().toNodeInfo());
        stub.join(clusterController.getMe().toNodeInfo(), new StreamObserver<NodeInfo>() {
                                       @Override
                                       public void onNext(NodeInfo nodeInfo) {
                                           if( ! messagingPlatformConfiguration.getName().equals(nodeInfo.getNodeName())) {
                                               clusterController.addConnection(nodeInfo);
                                               eventStoreManager.start();
                                           }
                                       }

                                       @Override
                                       public void onError(Throwable throwable) {
                                           logger.warn("Error connecting to {}:{} - {}", host, port, throwable.getMessage());
                                           eventStoreManager.start();
                                           future.completeExceptionally(throwable);
                                       }

                                       @Override
                                       public void onCompleted() {
                                           future.complete(null);
                                       }
                                   });
        return future;
    }
}
