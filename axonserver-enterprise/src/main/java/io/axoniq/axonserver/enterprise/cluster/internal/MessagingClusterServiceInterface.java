package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.grpc.internal.ConnectorCommand;
import io.axoniq.axonserver.grpc.internal.ConnectorResponse;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.grpc.stub.StreamObserver;

/**
 * @author Marc Gathier
 */
public interface MessagingClusterServiceInterface {
    StreamObserver<ConnectorCommand> openStream(
            StreamObserver<ConnectorResponse> responseObserver);

    void join(NodeInfo request,
              StreamObserver<NodeInfo> responseObserver);

    void closeChannel();
}
