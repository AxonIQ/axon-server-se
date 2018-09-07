package io.axoniq.axonserver.grpc.internal;

import io.axoniq.axonhub.Confirmation;
import io.axoniq.axonhub.internal.grpc.ConnectorCommand;
import io.axoniq.axonhub.internal.grpc.ConnectorResponse;
import io.axoniq.axonhub.internal.grpc.NodeContextInfo;
import io.axoniq.axonhub.internal.grpc.NodeInfo;
import io.grpc.stub.StreamObserver;

/**
 * Author: marc
 */
public interface MessagingClusterServiceInterface {
    StreamObserver<ConnectorCommand> openStream(
            StreamObserver<ConnectorResponse> responseObserver);

    void join(NodeInfo request,
              StreamObserver<NodeInfo> responseObserver);

    void requestLeader(NodeContextInfo nodeContextInfo, StreamObserver<Confirmation> confirmationStreamObserver);
}
