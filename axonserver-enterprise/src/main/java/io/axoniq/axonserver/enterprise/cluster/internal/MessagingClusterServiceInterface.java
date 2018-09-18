package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.grpc.Confirmation;
import io.axoniq.axonserver.internal.grpc.ConnectorCommand;
import io.axoniq.axonserver.internal.grpc.ConnectorResponse;
import io.axoniq.axonserver.internal.grpc.NodeContextInfo;
import io.axoniq.axonserver.internal.grpc.NodeInfo;
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
