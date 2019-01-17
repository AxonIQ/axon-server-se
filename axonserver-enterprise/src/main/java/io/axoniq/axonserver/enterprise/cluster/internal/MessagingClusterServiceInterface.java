package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.grpc.Confirmation;
import io.axoniq.axonserver.grpc.internal.ConnectResponse;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand;
import io.axoniq.axonserver.grpc.internal.ConnectorResponse;
import io.axoniq.axonserver.grpc.internal.NodeContextInfo;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.grpc.stub.StreamObserver;

/**
 * Author: marc
 */
public interface MessagingClusterServiceInterface {
    StreamObserver<ConnectorCommand> openStream(
            StreamObserver<ConnectorResponse> responseObserver);

    void join(NodeInfo request,
              StreamObserver<ConnectResponse> responseObserver);

    void requestLeader(NodeContextInfo nodeContextInfo, StreamObserver<Confirmation> confirmationStreamObserver);

    void closeChannel();
}
