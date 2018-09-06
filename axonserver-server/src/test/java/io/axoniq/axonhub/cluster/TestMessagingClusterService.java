package io.axoniq.axonhub.cluster;

import io.axoniq.axonhub.Confirmation;
import io.axoniq.axonhub.grpc.internal.MessagingClusterServiceInterface;
import io.axoniq.axonhub.internal.grpc.ConnectResponse;
import io.axoniq.axonhub.internal.grpc.ConnectorCommand;
import io.axoniq.axonhub.internal.grpc.ConnectorResponse;
import io.axoniq.axonhub.internal.grpc.NodeContextInfo;
import io.axoniq.axonhub.internal.grpc.NodeInfo;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Author: marc
 */
public class TestMessagingClusterService implements MessagingClusterServiceInterface {
    ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    @Override
    public StreamObserver<ConnectorCommand> openStream(StreamObserver<ConnectorResponse> responseObserver) {
        StreamObserver<ConnectorCommand> inputStream = new StreamObserver<ConnectorCommand>() {

            @Override
            public void onNext(ConnectorCommand connectorCommand) {
                switch (connectorCommand.getRequestCase()) {
                    case CONNECT:
                        scheduledExecutorService.schedule(() -> responseObserver.onNext(ConnectorResponse.newBuilder().setConnectResponse(ConnectResponse.newBuilder().setApplicationModelVersion(1)).build()), 50, TimeUnit.MILLISECONDS);
                        break;
                    case SUBSCRIBE_COMMAND:
                        break;
                    case UNSUBSCRIBE_COMMAND:
                        break;
                    case COMMAND_RESPONSE:
                        break;
                    case SUBSCRIBE_QUERY:
                        break;
                    case UNSUBSCRIBE_QUERY:
                        break;
                    case QUERY_RESPONSE:
                        break;
                    case FLOW_CONTROL:
                        break;
                    case DELETE_NODE:
                        break;
                    case REQUEST_APPLICATIONS:
                        break;
                    case REQUEST_NOT_SET:
                        break;
                }

            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {

            }
        };
        return inputStream;
    }

    @Override
    public void join(NodeInfo request, StreamObserver<NodeInfo> responseObserver) {

    }

    @Override
    public void requestLeader(NodeContextInfo nodeContextInfo,
                              StreamObserver<Confirmation> confirmationStreamObserver) {

    }
}
