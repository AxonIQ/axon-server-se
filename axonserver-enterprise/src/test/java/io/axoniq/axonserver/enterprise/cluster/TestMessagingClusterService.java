package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.enterprise.cluster.internal.MessagingClusterServiceInterface;
import io.axoniq.axonserver.grpc.internal.ConnectResponse;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand;
import io.axoniq.axonserver.grpc.internal.ConnectorResponse;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Marc Gathier
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
                        scheduledExecutorService.schedule(() -> responseObserver.onNext(ConnectorResponse.newBuilder().setConnectResponse(ConnectResponse.newBuilder()).build()), 50, TimeUnit.MILLISECONDS);
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
    public void closeChannel() {

    }

}
