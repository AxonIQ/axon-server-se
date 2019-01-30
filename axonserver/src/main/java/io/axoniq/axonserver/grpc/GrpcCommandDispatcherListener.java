package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.message.FlowControlQueues;
import io.axoniq.axonserver.message.command.WrappedCommand;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads messages for a specific client from a queue and sends them to the client using gRPC.
 * Only reads messages when there are permits left.
 * Author: marc
 */
public class GrpcCommandDispatcherListener extends GrpcFlowControlledDispatcherListener<SerializedCommandProviderInbound, WrappedCommand> {
    private static final Logger logger = LoggerFactory.getLogger(GrpcCommandDispatcherListener.class);

    public GrpcCommandDispatcherListener(FlowControlQueues<WrappedCommand> commandQueues, String queueName, StreamObserver<SerializedCommandProviderInbound> commandProviderInboundStreamObserver, int threads) {
        super(commandQueues, queueName, commandProviderInboundStreamObserver, threads);
    }

    @Override
    protected boolean send(WrappedCommand message) {
        try {
            inboundStream.onNext(SerializedCommandProviderInbound.newBuilder().setCommand(message.command()).build());
        } catch( Exception ex) {
            logger.warn("Could not send command to {}", queueName, ex);
        }
        return true;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

}
