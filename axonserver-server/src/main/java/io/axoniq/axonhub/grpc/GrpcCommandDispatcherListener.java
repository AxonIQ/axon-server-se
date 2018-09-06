package io.axoniq.axonhub.grpc;

import io.axoniq.axonhub.message.FlowControlQueues;
import io.axoniq.axonhub.message.command.WrappedCommand;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads messages for a specific client from a queue and sends them to the client using gRPC.
 * Only reads messages when there are permits left.
 * Author: marc
 */
public class GrpcCommandDispatcherListener extends GrpcFlowControlledDispatcherListener<CommandProviderInbound, WrappedCommand> {
    private static final Logger logger = LoggerFactory.getLogger(GrpcCommandDispatcherListener.class);

    public GrpcCommandDispatcherListener(FlowControlQueues<WrappedCommand> commandQueues, String client, StreamObserver<CommandProviderInbound> commandProviderInboundStreamObserver) {
        super(commandQueues, client, commandProviderInboundStreamObserver);
    }

    @Override
    protected boolean send(WrappedCommand message) {
        inboundStream.onNext(CommandProviderInbound.newBuilder().setCommand(message.command()).build());
        return true;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

}
