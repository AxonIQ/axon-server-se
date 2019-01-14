package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.ProcessingInstructionHelper;
import io.axoniq.axonserver.grpc.GrpcFlowControlledDispatcherListener;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.internal.ConnectorResponse;
import io.axoniq.axonserver.message.FlowControlQueues;
import io.axoniq.axonserver.message.command.WrappedCommand;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads messages for a specific messagingServerName from a queue and sends them to the messagingServerName using gRPC.
 * Only reads messages when there are permits left.
 * @author Marc Gathier
 */
public class GrpcInternalCommandDispatcherListener extends GrpcFlowControlledDispatcherListener<ConnectorResponse, WrappedCommand> {
    private static final Logger logger = LoggerFactory.getLogger(GrpcInternalCommandDispatcherListener.class);
    public GrpcInternalCommandDispatcherListener(FlowControlQueues<WrappedCommand> commandQueue, String messagingServerName, StreamObserver<ConnectorResponse> commandProviderInboundStreamObserver, int threads) {
        super(commandQueue, messagingServerName, commandProviderInboundStreamObserver, threads);
    }

    @Override
    protected boolean send(WrappedCommand message) {
        inboundStream.onNext(ConnectorResponse.newBuilder().setCommand(Command.newBuilder(message.command()).addProcessingInstructions(ProcessingInstructionHelper.context(message.context()))).build());
        return true;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

}
