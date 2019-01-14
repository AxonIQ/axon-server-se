package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.ProcessingInstructionHelper;
import io.axoniq.axonserver.grpc.Confirmation;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.internal.ConnectorResponse;
import io.axoniq.axonserver.message.command.CommandHandler;
import io.grpc.stub.StreamObserver;

/**
 * @author Marc Gathier
 */
public class ProxyCommandHandler extends CommandHandler<ConnectorResponse> {
    private static final Confirmation confirmationBase = Confirmation.newBuilder().setSuccess(true).build();
    private final String messagingServerName;

    public ProxyCommandHandler(StreamObserver<ConnectorResponse> streamObserver, String client, String componentName, String messagingServerName) {
        super(streamObserver, client, componentName);
        this.messagingServerName = messagingServerName;
    }

    public String getMessagingServerName() {
        return messagingServerName;
    }

    @Override
    public void dispatch(Command request) {
        observer.onNext(ConnectorResponse.newBuilder().setCommand(
                Command.newBuilder(request)
                        .addProcessingInstructions(ProcessingInstructionHelper.targetClient(client))
        ).build());
    }

    @Override
    public void confirm(String messageId) {
        observer.onNext(ConnectorResponse.newBuilder().setConfirmation(Confirmation.newBuilder(confirmationBase).setMessageId(messageId)).build());
    }

    @Override
    public String queueName() {
        return messagingServerName;
    }

}
