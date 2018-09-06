package io.axoniq.axonhub.message.command;

import io.axoniq.axonhub.Command;
import io.axoniq.axonhub.Confirmation;
import io.axoniq.axonhub.grpc.CommandProviderInbound;
import io.grpc.stub.StreamObserver;

/**
 * Author: marc
 */
public class DirectCommandHandler extends CommandHandler<CommandProviderInbound> {
    public DirectCommandHandler(StreamObserver<CommandProviderInbound> responseObserver, String client, String componentName) {
        super(responseObserver, client, componentName);
    }

    @Override
    public void dispatch(Command request) {
        observer.onNext(CommandProviderInbound.newBuilder().setCommand(request).build());
    }

    @Override
    public void confirm(String messageId) {
        observer.onNext(CommandProviderInbound.newBuilder().setConfirmation(Confirmation.newBuilder().setSuccess(true).setMessageId(messageId)).build());

    }

}
