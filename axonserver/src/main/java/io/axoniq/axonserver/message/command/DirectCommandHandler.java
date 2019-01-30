package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.grpc.Confirmation;
import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.grpc.SerializedCommandProviderInbound;
import io.axoniq.axonserver.message.ClientIdentification;
import io.grpc.stub.StreamObserver;

/**
 * @author Marc Gathier
 */
public class DirectCommandHandler extends CommandHandler<SerializedCommandProviderInbound> {
    public DirectCommandHandler(StreamObserver<SerializedCommandProviderInbound> responseObserver, ClientIdentification client, String componentName) {
        super(responseObserver, client, componentName);
    }

    @Override
    public void dispatch(SerializedCommand request) {
        observer.onNext(SerializedCommandProviderInbound.newBuilder().setCommand(request).build());
    }

    @Override
    public void confirm(String messageId) {
        observer.onNext(SerializedCommandProviderInbound.newBuilder().setConfirmation(Confirmation.newBuilder().setSuccess(true).setMessageId(messageId).build()).build());

    }

}
