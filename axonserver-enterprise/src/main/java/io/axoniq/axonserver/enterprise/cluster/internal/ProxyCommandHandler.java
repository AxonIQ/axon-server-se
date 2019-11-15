package io.axoniq.axonserver.enterprise.cluster.internal;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.grpc.Confirmation;
import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.grpc.internal.ConnectorResponse;
import io.axoniq.axonserver.grpc.internal.ForwardedCommand;
import io.axoniq.axonserver.message.ClientIdentification;
import io.axoniq.axonserver.message.command.CommandHandler;
import io.grpc.stub.StreamObserver;

/**
 * @author Marc Gathier
 */
public class ProxyCommandHandler extends CommandHandler<ConnectorResponse> {
    private static final Confirmation confirmationBase = Confirmation.newBuilder().setSuccess(true).build();
    private final String messagingServerName;

    public ProxyCommandHandler(StreamObserver<ConnectorResponse> streamObserver, ClientIdentification client, String componentName, String messagingServerName) {
        super(streamObserver, client, componentName);
        this.messagingServerName = messagingServerName;
    }

    @Override
    public String getMessagingServerName() {
        return messagingServerName;
    }

    @Override
    public void dispatch(SerializedCommand request) {
        observer.onNext(ConnectorResponse.newBuilder().setCommand(ForwardedCommand.newBuilder()
                                                                                  .setClient(client.getClient())
                                                                                  .setContext(client.getContext())
                                                                                  .setMessageId(request.getMessageIdentifier())
                                                                                  .setCommand(ByteString.copyFrom(request.toByteArray()))
                                                                                  .build()
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
