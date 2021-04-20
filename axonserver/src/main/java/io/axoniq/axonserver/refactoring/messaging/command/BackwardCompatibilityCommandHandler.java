package io.axoniq.axonserver.refactoring.messaging.command;

import io.axoniq.axonserver.ClientStreamIdentification;
import io.axoniq.axonserver.refactoring.messaging.command.api.Command;
import io.grpc.stub.StreamObserver;

/**
 * @author Milan Savic
 */
public class BackwardCompatibilityCommandHandler extends CommandHandler<Command> {

    public BackwardCompatibilityCommandHandler(
            StreamObserver<Command> responseObserver,
            ClientStreamIdentification clientStreamIdentification, String clientId,
            String componentName) {
        super(responseObserver, clientStreamIdentification, clientId, componentName);
    }

    public BackwardCompatibilityCommandHandler(io.axoniq.axonserver.refactoring.messaging.command.api.CommandHandler other) {
        // TODO: 4/20/2021
        super(null, null, null, null);
    }

    @Override
    public void dispatch(SerializedCommand request) {

    }

    @Override
    public void confirm(String messageId) {

    }
}
