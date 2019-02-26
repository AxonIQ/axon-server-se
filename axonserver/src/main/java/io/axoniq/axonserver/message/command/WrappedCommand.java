package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.message.ClientIdentification;

/**
 * @author Marc Gathier
 */
public class WrappedCommand {
    private final ClientIdentification client;
    private final SerializedCommand command;
    private final long priority;

    public WrappedCommand(ClientIdentification client, SerializedCommand command) {
        this.client = client;
        this.command = command;
        this.priority = command.getPriority();
    }

    public SerializedCommand command() {
        return command;
    }

    public ClientIdentification client() {
        return client;
    }

    public long priority() {
        return priority;
    }

}
