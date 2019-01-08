package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.grpc.SerializedCommand;

/**
 * Author: marc
 */
public class WrappedCommand {
    private final String context;
    private final String client;
    private final SerializedCommand command;
    private final long priority;

    public WrappedCommand(String context, String client, SerializedCommand command) {
        this.context = context;
        this.client = client;
        this.command = command;
        this.priority = command.getPriority();
    }

    public SerializedCommand command() {
        return command;
    }

    public String context() {
        return context;
    }

    public String client() {
        return client;
    }

    public long priority() {
        return priority;
    }

}
