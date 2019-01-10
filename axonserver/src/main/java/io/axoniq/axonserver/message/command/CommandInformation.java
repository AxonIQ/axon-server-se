package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.grpc.SerializedCommandResponse;
import io.axoniq.axonserver.message.ClientIdentification;

import java.util.function.Consumer;

/**
 * Author: marc
 */
public class CommandInformation {
    private final String command;
    private final Consumer<SerializedCommandResponse> responseConsumer;
    private final long timestamp = System.currentTimeMillis();
    private final ClientIdentification clientId;
    private final String componentName;

    public CommandInformation(String command, Consumer<SerializedCommandResponse> responseConsumer, ClientIdentification clientId, String componentName) {
        this.command = command;
        this.responseConsumer = responseConsumer;
        this.clientId = clientId;
        this.componentName = componentName;
    }

    public String getCommand() {
        return command;
    }

    public Consumer<SerializedCommandResponse> getResponseConsumer() {
        return responseConsumer;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public ClientIdentification getClientId() {
        return clientId;
    }

    public String getComponentName() {
        return componentName;
    }

    public boolean checkClient(ClientIdentification client) {
        return clientId.equals(client);
    }
}
