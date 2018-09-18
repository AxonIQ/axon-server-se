package io.axoniq.axonserver.message.command;

import io.axoniq.axonhub.CommandResponse;

import java.util.function.Consumer;

/**
 * Author: marc
 */
public class CommandInformation {
    private final String command;
    private final Consumer<CommandResponse> responseConsumer;
    private final long timestamp = System.currentTimeMillis();
    private final String clientId;
    private final String componentName;

    public CommandInformation(String command, Consumer<CommandResponse> responseConsumer, String clientId, String componentName) {
        this.command = command;
        this.responseConsumer = responseConsumer;
        this.clientId = clientId;
        this.componentName = componentName;
    }

    public String getCommand() {
        return command;
    }

    public Consumer<CommandResponse> getResponseConsumer() {
        return responseConsumer;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getClientId() {
        return clientId;
    }

    public String getComponentName() {
        return componentName;
    }
}
