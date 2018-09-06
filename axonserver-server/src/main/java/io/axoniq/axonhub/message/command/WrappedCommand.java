package io.axoniq.axonhub.message.command;

import io.axoniq.axonhub.Command;
import io.axoniq.axonhub.ProcessingInstructionHelper;

/**
 * Author: marc
 */
public class WrappedCommand {
    private final String context;
    private final Command command;
    private final long priority;

    public WrappedCommand(String context, Command command) {
        this.context = context;
        this.command = command;
        this.priority = ProcessingInstructionHelper.priority(command.getProcessingInstructionsList());
    }

    public Command command() {
        return command;
    }

    public String context() {
        return context;
    }

    public long priority() {
        return priority;
    }

}
