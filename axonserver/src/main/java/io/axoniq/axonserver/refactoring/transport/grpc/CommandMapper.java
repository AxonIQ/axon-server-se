package io.axoniq.axonserver.refactoring.transport.grpc;

import io.axoniq.axonserver.refactoring.messaging.command.SerializedCommand;
import io.axoniq.axonserver.refactoring.messaging.command.api.Command;
import io.axoniq.axonserver.refactoring.transport.Mapper;
import org.springframework.stereotype.Component;

/**
 * @author Milan Savic
 */
@Component("commandMapper")
public class CommandMapper implements Mapper<Command, SerializedCommand> {

    @Override
    public SerializedCommand map(Command origin) {
        return null;
    }

    @Override
    public Command unmap(SerializedCommand origin) {
        return null;
    }
}
