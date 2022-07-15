package io.axoniq.axonserver.commandprocessing.spi;

import java.io.Serializable;

/**
 * A result to a specific {@link Command} sent to its {@link CommandHandler}.
 *
 * @author Sara Pellegrini
 * @author Milan Savic
 */
public interface CommandResult extends Serializable {

    /**
     * The unique identifier of the command result.
     *
     * @return the unique identifier of the command result
     */
    String id();

    /**
     * The unique identifier of the command which caused this result to happen.
     *
     * @return the unique identifier of the command which caused this result to happen
     */
    String commandId(); // ???

    Payload payload();

    Metadata metadata();
}
