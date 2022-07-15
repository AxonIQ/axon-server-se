package io.axoniq.axonserver.commandprocessing.spi;

import java.io.Serializable;

/**
 * A command which should be routed to the specific {@link CommandHandler}.
 *
 * @author Sara Pellegrini
 * @author Milan Savic
 */
public interface Command extends Serializable {

    /**
     * The unique identifier of this command.
     *
     * @return the unique identifier of this command
     */
    String id();

    /**
     * The name of the command.
     *
     * @return the name of the command
     */
    String commandName();

    /**
     * The context this command belongs to.
     *
     * @return the context this command belongs to
     */
    String context();

    Payload payload();

    Metadata metadata();
}
