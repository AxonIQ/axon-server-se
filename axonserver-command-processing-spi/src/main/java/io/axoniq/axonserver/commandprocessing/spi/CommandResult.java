package io.axoniq.axonserver.commandprocessing.spi;

import java.io.Serializable;

/**
 * A result to a specific {@link Command} sent to its {@link CommandHandler}.
 *
 * @author Sara Pellegrini
 * @author Milan Savic
 */
public interface CommandResult extends Serializable {

    String CLIENT_ID = "clientId";

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
    String commandId();

    ResultPayload payload();

    Metadata metadata();
}
