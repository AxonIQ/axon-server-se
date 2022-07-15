package io.axoniq.axonserver.commandprocessing.spi;

import java.io.Serializable;


public interface CommandHandler extends Serializable {

    /**
     * The unique identifier of the command handler.
     *
     * @return the unique identifier of the command handler
     */
    String id();

    /**
     * The description of the command handler. Used mostly for
     * @return
     */
    String description();

    String commandName();

    String context();

    Metadata metadata();
}