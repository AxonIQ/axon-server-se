package io.axoniq.axonserver.commandprocessing.spi;

import java.io.Serializable;


public interface CommandHandler extends Serializable {

    String LOAD_FACTOR = "__loadfactor";
    String COMPONENT_NAME = "__component";
    String CLIENT_ID = "__clientId";
    String CLIENT_STREAM_ID = "__clientStreamId";

    String PROXIED = "__proxied";

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