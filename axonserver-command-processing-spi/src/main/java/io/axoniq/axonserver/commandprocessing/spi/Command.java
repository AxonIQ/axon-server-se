/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.commandprocessing.spi;

import java.io.Serializable;

/**
 * A command which should be routed to the specific {@link CommandHandler}.
 *
 * @author Sara Pellegrini
 * @author Milan Savic
 */
public interface Command extends Serializable {

    String ROUTING_KEY = "__routingkey";
    String PRIORITY = "__priority";

    String PRINCIPAL = "__principal";
    String CLIENT_ID = "__clientId";
    String COMPONENT_NAME = "__componentName";
    String TIMEOUT = "__timeout";

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
