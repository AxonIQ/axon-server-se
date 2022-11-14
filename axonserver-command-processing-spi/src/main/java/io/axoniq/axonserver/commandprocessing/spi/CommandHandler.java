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