/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.instance;

import io.axoniq.axonserver.component.ComponentItem;
import io.axoniq.axonserver.serializer.Media;
import io.axoniq.axonserver.serializer.Printable;

/**
 * Represents an instance of a client connected to AxonServer.
 */
public interface Client extends Printable, ComponentItem {

    /**
     * Returns the id of the client
     *
     * @return the id of the client
     */
    String id();

    /**
     * Returns the platform stream id of the client
     *
     * @return the platform stream id of the client
     */
    String streamId();

    /**
     * Returns the principal context of the client
     *
     * @return the principal context of the client
     */
    String context();

    @Override
    default boolean belongsToContext(String context) {
        return context.equals(context());
    }

    @Override
    default void printOn(Media media) {
        media.with("name", id());
    }
}
