/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.grpc.event.Event;

import javax.annotation.Nonnull;

/**
 * Defines the function that is executed for an even to transform the event content during the event store
 * transformation process.
 *
 * @author Marc Gathier
 * @since 4.6.0
 */
@FunctionalInterface
public interface EventTransformationFunction {

    /**
     * Transforms a single event.
     *
     * @param event the stored event
     * @param token the token (global sequence number) of the stored event
     * @return result of the transformation
     */
    @Nonnull
    EventTransformationResult apply(Event event, Long token);
}
