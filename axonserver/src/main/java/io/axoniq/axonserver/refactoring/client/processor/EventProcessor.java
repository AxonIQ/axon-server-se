/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.refactoring.client.processor;

import io.axoniq.axonserver.refactoring.client.processor.warning.ActiveWarnings;
import io.axoniq.axonserver.refactoring.client.processor.warning.Warning;
import io.axoniq.axonserver.refactoring.transport.rest.serializer.Media;
import io.axoniq.axonserver.refactoring.transport.rest.serializer.Printable;

import static java.util.Collections.emptyList;

/**
 * Contract describing a {@link Printable} format of event processor information.
 *
 * @author Sara Pellegrini
 * @since 4.0
 */
public interface EventProcessor extends Printable {

    /**
     * The processor name.
     *
     * @return the name of the processor
     */
    String name();

    /**
     * The mode/type of the processor. Could for example be "Subscribing", "Tracking" or "Pooled Streaming".
     *
     * @return the mode/type of the processor
     */
    String mode();

    /**
     * A flag indicating whether the described processor is a streaming instance yes or no. If it is, this should enable
     * streaming specific operations like split and merge.
     *
     * @return {@code true} if this {@link EventProcessor} is streaming, {@code false} otherwise
     */
    default Boolean isStreaming() {
        return false;
    }

    /**
     * The full name of this event processor. Defaults to the {@link #name()}
     *
     * @return the full name of this event processor
     */
    default String fullName() {
        return name();
    }

    /**
     * The {@link Warning}s currently active on this {@link EventProcessor}.
     *
     * @return the {@link Warning}s currently active on this {@link EventProcessor}
     */
    default Iterable<Warning> warnings() {
        return emptyList();
    }

    @Override
    default void printOn(Media media) {
        media.with("name", name())
             .with("mode", mode())
             .with("isStreaming", isStreaming())
             .with("fullName", fullName())
             .with("warnings", new ActiveWarnings(warnings()));
    }
}
