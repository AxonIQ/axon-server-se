/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.processor.listener;

import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo.SegmentStatus;

import java.util.Iterator;
import javax.annotation.Nonnull;

/**
 * It is the representation of the status of an event processor in a single client instance.
 *
 * @author Sara Pellegrini
 */
public interface ClientProcessor extends Iterable<SegmentStatus> {

    /**
     * Returns the client identifiers
     *
     * @return the client identifiers
     */
    String clientId();

    /**
     * Returns the information about the event processor.
     *
     * @return the information about the event processor.
     */
    EventProcessorInfo eventProcessorInfo();

    /**
     * Returns {@code true} if the client is an instance of the specified component, {@code false} otherwise.
     *
     * @param component the component name that to check
     * @return {@code true} if the client is an instance of the specified component, {@code false} otherwise.
     */
    Boolean belongsToComponent(String component);

    /**
     * Returns {@code true} if the event processor is running, {@code false} otherwise.
     *
     * @return {@code true} if the event processor is running, {@code false} otherwise.
     */
    default boolean running() {
        return eventProcessorInfo().getRunning();
    }

    /**
     * Returns an iterator over the event processor's {@link SegmentStatus}es claimed by the client.
     *
     * @return an iterator over the event processor's {@link SegmentStatus}es claimed by the client.
     */
    @Nonnull
    @Override
    default Iterator<SegmentStatus> iterator() {
        return eventProcessorInfo().getSegmentStatusList().iterator();
    }
}
