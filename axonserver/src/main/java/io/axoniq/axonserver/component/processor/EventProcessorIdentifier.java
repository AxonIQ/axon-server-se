/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorId;
import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Identifies uniquely an event processor inside a specific context.
 *
 * @author Sara Pellegrini
 * @since 4.4
 */
public final class EventProcessorIdentifier implements EventProcessorId {

    private final String name;

    private final String tokenStoreIdentifier;

    private final String context;

    public EventProcessorIdentifier(ClientProcessor clientProcessor) {
        this(clientProcessor.eventProcessorInfo().getProcessorName(),
             clientProcessor.eventProcessorInfo().getTokenStoreIdentifier(),
             clientProcessor.context());
    }

    public EventProcessorIdentifier(TrackingEventProcessor eventProcessor) {
        this(eventProcessor.name(), eventProcessor.tokenStoreIdentifier(), eventProcessor.context());
    }

    public EventProcessorIdentifier(String name, String tokenStoreIdentifier, String context) {
        this.name = name;
        this.tokenStoreIdentifier = tokenStoreIdentifier;
        this.context = context;
    }

    @Nonnull
    public String name() {
        return name;
    }

    @Nonnull
    public String tokenStoreIdentifier() {
        return tokenStoreIdentifier;
    }

    public boolean equals(EventProcessorId id) {
        return Objects.equals(name, id.name()) &&
                Objects.equals(tokenStoreIdentifier, id.tokenStoreIdentifier()) &&
                Objects.equals(context, id.context());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EventProcessorIdentifier that = (EventProcessorIdentifier) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(context, that.context) &&
                Objects.equals(tokenStoreIdentifier, that.tokenStoreIdentifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, tokenStoreIdentifier);
    }

    @Override
    public String toString() {
        return "EventProcessorIdentifier{" +
                "name='" + name + '\'' +
                ", tokenStoreIdentifier='" + tokenStoreIdentifier + '\'' +
                '}';
    }

    @NotNull
    @Override
    public String context() {
        return context;
    }
}
