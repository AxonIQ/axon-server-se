/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.processor.balancing;

import java.io.Serializable;
import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * Represents the tracking event processor semantic key.
 * @author Sara Pellegrini
 * @since 4.0
 */
public final class TrackingEventProcessor implements Serializable {

    private String name;

    private String context;

    @SuppressWarnings("unused")
    public TrackingEventProcessor() {
    }

    /**
     * Creates an instance with the specified name and context
     *
     * @param name    the name of the tracking event processor
     * @param context the context of the tracking event processor
     */
    public TrackingEventProcessor(@Nonnull String name, @Nonnull String context) {
        this.name = name;
        this.context = context;
    }

    /**
     * Returns the name of the tracking event processor
     *
     * @return the name of the tracking event processor
     */
    public String name() {
        return name;
    }

    /**
     * Returns the context of the tracking event processor
     * @return the context of the tracking event processor
     */
    public String context() {
        return context;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TrackingEventProcessor that = (TrackingEventProcessor) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(context, that.context);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, context);
    }

    @Override
    public String toString() {
        return "TrackingEventProcessor{" +
                "name='" + name + '\'' +
                ", context='" + context + '\'' +
                '}';
    }
}
