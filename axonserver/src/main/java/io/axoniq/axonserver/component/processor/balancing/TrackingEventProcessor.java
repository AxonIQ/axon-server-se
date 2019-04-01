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
 * Created by Sara Pellegrini on 07/08/2018.
 * sara.pellegrini@gmail.com
 */
public final class TrackingEventProcessor implements Serializable {

    private String name;

    private String component;

    private String context;

    @SuppressWarnings("unused")
    public TrackingEventProcessor() {
    }

    public TrackingEventProcessor(@Nonnull String name, @Nonnull String component, @Nonnull String context) {
        this.name = name;
        this.component = component;
        this.context = context;
    }

    public String name() {
        return name;
    }

    public String component() {
        return component;
    }

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
                Objects.equals(component, that.component) &&
                Objects.equals(context, that.context);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, component, context);
    }

    @Override
    public String toString() {
        return "TrackingEventProcessor{" +
                "name='" + name + '\'' +
                ", component='" + component + '\'' +
                ", context='" + context + '\'' +
                '}';
    }
}
