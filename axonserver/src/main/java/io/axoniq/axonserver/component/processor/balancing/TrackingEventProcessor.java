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
import javax.persistence.Embeddable;

/**
 * Represents the tracking event processor semantic key.
 *
 * @author Sara Pellegrini
 * @since 4.0
 */
@Embeddable
public final class TrackingEventProcessor implements Serializable {

    private String name;

    private String context;

    private String tokenStoreIdentifier;

    @SuppressWarnings("unused")
    public TrackingEventProcessor() {
    }

    /**
     * Creates an instance with the specified name and context
     *
     * @param name    the name of the tracking event processor
     * @param context the principal context of the tracking event processor
     */
    public TrackingEventProcessor(@Nonnull String name,
                                  @Nonnull String context) {
        this(name, context, "");
    }

    /**
     * Creates an instance with the specified name, context and token store identifier
     *
     * @param name                 the name of the tracking event processor
     * @param context              the principal context of the tracking event processor
     * @param tokenStoreIdentifier the token store identifier of the tracking event processor
     */
    public TrackingEventProcessor(@Nonnull String name,
                                  @Nonnull String context,
                                  @Nonnull String tokenStoreIdentifier) {
        this.name = name;
        this.context = context;
        this.tokenStoreIdentifier = tokenStoreIdentifier;
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
     * Returns the principal context of the tracking event processor
     *
     * @return the principal context of the tracking event processor
     */
    public String context() {
        return context;
    }

    /**
     * Returns the token store identifier of the tracking event processor
     *
     * @return the token store identifier of the tracking event processor
     */
    public String tokenStoreIdentifier() {
        return tokenStoreIdentifier;
    }

    /**
     * Returns the full name of processor, that is processorName@tokenStoreIdentifier
     *
     * @return the full name of processor
     */
    public String fullName() {
        return name + "@" + tokenStoreIdentifier;
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
                Objects.equals(context, that.context) &&
                Objects.equals(tokenStoreIdentifier, that.tokenStoreIdentifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, context, tokenStoreIdentifier);
    }

    @Override
    public String toString() {
        return "TrackingEventProcessor{" +
                "name='" + name + '\'' +
                ", context='" + context + '\'' +
                ", tokenStoreIdentifier='" + tokenStoreIdentifier + '\'' +
                '}';
    }
}
