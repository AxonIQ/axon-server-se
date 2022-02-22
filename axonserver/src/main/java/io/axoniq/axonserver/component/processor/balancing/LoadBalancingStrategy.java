/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.processor.balancing;

import io.axoniq.axonserver.serializer.Media;
import io.axoniq.axonserver.serializer.Printable;

/**
 * Interface describing a strategy usable for balancing the load on Streaming Event Processor instances across
 * applications.
 */
public interface LoadBalancingStrategy extends Printable {

    String getLabel();

    String getName();

    LoadBalancingOperation balance(TrackingEventProcessor processor);

    @Override
    default void printOn(Media media) {
        media.with("name", getName())
             .with("label", getLabel());
    }
}
