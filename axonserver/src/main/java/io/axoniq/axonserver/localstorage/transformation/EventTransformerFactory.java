/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.transformation;

/**
 * Defines the interface for an EventTransformer factory.
 *
 * @author Marc Gathier
 */
public interface EventTransformerFactory {

    /**
     * Get an event transformer.
     *
     * @param flags the flags for existing data
     * @return the transformer
     */
    EventTransformer get(int flags);
}
