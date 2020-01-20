/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

/**
 * @author Marc Gathier
 */
public interface EventStoreExistChecker {

    /**
     * Checks if a context exists on the Axon Server node. Does not create an EventStorageEngine.
     *
     * @param context
     * @return
     */
    boolean exists(String context);
}
