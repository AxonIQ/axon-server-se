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
 * Defines the interface to configure an event store for a specific context.
 * @author Marc Gathier
 */
public interface EventStoreFactory {

    /**
     * Creates the storage engine for events for a context.
     * @param context the context
     * @return the storage engine
     */
    EventStorageEngine createEventStorageEngine(String context);

    /**
     * Creates the storage engine for snapshots for a context.
     * @param context the context
     * @return the storage engine
     */
    EventStorageEngine createSnapshotStorageEngine(String context);

}
