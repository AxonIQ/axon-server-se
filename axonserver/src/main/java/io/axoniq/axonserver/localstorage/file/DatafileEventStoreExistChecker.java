/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.localstorage.EventStoreExistChecker;

import java.io.File;

/**
 * Component to check if an event store exists for a specific context, without actually opening it.
 *
 * @author Marc Gathier
 * @since 4.3
 */
public class DatafileEventStoreExistChecker implements EventStoreExistChecker {

    private final EmbeddedDBProperties embeddedDBProperties;

    public DatafileEventStoreExistChecker(EmbeddedDBProperties embeddedDBProperties) {
        this.embeddedDBProperties = embeddedDBProperties;
    }

    @Override
    public boolean exists(String context) {
        String folder = embeddedDBProperties.getEvent().getStorage(context);
        File events = new File(folder);
        if (!events.isDirectory()) {
            return false;
        }

        String[] files = events.list((dir, name) -> name.endsWith(embeddedDBProperties.getEvent().getEventsSuffix()));
        return files != null && files.length > 0;
    }
}
