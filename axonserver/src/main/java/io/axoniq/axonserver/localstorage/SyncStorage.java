/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author Marc Gathier
 */
public class SyncStorage {
    private static final Logger logger = LoggerFactory.getLogger(SyncStorage.class);


    private final EventStorageEngine eventStore;

    public SyncStorage(EventStorageEngine eventStore) {
        this.eventStore = eventStore;
    }

    public void sync(long token, List<SerializedEvent> eventList) {
        if (token < eventStore.nextToken()) {
            logger.debug("{}: {} with token {} already stored",
                        eventStore.getType().getContext(),
                        eventStore.getType().getEventType(),
                        token);
            return;
        }

        if (token != eventStore.nextToken()) {
            logger.error("{}: {} expecting token {} received {}",
                        eventStore.getType().getContext(), eventStore.getType().getEventType(),
                        eventStore.nextToken(),
                        token);
            throw new MessagingPlatformException(ErrorCode.DATAFILE_WRITE_ERROR, "Received invalid token");
        }
        try {
            eventStore.store(eventList).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MessagingPlatformException(ErrorCode.DATAFILE_WRITE_ERROR, e.getMessage(), e);
        } catch (ExecutionException e) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_WRITE_ERROR, e.getMessage(), e.getCause());
        }
    }
}
