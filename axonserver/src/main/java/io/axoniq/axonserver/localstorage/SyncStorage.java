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
import io.axoniq.axonserver.exception.EventStoreValidationException;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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
        if( eventList.isEmpty()) return;
        if (token < eventStore.nextToken()) {
            eventStore.validateTransaction(token, eventList);
            return;
        }

        if (token != eventStore.nextToken()) {
            logger.error("{}: {} expecting token {} received {}",
                        eventStore.getType().getContext(), eventStore.getType().getEventType(),
                        eventStore.nextToken(),
                        token);
            throw new EventStoreValidationException("Received invalid token");
        }
        try {
            List<Event> events = new ArrayList<>(eventList.size());
            eventList.forEach(e -> events.add(e.asEvent()));
            eventStore.store(events).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MessagingPlatformException(ErrorCode.DATAFILE_WRITE_ERROR, e.getMessage(), e);
        } catch (ExecutionException e) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_WRITE_ERROR, e.getMessage(), e.getCause());
        }
    }
}
