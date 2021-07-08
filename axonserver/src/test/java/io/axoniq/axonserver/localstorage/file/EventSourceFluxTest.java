/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Marc Gathier
 * @since 4.5.6
 */
class EventSourceFluxTest {

    @Test
    void get() {
        AtomicBoolean closed = new AtomicBoolean();
        EventSourceFactory eventSourceFactory = new EventSourceFactory() {
            final AtomicBoolean first = new AtomicBoolean(true);
            @Override
            public Optional<EventSource> create() {
                if (first.compareAndSet(true, false)) {
                    return Optional.empty();
                }

                return Optional.of(new EventSource() {

                    @Override
                    public SerializedEvent readEvent(int position) {
                        return new SerializedEvent(Event.getDefaultInstance());
                    }

                    @Override
                    public TransactionIterator createTransactionIterator(long segment, long token, boolean validating) {
                        return null;
                    }

                    @Override
                    public EventIterator createEventIterator(long segment, long startToken) {
                        return null;
                    }

                    @Override
                    public void close() {
                        closed.set(true);
                    }
                });
            }
        };
        EventSourceFlux eventSourceFlux = new EventSourceFlux(new StandardIndexEntries(0, new Integer[] {1,2,3}),
                                                              eventSourceFactory,
                                                              100);
        List<SerializedEvent> events = eventSourceFlux.get().retry(1).collect(Collectors.toList()).block();
        assertEquals(3, events.size());
        assertTrue(closed.get());
    }
}