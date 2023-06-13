/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.util;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class DefaultIdLock implements IdLock {

    private final AtomicReference<IdAndCounter> idReference = new AtomicReference<>();

    @Override
    public Ticket request(String id) {
        synchronized (idReference) {
            IdAndCounter locked = idReference.updateAndGet(
                    old -> Objects.requireNonNullElseGet(old, () -> new IdAndCounter(id)));

            if (!locked.id().equals(id)) {
                return new InvalidTicket();
            }
            return locked.emitTicket();
        }
    }

    private class IdAndCounter {

        private final String id;
        private final AtomicInteger counter = new AtomicInteger();


        public IdAndCounter(String id) {

            this.id = id;
        }

        public String id() {
            return id;
        }

        public Ticket emitTicket() {
            counter.incrementAndGet();
            return new Ticket() {
                private final AtomicBoolean acquired = new AtomicBoolean(true);

                @Override
                public boolean isAcquired() {
                    return acquired.get();
                }

                @Override
                public void release() {
                    if (acquired.compareAndSet(true, false)) {
                        synchronized (idReference) {
                            int remaining = counter.decrementAndGet();
                            if (remaining == 0) {
                                idReference.set(null);
                            }
                        }
                    }
                }
            };
        }
    }

    private class InvalidTicket implements Ticket {

        @Override
        public boolean isAcquired() {
            return false;
        }

        @Override
        public void release() {
            //do nothing
        }
    }
}
