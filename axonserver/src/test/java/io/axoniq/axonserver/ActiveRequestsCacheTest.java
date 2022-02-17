/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver;

import io.axoniq.axonserver.ActiveRequestsCache.Completable;
import org.junit.*;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nonnull;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link ActiveRequestsCache}.
 *
 * @author Marc Gathier
 * @author Sara Pellegrini
 */
public class ActiveRequestsCacheTest {

    @Test
    public void cancel() {
        Set<String> canceledId = new HashSet<>();
        LimitedBuffer<Instruction> buffer = new LimitedBuffer<>("testBuffer", "errorMessage", 10);
        ActiveRequestsCache<Instruction> cache = new ActiveRequestsCache<>(buffer);
        cache.put("request1", new Instruction(false));
        cache.put("request2", new Instruction(true));
        cache.put("request3", new Instruction(true));

        cache.cancel(new ActiveRequestsCache.CancelStrategy<Instruction>() {
            @Override
            public void cancel(String requestId, Instruction request) {
                canceledId.add(requestId);
            }

            @Override
            public boolean requestToBeCanceled(String requestId, Instruction request) {
                return request.cancelable;
            }
        });

        assertEquals(new HashSet<>(asList("request2", "request3")), canceledId);
    }

    @Test
    public void putTest() {
        LimitedBuffer<Instruction> buffer = new LimitedBuffer<>("testBuffer", "errorMessage", 10);
        ActiveRequestsCache<Instruction> cache = new ActiveRequestsCache<>(buffer);
        Instruction instruction = new Instruction(false);
        cache.put("request1", instruction);
        assertEquals(instruction, cache.get("request1"));
        instruction.complete();
        assertNull(cache.get("request1"));
    }

    private static class Instruction implements Completable {

        private final boolean cancelable;
        private final AtomicReference<Runnable> callback = new AtomicReference<>(() -> {});

        private Instruction(boolean cancelable) {
            this.cancelable = cancelable;
        }

        public boolean isCancelable() {
            return cancelable;
        }

        @Override
        public void onCompletion(@Nonnull Runnable listener) {
            callback.set(listener);
        }

        public void complete() {
            callback.get().run();
        }
    }
}