/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver;

import io.axoniq.axonserver.util.ConstraintCache;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * @author Sara Pellegrini
 * @since 4.6.0
 */
public class ActiveRequestsCache<R extends ActiveRequestsCache.Completable>
        implements ConstraintCache<String, R> /*Cancellable*/ {

    private final ConstraintCache<String, R> buffer;


    public ActiveRequestsCache(ConstraintCache<String, R> buffer) {
        this.buffer = buffer;
    }

    @Override
    public int size() {
        return buffer.size();
    }

    @Override
    public R remove(String key) {
        return buffer.remove(key);
    }

    @Override
    public R get(String key) {
        return buffer.get(key);
    }

    @Override
    public R put(String key, R value) {
        value.onCompletion(() -> buffer.remove(key));
        return buffer.put(key, value);
    }

    @Override
    public Collection<Entry<String, R>> entrySet() {
        return buffer.entrySet();
    }


    public void cancel(CancelStrategy<R> cancelStrategy) {
        Set<Entry<String, R>> toCancel =
                buffer.entrySet()
                      .stream()
                      .filter(e -> cancelStrategy.requestToBeCanceled(e.getKey(), e.getValue()))
                      .collect(Collectors.toSet());


        if (!toCancel.isEmpty()) {
            toCancel.forEach(e -> {
                buffer.remove(e.getKey());
                cancelStrategy.cancel(e.getKey(), e.getValue());
            });
        }
    }

    public interface CancelStrategy<R> {

        void cancel(String requestId, R request);

        boolean requestToBeCanceled(String requestId, R request);
    }

    @FunctionalInterface
    public interface Completable {

        void onCompletion(@Nonnull Runnable listener);
    }
}
