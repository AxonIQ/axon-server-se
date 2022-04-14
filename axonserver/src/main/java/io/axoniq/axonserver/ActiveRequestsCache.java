/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
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
 * A cache for generic active requests.
 *
 * @author Marc Gathier
 * @author Sara Pellegrini
 * @since 4.6.0
 */
public class ActiveRequestsCache<R extends ActiveRequestsCache.Completable>
        implements ConstraintCache<String, R> {

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


    /**
     * Cancel the active requests inside the cache according to the specified strategy.
     *
     * @param cancelStrategy the strategy to decide which requests need to be canceled.
     */
    protected void cancel(CancelStrategy<R> cancelStrategy) {
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

    /**
     * Strategy to define which requests should be canceled and how they are canceled.
     *
     * @param <R> the type of the request
     */
    public interface CancelStrategy<R> {

        /**
         * Performs the cancel operation of the request
         *
         * @param requestId the identifier of the request
         * @param request   the request to be canceled
         */
        void cancel(String requestId, R request);

        /**
         * Returns {@code  true} if the request must be canceled, {@code  false} otherwise
         *
         * @param requestId the identifier of the request
         * @param request   the request to be checked
         * @return {@code  true} if the request must be canceled, {@code  false} otherwise
         */
        boolean requestToBeCanceled(String requestId, R request);
    }


    /**
     * Interface to provide observability on item completion.
     */
    @FunctionalInterface
    public interface Completable {

        /**
         * Register a callback to be invoked on the completion of the item
         *
         * @param listener the {@link Runnable} to be invoked when the item is completed
         */
        void onCompletion(@Nonnull Runnable listener);
    }
}
