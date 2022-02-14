/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver;

import io.axoniq.axonserver.message.command.InsufficientBufferCapacityException;
import io.axoniq.axonserver.util.ConstraintCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nonnull;

/**
 * @author Sara Pellegrini
 * @since 4.6.0
 */
public class LimitedBuffer<T> implements ConstraintCache<String, T> {

    private final Logger logger = LoggerFactory.getLogger(LimitedBuffer.class);

    private final long capacity;
    private final String bufferName;
    private final String fullBufferMessage;
    private final ConcurrentMap<String, T> buffer = new ConcurrentHashMap<>();


    public LimitedBuffer(String bufferName, String fullBufferMessage, long capacity) {
        super();
        this.bufferName = bufferName;
        this.fullBufferMessage = fullBufferMessage;
        this.capacity = capacity;
    }

    @Override
    public int size() {
        return buffer.size();
    }

    @Override
    public T remove(@Nonnull String key) {
        logger.debug("Remove {} from {} buffer", key, bufferName);
        return buffer.remove(key);
    }

    @Override
    public T get(String key) {
        return buffer.get(key);
    }

    @Override
    public T put(@Nonnull String key, @Nonnull T value) {
        checkCapacity();
        return buffer.put(key, value);
    }

    @Override
    public Collection<Map.Entry<String, T>> entrySet() {
        return buffer.entrySet();
    }

    private void checkCapacity() {
        if (buffer.size() >= capacity) {
            String message =
                    bufferName + "buffer is full " + "(" + capacity + "/" + capacity + ") " + fullBufferMessage;
            throw new InsufficientBufferCapacityException(message);
        }
    }
}