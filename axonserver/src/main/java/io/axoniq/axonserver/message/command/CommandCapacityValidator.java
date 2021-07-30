/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.grpc.SerializedCommand;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static io.axoniq.axonserver.util.ObjectUtils.getOrDefault;

/**
 * @author Marc Gathier
 * @since
 */
@Component
public class CommandCapacityValidator implements CapacityValidator {

    private final Map<String, Integer> limitPerContext = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> remainingMessagesPerContext = new ConcurrentHashMap<>();
    private final int defaultLimitPerContext;
    private final ContextLimitProvider contextLimitProvider;

    public CommandCapacityValidator(@Value("${axoniq.axonserver.messages.context-buffer-limit:-1}") int defaultLimitPerContext,
                                    ContextLimitProvider contextLimitProvider ) {
        this.defaultLimitPerContext = defaultLimitPerContext;
        this.contextLimitProvider = contextLimitProvider;
    }

    @Override
    public void checkCapacity(String context, SerializedCommand command) {
        int bufferLimit = limitPerContext.computeIfAbsent(context, this::getLimitForContext);
        if (bufferLimit != -1) {
            remainingMessagesPerContext.computeIfAbsent(context, k -> new AtomicInteger(bufferLimit)).getAndUpdate(s -> {
                if (s <= 0) {
                    throw new InsufficientBufferCapacityException("Buffer is full, slow down.");
                } else {
                    return s - 1;
                }
            });
        }
    }

    private int getLimitForContext(String context) {
        return getOrDefault(contextLimitProvider.maxPendingCommands(context), defaultLimitPerContext);
    }

    @Override
    public void requestDone(String context, SerializedCommand command) {
        AtomicInteger current = remainingMessagesPerContext.get(context);
        if (current != null) {
            current.incrementAndGet();
        }
    }
}
