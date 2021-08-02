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
import org.springframework.util.unit.DataSize;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Marc Gathier
 * @since
 */
@Component
public class CommandCapacityValidator implements CapacityValidator {

    private final AtomicInteger pendingCommands = new AtomicInteger();
    private final long cacheCapacity;
    private static final int COMMANDS_PER_GB = 25000;

    public CommandCapacityValidator(@Value("${axoniq.axonserver.command-cache-capacity:0}") long cacheCapacity ) {
        if (cacheCapacity > 0) {
            this.cacheCapacity = cacheCapacity;
        } else {
            long totalMemory = DataSize.ofBytes(Runtime.getRuntime().maxMemory()).toGigabytes();
            this.cacheCapacity = (totalMemory > 0) ? (COMMANDS_PER_GB * totalMemory) : COMMANDS_PER_GB;
        }
    }

    @Override
    public void checkCapacity(String context, SerializedCommand command) {
        pendingCommands.updateAndGet(current -> {
            if (current >= cacheCapacity) {
                throw new InsufficientBufferCapacityException("Command buffer is full " + "("+ cacheCapacity + "/" + cacheCapacity + ") "
                                                                      + "Command handlers might be slow. Try increasing 'axoniq.axonserver.command-cache-capacity' property.");
            }
            return current+1;
        });
    }

    @Override
    public void requestDone(String context, SerializedCommand command) {
        pendingCommands.decrementAndGet();
    }
}
