/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.util.ConstraintCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.unit.DataSize;

import java.time.Clock;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * Cache for pending commands.
 * Has a scheduled task to check for commands that are pending for longer than the configured timeout
 * and will cancel these commands when timeout occurs.
 *
 * @author Marc Gathier
 */
@Component
public class CommandCache extends ConcurrentHashMap<String, CommandInformation>
        implements ConstraintCache<String, CommandInformation> {

    private final Logger logger = LoggerFactory.getLogger(CommandCache.class);
    private final long defaultCommandTimeout;
    private final Clock clock;
    private final long cacheCapacity;
    private final int COMMANDS_PER_GB = 25000;

    @Autowired
    public CommandCache(@Value("${axoniq.axonserver.default-command-timeout:300000}") long defaultCommandTimeout,
                        Clock clock, @Value("${axoniq.axonserver.command-cache-capacity:0}") long cacheCapacity) {
        this.defaultCommandTimeout = defaultCommandTimeout;
        this.clock = clock;

        if (cacheCapacity > 0) {
            this.cacheCapacity = cacheCapacity;
        } else {
            long totalMemory = DataSize.ofBytes(Runtime.getRuntime().maxMemory()).toGigabytes();
            this.cacheCapacity = (totalMemory > 0) ? (COMMANDS_PER_GB * totalMemory) : COMMANDS_PER_GB;
        }

    }

    public CommandCache(Clock clock) {
        this(300000, clock, 25000);
    }

    @Scheduled(fixedDelayString = "${axoniq.axonserver.cache-close-rate:5000}")
    public void clearOnTimeout() {
        logger.debug("Checking timed out commands");
        long minTimestamp = clock.millis() - defaultCommandTimeout;
        Set<Entry<String, CommandInformation>> toDelete = entrySet().stream().filter(e -> e.getValue().getTimestamp() < minTimestamp).collect(
                Collectors.toSet());
        if( ! toDelete.isEmpty()) {
            logger.warn("Found {} waiting commands to delete", toDelete.size());
            toDelete.forEach(e -> {
                logger.warn("Cancelling command {} sent by {}, waiting for reply from {}",
                            e.getValue().getRequestIdentifier(),
                            e.getValue().getSourceClientId(),
                            e.getValue().getClientStreamIdentification());
                remove(e.getKey());
                e.getValue().cancel();
            });
        }
    }

    @Override
    public CommandInformation put(@Nonnull String key, @Nonnull CommandInformation value) {
        checkCapacity();
        return super.put(key, value);
    }


    @Override
    public CommandInformation putIfAbsent(String key, CommandInformation value) {
        checkCapacity();
        return super.putIfAbsent(key, value);
    }

    @Override
    public void putAll(Map<? extends String, ? extends CommandInformation> m) {
        checkCapacity();
        super.putAll(m);
    }

    private void checkCapacity() {
        if (mappingCount() >= cacheCapacity) {
            throw new InsufficientBufferCapacityException("Command buffer is full " + "("+ cacheCapacity + "/" + cacheCapacity + ") "
            + "Command handlers might be slow. Try increasing 'axoniq.axonserver.command-cache-capacity' property.");
        }
    }
}
