/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Cache for running commands.
 * @author Marc Gathier
 */
@Component
public class CommandCache extends ConcurrentHashMap<String, CommandInformation> {
    private final Logger logger = LoggerFactory.getLogger(CommandCache.class);
    private final long defaultCommandTimeout;
    private final Clock clock;

    @Autowired
    public CommandCache(@Value("${axoniq.axonserver.default-command-timeout:300000}") long defaultCommandTimeout, Clock clock) {
        this.defaultCommandTimeout = defaultCommandTimeout;
        this.clock = clock;
    }

    public CommandCache(Clock clock) {
        this(300000, clock);
    }

    @Scheduled(fixedDelayString = "${axoniq.axonserver.cache-close-rate:5000}")
    public void clearOnTimeout() {
        logger.debug("Checking timed out queries");
        long minTimestamp = clock.millis() - defaultCommandTimeout;
        Set<Entry<String, CommandInformation>> toDelete = entrySet().stream().filter(e -> e.getValue().getTimestamp() < minTimestamp).collect(
                Collectors.toSet());
        if( ! toDelete.isEmpty()) {
            logger.warn("Found {} waiting commands to delete", toDelete.size());
            toDelete.forEach(e -> {
                remove(e.getKey());
                e.getValue().cancel();
            });
        }
    }

}
