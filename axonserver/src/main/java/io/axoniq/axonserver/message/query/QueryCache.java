/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.exception.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * @author Marc Gathier
 */
@Component("QueryCache")
public class QueryCache extends ConcurrentHashMap<String, QueryInformation> {
    private final Logger logger = LoggerFactory.getLogger(QueryCache.class);
    private final long defaultQueryTimeout;

    public QueryCache(@Value("${axoniq.axonserver.default-query-timeout:300000}") long defaultQueryTimeout) {
        this.defaultQueryTimeout = defaultQueryTimeout;
    }

    public QueryInformation remove(String messagId) {
        logger.debug("Remove messageId {}", messagId);
        return super.remove(messagId);
    }

    @Scheduled(fixedDelayString = "${axoniq.axonserver.cache-close-rate:5000}")
    public void clearOnTimeout() {
        logger.debug("Checking timed out queries");
        long minTimestamp = System.currentTimeMillis() - defaultQueryTimeout;
        Set<Entry<String, QueryInformation>> toDelete = entrySet().stream().filter(e -> e.getValue().getTimestamp() < minTimestamp).collect(
                Collectors.toSet());
        if( ! toDelete.isEmpty()) {
            logger.warn("Found {} waiting queries to delete", toDelete.size());
            toDelete.forEach(e -> {
                logger.warn("Cancelling query {} sent by {}, waiting for reply from {}",
                            e.getValue().getQuery().getQueryName(),
                            e.getValue().getSourceClientId(),
                            e.getValue().waitingFor());
                remove(e.getKey());
                e.getValue().cancel();
            });
        }
    }

    //TODO
    @EventListener
    public void on(TopologyEvents.ApplicationDisconnected applicationDisconnected) {
        forEach((key, value) -> completeForApplication(value, applicationDisconnected.getClientId()));
    }

    //TODO
    @EventListener
    public void on(TopologyEvents.QueryHandlerDisconnected queryHandlerDisconnected) {
        forEach((key, value) -> completeForApplication(value, queryHandlerDisconnected.getClient()));
    }

    private void completeForApplication(QueryInformation entry, String client) {
        if( entry.waitingFor(client) && entry.completeWithError(client, ErrorCode.CONNECTION_TO_HANDLER_LOST,
                                                                format("Connection to handler %s lost", client))) {
            remove(entry.getKey());
        }
    }

}
