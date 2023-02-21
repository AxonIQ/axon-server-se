/*
 * Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.message.command.InsufficientBufferCapacityException;
import io.axoniq.axonserver.util.NonReplacingConstraintCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.unit.DataSize;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Cache for all active queries this instance of AS is involved into. Extends a {@link ConcurrentHashMap} where the key
 * represents the unique identifier of the query request message.
 *
 * @author Marc Gathier
 */
@Component("QueryCache")
public class QueryCache
        implements NonReplacingConstraintCache<String, ActiveQuery> {

    private final Logger logger = LoggerFactory.getLogger(QueryCache.class);
    private final long defaultQueryTimeout;
    private final long cacheCapacity;
    private final int QUERIES_PER_GB = 25000;
    private final Map<String, ActiveQuery> map = new ConcurrentHashMap<>();

    public QueryCache(@Value("${axoniq.axonserver.default-query-timeout:300000}") long defaultQueryTimeout,
                      @Value("${axoniq.axonserver.query-cache-capacity:0}") long cacheCapacity) {
        this.defaultQueryTimeout = defaultQueryTimeout;

        if (cacheCapacity > 0) {
            this.cacheCapacity = cacheCapacity;
        } else {
            long totalMemory = DataSize.ofBytes(Runtime.getRuntime().maxMemory()).toGigabytes();
            this.cacheCapacity = (totalMemory > 0) ? (QUERIES_PER_GB * totalMemory) : QUERIES_PER_GB;
        }
    }

    @Override
    public int size() {
        return map.size();
    }

    public ActiveQuery remove(String messageId) {
        logger.debug("Remove messageId {}", messageId);
        return map.remove(messageId);
    }

    @Override
    public ActiveQuery get(String key) {
        return map.get(key);
    }

    @Scheduled(fixedDelayString = "${axoniq.axonserver.cache-close-rate:5000}")
    public void clearOnTimeout() {
        logger.debug("Checking timed out queries");
        long minTimestamp = System.currentTimeMillis() - defaultQueryTimeout;
        Set<Map.Entry<String, ActiveQuery>> toDelete = entrySet().stream()
                                                                 .filter(e -> e.getValue().getTimestamp()
                                                                         < minTimestamp)
                                                                 .filter(e -> !e.getValue()
                                                                                .isStreaming()) // streaming queries can last theoretically forever, let's keep them in cache
                                                                 .collect(Collectors.toSet());
        if (!toDelete.isEmpty()) {
            logger.warn("Found {} waiting queries to delete", toDelete.size());
            toDelete.forEach(e -> {
                logger.warn("Cancelling query {} sent by {}, waiting for reply from {}",
                            e.getValue().getQuery().getQueryName(),
                            e.getValue().getSourceClientId(),
                            e.getValue().waitingFor());
                remove(e.getKey());
                e.getValue().cancelWithError(ErrorCode.QUERY_TIMEOUT, "Query cancelled due to timeout");
            });
        }
    }

    @EventListener
    public void on(TopologyEvents.QueryHandlerDisconnected queryHandlerDisconnected) {
        map.forEach((key, value) -> completeForApplication(value, queryHandlerDisconnected.getClientStreamId()));
    }

    private void completeForApplication(ActiveQuery activeQuery, String handlerClientStreamId) {
        logger.debug("Complete query {} for query handler identified by clientStreamId {}.",
                     activeQuery.getKey(), handlerClientStreamId);
        if (activeQuery.waitingFor(handlerClientStreamId)) {
            boolean noHandlers = activeQuery.completeWithError(handlerClientStreamId,
                                                               ErrorCode.CONNECTION_TO_HANDLER_LOST,
                                                               format("Connection to handler %s lost",
                                                                      handlerClientStreamId));
            if (noHandlers) {
                remove(activeQuery.getKey());
            }
        }
    }


    /**
     * This operation is performed atomically w.r.t. the insert itself, not the constraints.
     */
    @Override
    public ActiveQuery putIfAbsent(String key, ActiveQuery value) {
        checkCapacity();
        return map.putIfAbsent(key, value);
    }


    @Override
    public Collection<Map.Entry<String, ActiveQuery>> entrySet() {
        return map.entrySet();
    }


    private void checkCapacity() {
        if (map.size() >= cacheCapacity) {
            throw new InsufficientBufferCapacityException(
                    "Query buffer is full " + "(" + cacheCapacity + "/" + cacheCapacity + ") "
                            + "Query handlers might be slow. Try increasing 'axoniq.axonserver.query-cache-capacity' property.");
        }
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }
}
