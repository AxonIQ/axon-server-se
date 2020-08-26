/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.ProcessingInstructionHelper;
import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.ErrorMessageFactory;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.SerializedQuery;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.axonserver.message.ClientIdentification;
import io.axoniq.axonserver.message.FlowControlQueues;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.metric.BaseMetricName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author Marc Gathier
 */
@Component("QueryDispatcher")
public class QueryDispatcher {
    private final Logger logger = LoggerFactory.getLogger(QueryDispatcher.class);
    private final QueryRegistrationCache registrationCache;
    private final QueryCache queryCache;
    private final QueryMetricsRegistry queryMetricsRegistry;
    private final FlowControlQueues<WrappedQuery> queryQueue;
    private final Map<String, MeterFactory.RateMeter> queryRatePerContext = new ConcurrentHashMap<>();

    public QueryDispatcher(QueryRegistrationCache registrationCache, QueryCache queryCache,
                           QueryMetricsRegistry queryMetricsRegistry,
                           MeterFactory meterFactory,
                           @Value("${axoniq.axonserver.query-queue-capacity-per-client:10000}") int queueCapacity) {
        this.registrationCache = registrationCache;
        this.queryMetricsRegistry = queryMetricsRegistry;
        this.queryCache = queryCache;
        queryQueue = new FlowControlQueues<>(Comparator.comparing(WrappedQuery::priority).reversed(),
                                             queueCapacity,
                                             BaseMetricName.AXON_APPLICATION_QUERY_QUEUE_SIZE,
                                             meterFactory,
                                             ErrorCode.QUERY_DISPATCH_ERROR);
        queryMetricsRegistry.gauge(BaseMetricName.AXON_ACTIVE_QUERIES, queryCache, QueryCache::size);
    }



    public void handleResponse(QueryResponse queryResponse, String client, boolean proxied) {
        String key = queryResponse.getRequestIdentifier();
        QueryInformation queryInformation = getQueryInformation(client, key);
        if( queryInformation != null) {
            if( queryInformation.forward(client, queryResponse) <= 0) {
                queryCache.remove(queryInformation.getKey());
                if (!proxied) {
                    queryMetricsRegistry.add(queryInformation.getQuery(),
                                             queryInformation.getSourceClientId(),
                                             new ClientIdentification(queryInformation.getContext(), client),
                                             System.currentTimeMillis() - queryInformation.getTimestamp());

                }
            }
        } else {
            logger.debug("No (more) information for {}", queryResponse.getRequestIdentifier());
        }
    }

    private QueryInformation getQueryInformation(String client, String requestIdentifier) {
        QueryInformation queryInformation = queryCache.get(requestIdentifier);
        if( queryInformation == null) {
            requestIdentifier = requestIdentifier + "/" + client;
            queryInformation = queryCache.get(requestIdentifier);
        }
        return queryInformation;
    }

    public void handleComplete(String requestId, String client, boolean proxied) {
        QueryInformation queryInformation = getQueryInformation(client, requestId);
        if( queryInformation != null) {
            if( queryInformation.completed(client)) {
                queryCache.remove(queryInformation.getKey());
            }
            if (!proxied) {
                queryMetricsRegistry.add(queryInformation.getQuery(),
                                         queryInformation.getSourceClientId(),
                                         new ClientIdentification(queryInformation.getContext(), client),
                                         System.currentTimeMillis() - queryInformation.getTimestamp());
            }
        } else {
            logger.debug("No (more) information for {} on completed", requestId);
        }
    }

    @EventListener
    public void on(TopologyEvents.ApplicationDisconnected event) {
        registrationCache.remove(event.clientIdentification());
    }

    @EventListener
    public void on(TopologyEvents.QueryHandlerDisconnected event) {
        registrationCache.remove(event.clientIdentification());
    }

    public void removeFromCache(String client, String messageId) {
        QueryInformation query = queryCache.remove(messageId);
        if (query != null) {
            query.completeWithError(client, ErrorCode.COMMAND_TIMEOUT, "Query cancelled due to timeout");
        }
    }

    public FlowControlQueues<WrappedQuery> getQueryQueue() {
        return queryQueue;
    }


    public void query(SerializedQuery serializedQuery, Consumer<QueryResponse> callback, Consumer<String> onCompleted) {
        queryRate(serializedQuery.context()).mark();
        QueryRequest query = serializedQuery.query();
        long timeout = System.currentTimeMillis() + ProcessingInstructionHelper.timeout(query.getProcessingInstructionsList());
        Set<? extends QueryHandler> handlers = registrationCache.find(serializedQuery.context(), query);
        if( handlers.isEmpty()) {
            callback.accept(QueryResponse.newBuilder()
                                         .setErrorCode(ErrorCode.NO_HANDLER_FOR_QUERY.getCode())
                                         .setMessageIdentifier(query.getMessageIdentifier())
                                         .setErrorMessage(ErrorMessageFactory.build("No handler for query: " + query.getQuery()))
                                         .build());
            onCompleted.accept("NoClient");
        } else {
            QueryDefinition queryDefinition =new QueryDefinition(serializedQuery.context(), query.getQuery());
            int expectedResults = Integer.MAX_VALUE;
            int nrOfResults = ProcessingInstructionHelper.numberOfResults(query.getProcessingInstructionsList());
            if( nrOfResults > 0) {
                expectedResults = Math.min(nrOfResults, expectedResults);
            }
            QueryInformation queryInformation = new QueryInformation(query.getMessageIdentifier(),
                                                                     query.getClientId(), queryDefinition,
                                                                     handlers.stream().map(QueryHandler::getClientId).collect(Collectors.toSet()),
                                                                     expectedResults, callback,
                                                                     onCompleted);
            queryCache.put(query.getMessageIdentifier(), queryInformation);
            handlers.forEach(h -> dispatchOne(h, serializedQuery, timeout));
        }
    }

    public MeterFactory.RateMeter queryRate(String context) {
        return queryRatePerContext.computeIfAbsent(context,
                                                   c -> queryMetricsRegistry
                                                           .rateMeter(c, BaseMetricName.AXON_QUERY_RATE));
    }

    public void dispatchProxied(SerializedQuery serializedQuery, Consumer<QueryResponse> callback, Consumer<String> onCompleted) {
        QueryRequest query = serializedQuery.query();
        long timeout = System.currentTimeMillis() + ProcessingInstructionHelper.timeout(query.getProcessingInstructionsList());
        String context = serializedQuery.context();
        String client = serializedQuery.client();
        QueryHandler queryHandler = registrationCache.find( context, query, client);
        if( queryHandler == null) {
            callback.accept(QueryResponse.newBuilder()
                                         .setErrorCode(ErrorCode.CLIENT_DISCONNECTED.getCode())
                                         .setMessageIdentifier(query.getMessageIdentifier())
                                         .setErrorMessage(
                                                 ErrorMessageFactory.build(String.format("Client %s not found while processing: %s"
                                                         , client, query.getQuery())))
                                         .build());
            onCompleted.accept(client);
        } else {
            QueryDefinition queryDefinition = new QueryDefinition(context, query.getQuery());
            int expectedResults = Integer.MAX_VALUE;
            int nrOfResults = ProcessingInstructionHelper.numberOfResults(query.getProcessingInstructionsList());
            if( nrOfResults > 0) {
                expectedResults = Math.min(nrOfResults, expectedResults);
            }
            String key = query.getMessageIdentifier() + "/" + client;
            QueryInformation queryInformation = new QueryInformation(key,
                                                                     serializedQuery.query().getClientId(),
                                                                     queryDefinition,
                                                                     Collections.singleton(queryHandler.getClientId()),
                                                                     expectedResults,
                                                                     callback,
                                                                     onCompleted);
            queryCache.put(key, queryInformation);
            dispatchOne(queryHandler, serializedQuery, timeout);
        }
    }

    private void dispatchOne(QueryHandler queryHandler, SerializedQuery query, long timeout) {
        try {
            queryHandler.enqueue(query, queryQueue, timeout);
        } catch (MessagingPlatformException mpe) {
            QueryInformation information = queryCache.remove(query.getMessageIdentifier());
            if (information != null) {
                information.completeWithError(queryHandler.getClientId(), mpe.getErrorCode(), mpe.getMessage());
            }
        }
    }
}
