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
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.message.FlowControlQueues;
import io.axoniq.axonserver.message.command.InsufficientCacheCapacityException;
import io.axoniq.axonserver.metric.BaseMetricName;
import io.axoniq.axonserver.metric.MeterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Collections.singleton;

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


    /**
     * Handles a received {@link QueryResponse}.
     *
     * @param queryResponse  the {@link QueryResponse} has been received
     * @param clientStreamId the query long living stream identifier that the client used to send the response
     * @param clientId
     * @param proxied        {@code true} if the response has been proxied by another AS node, {@code true} if the
     *                       response is directly received from the client handler.
     */
    public void handleResponse(QueryResponse queryResponse,
                               String clientStreamId,
                               String clientId,
                               boolean proxied) {
        String requestIdentifier = queryResponse.getRequestIdentifier();
        QueryInformation queryInformation = getQueryInformation(clientStreamId, requestIdentifier);
        if (queryInformation != null) {
            ClientStreamIdentification clientStream = new ClientStreamIdentification(queryInformation.getContext(),
                                                                                     clientStreamId);
            if (queryInformation.forward(clientStreamId, queryResponse) <= 0) {
                queryCache.remove(queryInformation.getKey());
                if (!proxied) {
                    queryMetricsRegistry.add(queryInformation.getQuery(),
                                             queryInformation.getSourceClientId(), clientId,
                                             clientStream.getContext(),
                                             System.currentTimeMillis() - queryInformation.getTimestamp());
                }
            }
        } else {
            logger.debug("No (more) information for {}", queryResponse.getRequestIdentifier());
        }
    }

    private QueryInformation getQueryInformation(String clientStreamId, String requestIdentifier) {
        QueryInformation queryInformation = queryCache.get(requestIdentifier);
        if (queryInformation == null) {
            requestIdentifier = requestIdentifier + "/" + clientStreamId;
            queryInformation = queryCache.get(requestIdentifier);
        }
        return queryInformation;
    }

    public void handleComplete(String requestId, String clientStreamId, String clientId, boolean proxied) {
        QueryInformation queryInformation = getQueryInformation(clientStreamId, requestId);
        if (queryInformation != null) {
            if (queryInformation.completed(clientStreamId)) {
                queryCache.remove(queryInformation.getKey());
            }
            if (!proxied) {
                queryMetricsRegistry.add(queryInformation.getQuery(),
                                         queryInformation.getSourceClientId(),
                                         clientId,
                                         queryInformation.getContext(),
                                         System.currentTimeMillis() - queryInformation.getTimestamp());
            }
        } else {
            logger.debug("No (more) information for {} on completed", requestId);
        }
    }

    @EventListener
    public void on(TopologyEvents.QueryHandlerDisconnected event) {
        registrationCache.remove(event.clientIdentification());
    }

    /**
     * Removes the query from the cache and completes it with a {@link ErrorCode#COMMAND_TIMEOUT} error.
     *
     * @param client
     * @param messageId
     */
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
        long timeout =
                System.currentTimeMillis() + ProcessingInstructionHelper.timeout(query.getProcessingInstructionsList());
        Set<? extends QueryHandler> handlers = registrationCache.find(serializedQuery.context(), query);
        if (handlers.isEmpty()) {
            callback.accept(QueryResponse.newBuilder()
                                         .setErrorCode(ErrorCode.NO_HANDLER_FOR_QUERY.getCode())
                                         .setMessageIdentifier(query.getMessageIdentifier())
                                         .setErrorMessage(ErrorMessageFactory
                                                                  .build("No handler for query: " + query.getQuery()))
                                         .build());
            onCompleted.accept("NoClient");
        } else {
            QueryDefinition queryDefinition = new QueryDefinition(serializedQuery.context(), query.getQuery());
            int expectedResults = Integer.MAX_VALUE;
            int nrOfResults = ProcessingInstructionHelper.numberOfResults(query.getProcessingInstructionsList());
            if (nrOfResults > 0) {
                expectedResults = nrOfResults;
            }
            QueryInformation queryInformation = new QueryInformation(query.getMessageIdentifier(),
                                                                     query.getClientId(), queryDefinition,
                                                                     handlers.stream()
                                                                             .map(QueryHandler::getClientStreamId)
                                                                             .collect(Collectors.toSet()),
                                                                     expectedResults, callback,
                                                                     onCompleted);
            try {
                queryCache.put(query.getMessageIdentifier(), queryInformation);
                handlers.forEach(h -> dispatchOne(h, serializedQuery, timeout));
            } catch (InsufficientCacheCapacityException insufficientCacheCapacityException) {
                callback.accept(QueryResponse.newBuilder()
                                             .setErrorCode(ErrorCode.QUERY_DISPATCH_ERROR.getCode())
                                             .setMessageIdentifier(query.getMessageIdentifier())
                                             .setErrorMessage(ErrorMessageFactory
                                                                      .build(insufficientCacheCapacityException
                                                                                     .getMessage()))
                                             .build());
                onCompleted.accept("NoCapacity");
            }
        }
    }

    public MeterFactory.RateMeter queryRate(String context) {
        return queryRatePerContext.computeIfAbsent(context,
                                                   c -> queryMetricsRegistry
                                                           .rateMeter(c, BaseMetricName.AXON_QUERY_RATE));
    }

    public void dispatchProxied(SerializedQuery serializedQuery, Consumer<QueryResponse> callback,
                                Consumer<String> onCompleted) {
        QueryRequest query = serializedQuery.query();
        long timeout =
                System.currentTimeMillis() + ProcessingInstructionHelper.timeout(query.getProcessingInstructionsList());
        String context = serializedQuery.context();
        String clientId = serializedQuery.clientStreamId();
        QueryHandler queryHandler = registrationCache.find(context, query, clientId);
        if (queryHandler == null) {
            callback.accept(QueryResponse.newBuilder()
                                         .setErrorCode(ErrorCode.CLIENT_DISCONNECTED.getCode())
                                         .setMessageIdentifier(query.getMessageIdentifier())
                                         .setErrorMessage(
                                                 ErrorMessageFactory
                                                         .build(String.format("Client %s not found while processing: %s"
                                                                 , clientId, query.getQuery())))
                                         .build());
            onCompleted.accept(clientId);
        } else {
            QueryDefinition queryDefinition = new QueryDefinition(context, query.getQuery());
            int expectedResults = Integer.MAX_VALUE;
            int nrOfResults = ProcessingInstructionHelper.numberOfResults(query.getProcessingInstructionsList());
            if (nrOfResults > 0) {
                expectedResults = nrOfResults;
            }
            String key = query.getMessageIdentifier() + "/" + serializedQuery.clientStreamId();
            QueryInformation queryInformation = new QueryInformation(key,
                                                                     serializedQuery.query().getClientId(),
                                                                     queryDefinition,
                                                                     singleton(queryHandler.getClientStreamId()),
                                                                     expectedResults,
                                                                     callback,
                                                                     onCompleted);
            try {
                queryCache.put(key, queryInformation);
                dispatchOne(queryHandler, serializedQuery, timeout);
            } catch (InsufficientCacheCapacityException insufficientCacheCapacityException) {
                queryInformation.completeWithError(queryHandler.getClientId(),
                                                   ErrorCode.QUERY_DISPATCH_ERROR,
                                                   insufficientCacheCapacityException.getMessage());
            }
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
