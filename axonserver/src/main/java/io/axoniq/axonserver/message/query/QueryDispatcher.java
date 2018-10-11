package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.DispatchEvents;
import io.axoniq.axonserver.ProcessingInstructionHelper;
import io.axoniq.axonserver.SubscriptionEvents;
import io.axoniq.axonserver.TopologyEvents;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.ErrorMessageFactory;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.axonserver.grpc.query.QuerySubscription;
import io.axoniq.axonserver.message.FlowControlQueues;
import io.micrometer.core.instrument.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Comparator;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Author: marc
 */
@Component("QueryDispatcher")
public class QueryDispatcher {
    private static final String QUERY_COUNTER_NAME = "axon.queries.count";
    private static final String ACTIVE_QUERY_GAUGE = "axon.queries.active";

    private final Logger logger = LoggerFactory.getLogger(QueryDispatcher.class);
    private final QueryRegistrationCache registrationCache;
    private final QueryCache queryCache;
    private final QueryMetricsRegistry queryMetricsRegistry;
    private final FlowControlQueues<WrappedQuery> queryQueue = new FlowControlQueues<>(Comparator.comparing(WrappedQuery::priority).reversed());
    private final Counter queryCounter;

    public QueryDispatcher(QueryRegistrationCache registrationCache, QueryCache queryCache, QueryMetricsRegistry queryMetricsRegistry) {
        this.registrationCache = registrationCache;
        this.queryMetricsRegistry = queryMetricsRegistry;
        this.queryCache = queryCache;
        queryMetricsRegistry.gauge(ACTIVE_QUERY_GAUGE, queryCache, QueryCache::size);
        this.queryCounter = queryMetricsRegistry.counter(QUERY_COUNTER_NAME);
    }

    @EventListener
    public void on(DispatchEvents.DispatchQuery event) {
        if( event.isProxied()) {
            dispatchProxied(event.getQuery(),event.getCallback(), event.getOnCompleted());
        } else {
            query(event.getContext(), event.getQuery(), event.getCallback(), event.getOnCompleted());
        }
    }

    @EventListener
    public void on(SubscriptionEvents.UnsubscribeQuery event) {
        QuerySubscription unsubscribe = event.getUnsubscribe();
        QueryDefinition queryDefinition = new QueryDefinition(event.getContext(), unsubscribe);
        registrationCache.remove(queryDefinition, unsubscribe.getClientName());
    }

    @EventListener
    public void on(SubscriptionEvents.SubscribeQuery event) {
        QuerySubscription subscription = event.getSubscription();
        QueryDefinition queryDefinition = new QueryDefinition(event.getContext(), subscription);
        registrationCache.add(queryDefinition, subscription.getResultName(), event.getQueryHandler());
    }


    public void handleResponse(QueryResponse queryResponse, String client, boolean proxied) {
        String key = queryResponse.getRequestIdentifier();
        QueryInformation queryInformation = getQueryInformation(client, key);
        if( queryInformation != null) {
            if( queryInformation.forward(client, queryResponse) <= 0) {
                queryCache.remove(queryInformation.getKey());
                if (!proxied) {
                    queryMetricsRegistry.add(queryInformation.getQuery(), client,
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
                queryMetricsRegistry.add(queryInformation.getQuery(), client, System.currentTimeMillis() - queryInformation.getTimestamp());
            }
        } else {
            logger.debug("No (more) information for {} on completed", requestId);
        }
    }

    @EventListener
    public void on(TopologyEvents.ApplicationDisconnected event) {
        registrationCache.remove(event.getClient());
    }

    @EventListener
    public void on(TopologyEvents.QueryHandlerDisconnected event) {
        registrationCache.remove(event.getClient());
    }

    public void removeFromCache( String messageId) {
        queryCache.remove(messageId);
    }

    public FlowControlQueues<WrappedQuery> getQueryQueue() {
        return queryQueue;
    }

    public long getNrOfQueries() {
        return (long)queryCounter.count();
    }

    private void query(String context, QueryRequest query, Consumer<QueryResponse> callback, Consumer<String> onCompleted) {
        queryCounter.increment();
        long timeout = System.currentTimeMillis() + ProcessingInstructionHelper.timeout(query.getProcessingInstructionsList());
        Set<? extends QueryHandler> handlers = registrationCache.find(context, query);
        if( handlers.isEmpty()) {
            callback.accept(QueryResponse.newBuilder()
                                         .setErrorCode(ErrorCode.NO_HANDLER_FOR_QUERY.getCode())
                                         .setMessageIdentifier(query.getMessageIdentifier())
                                         .setMessage(ErrorMessageFactory.build("No handler for query: " + query.getQuery()))
                                         .build());
            onCompleted.accept("NoClient");
        } else {
            QueryDefinition queryDefinition =new QueryDefinition(context, query.getQuery());
            int expectedResults = Integer.MAX_VALUE;
            int nrOfResults = ProcessingInstructionHelper.numberOfResults(query.getProcessingInstructionsList());
            if( nrOfResults > 0) {
                expectedResults = Math.min(nrOfResults, expectedResults);
            }
            QueryInformation queryInformation = new QueryInformation(query.getMessageIdentifier(), queryDefinition,
                                                                     handlers.stream().map(QueryHandler::getClientName).collect(Collectors.toSet()),
                                                                     expectedResults, callback,
                                                                     onCompleted);
            queryCache.put(query.getMessageIdentifier(), queryInformation);
            handlers.forEach(h -> dispatchOne(h, context, query, timeout));
        }
    }

    private void dispatchProxied(QueryRequest query, Consumer<QueryResponse> callback, Consumer<String> onCompleted) {
        long timeout = System.currentTimeMillis() + ProcessingInstructionHelper.timeout(query.getProcessingInstructionsList());
        String context = ProcessingInstructionHelper.context(query.getProcessingInstructionsList());
        String client = ProcessingInstructionHelper.targetClient(query.getProcessingInstructionsList());
        QueryHandler queryHandler = registrationCache.find( context, query, client);
        if( queryHandler == null) {
            callback.accept(QueryResponse.newBuilder()
                                         .setErrorCode(ErrorCode.NO_HANDLER_FOR_QUERY.getCode())
                                         .setMessageIdentifier(query.getMessageIdentifier())
                                         .setMessage(ErrorMessageFactory.build("No handler for query: " + query.getQuery()))
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
                    queryDefinition, Collections.singleton(queryHandler.getClientName()),
                                                                     expectedResults,
                                                                     callback,
                                                                     onCompleted);
            queryCache.put(key, queryInformation);
            dispatchOne(queryHandler, context, query, timeout);
        }
    }

    private void dispatchOne(QueryHandler queryHandler, String context, QueryRequest query, long timeout) {
        queryHandler.enqueue(context, query, queryQueue, timeout);
    }


}
