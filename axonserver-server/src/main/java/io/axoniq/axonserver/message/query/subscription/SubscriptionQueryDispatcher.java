package io.axoniq.axonserver.message.query.subscription;

import io.axoniq.axonserver.SubscriptionEvents;
import io.axoniq.axonhub.SubscriptionQuery;
import io.axoniq.axonserver.SubscriptionQueryEvents.ProxiedSubscriptionQueryRequest;
import io.axoniq.axonserver.SubscriptionQueryEvents.SubscriptionQueryCanceled;
import io.axoniq.axonserver.SubscriptionQueryEvents.SubscriptionQueryRequestEvent;
import io.axoniq.axonhub.SubscriptionQueryRequest;
import io.axoniq.axonserver.TopologyEvents;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.message.query.QueryDefinition;
import io.axoniq.axonserver.message.query.QueryHandler;
import io.axoniq.axonserver.message.query.QueryRegistrationCache;
import io.axoniq.axonserver.message.query.subscription.DirectSubscriptionQueries.ContextSubscriptionQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Created by Sara Pellegrini on 04/05/2018.
 * sara.pellegrini@gmail.com
 */
@Component
public class SubscriptionQueryDispatcher {

    private final Logger logger = LoggerFactory.getLogger(SubscriptionQueryDispatcher.class);
    private final Iterable<ContextSubscriptionQuery> directSubscriptions;
    private final QueryRegistrationCache registrationCache;
    private final Map<String, Set<String>> subscriptionsSent = new ConcurrentHashMap<>();

    public SubscriptionQueryDispatcher(Iterable<ContextSubscriptionQuery> directSubscriptions,
                                       QueryRegistrationCache registrationCache) {
        this.directSubscriptions = directSubscriptions;
        this.registrationCache = registrationCache;
    }

    @EventListener
    public void on(ProxiedSubscriptionQueryRequest event) {
        SubscriptionQueryRequest request = event.subscriptionQueryRequest();
        SubscriptionQuery query = event.subscriptionQuery();
        QueryHandler handler = registrationCache.find(event.context(), query.getQueryRequest(), event.targetClient());
        handler.dispatch(request);

    }


    @EventListener
    public void on(SubscriptionQueryRequestEvent event) {
        logger.debug("Dispatch subscription query request with subscriptionId = {}", event.subscriptionId());
        SubscriptionQuery query = event.subscription();
        Set<? extends QueryHandler> handlers = registrationCache.findAll(event.context(), query.getQueryRequest());
        if (handlers == null || handlers.isEmpty()) {
            event.errorHandler().accept(new IllegalArgumentException(ErrorCode.NO_HANDLER_FOR_QUERY.getCode()));
            return;
        }
        handlers.forEach(handler -> {
            handler.dispatch(event.subscriptionQueryRequest());
            subscriptionsSent.computeIfAbsent(handler.getClientName(), client -> new CopyOnWriteArraySet<>()).add(event.subscriptionId());
        });
    }

    @EventListener
    public void on(SubscriptionQueryCanceled evt) {
        logger.debug("Dispatch subscription query cancel with subscriptionId = {}", evt.subscriptionId());
        SubscriptionQueryRequest queryRequest = SubscriptionQueryRequest.newBuilder()
                                                                        .setUnsubscribe(evt.unsubscribe())
                                                                        .setContext(evt.context())
                                                                        .build();
        Set<QueryHandler> handlers = registrationCache.findAll(evt.context(), evt.unsubscribe().getQueryRequest());
        handlers.forEach(handler -> handler.dispatch(queryRequest));
    }


    @EventListener
    public void on(SubscriptionEvents.SubscribeQuery event){
        String clientName = event.getSubscription().getClientName();
        QueryDefinition queryDefinition = new QueryDefinition(event.getContext(), event.getSubscription().getQuery());
        directSubscriptions.forEach(subscription -> {
            String subscriptionId = subscription.subscriptionQuery().getSubscriptionIdentifier();
            QueryDefinition query = new QueryDefinition(subscription.context(), subscription.queryName());
            Set<String> ids = subscriptionsSent.computeIfAbsent(clientName, client -> new CopyOnWriteArraySet<>());
            if (queryDefinition.equals(query) && !ids.contains(subscriptionId)) {
                SubscriptionQuery subscriptionQuery = subscription.subscriptionQuery();
                event.getQueryHandler().dispatch(SubscriptionQueryRequest.newBuilder()
                                                                         .setSubscribe(subscriptionQuery)
                                                                         .setContext(subscription.context())
                                                                         .build());
                ids.add(subscriptionId);
            }
        });
    }


    @EventListener
    public void on(TopologyEvents.ApplicationDisconnected event){
        subscriptionsSent.remove(event.getClient());
    }
}
