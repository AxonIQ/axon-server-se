/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.applicationevents.SubscriptionEvents;
import io.axoniq.axonserver.applicationevents.SubscriptionQueryEvents.SubscriptionQueryResponseReceived;
import io.axoniq.axonserver.applicationevents.TopologyEvents.QueryHandlerDisconnected;
import io.axoniq.axonserver.grpc.query.QueryProviderInbound;
import io.axoniq.axonserver.grpc.query.QueryProviderOutbound;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.axonserver.grpc.query.QueryServiceGrpc;
import io.axoniq.axonserver.grpc.query.QuerySubscription;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryResponse;
import io.axoniq.axonserver.message.ClientIdentification;
import io.axoniq.axonserver.message.query.DirectQueryHandler;
import io.axoniq.axonserver.message.query.QueryDispatcher;
import io.axoniq.axonserver.message.query.QueryHandler;
import io.axoniq.axonserver.message.query.QueryResponseConsumer;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.PreDestroy;

/**
 * GRPC service to handle query bus requests from Axon Application
 * Client can sent two requests:
 * query: sends a singe query to AxonServer
 * openStream: used by application providing query handlers, maintains an open bi directional connection between the application and AxonServer
 * @author Marc Gathier
 */
@Service("QueryService")
public class QueryService extends QueryServiceGrpc.QueryServiceImplBase implements AxonServerClientService {

    private final QueryDispatcher queryDispatcher;
    private final ContextProvider contextProvider;
    private final ApplicationEventPublisher eventPublisher;
    private final Logger logger = LoggerFactory.getLogger(QueryService.class);
    private final Set<GrpcQueryDispatcherListener> dispatcherListenerSet = new CopyOnWriteArraySet<>();

    @Value("${axoniq.axonserver.query-threads:1}")
    private int processingThreads = 1;


    public QueryService(QueryDispatcher queryDispatcher, ContextProvider contextProvider,
                        ApplicationEventPublisher eventPublisher) {
        this.queryDispatcher = queryDispatcher;
        this.contextProvider = contextProvider;
        this.eventPublisher = eventPublisher;
    }

    @PreDestroy
    public void cleanup() {
        dispatcherListenerSet.forEach(GrpcFlowControlledDispatcherListener::cancel);
        dispatcherListenerSet.clear();
    }

    @Override
    public StreamObserver<QueryProviderOutbound> openStream(
            StreamObserver<QueryProviderInbound> inboundStreamObserver) {
        String context = contextProvider.getContext();

        SendingStreamObserver<QueryProviderInbound> wrappedQueryProviderInboundObserver = new SendingStreamObserver<>(
                inboundStreamObserver);

        return new ReceivingStreamObserver<QueryProviderOutbound>(logger) {
            private AtomicReference<GrpcQueryDispatcherListener> listener = new AtomicReference<>();
            private AtomicReference<ClientIdentification> client = new AtomicReference<>();
            private AtomicReference<QueryHandler> queryHandler = new AtomicReference<>();

            @Override
            protected void consume(QueryProviderOutbound queryProviderOutbound) {
                switch (queryProviderOutbound.getRequestCase()) {
                    case SUBSCRIBE:
                        QuerySubscription subscription = queryProviderOutbound
                                .getSubscribe();
                        checkInitClient(subscription.getClientId(), subscription.getComponentName());
                        eventPublisher.publishEvent(
                                new SubscriptionEvents.SubscribeQuery(context,
                                                                      queryProviderOutbound
                                                                              .getSubscribe(),
                                                                      queryHandler.get()));
                        break;
                    case UNSUBSCRIBE:
                        if (client.get() != null) {
                            eventPublisher.publishEvent(
                                    new SubscriptionEvents.UnsubscribeQuery(context,
                                                                            queryProviderOutbound
                                                                                    .getUnsubscribe(),
                                                                            false));
                        }
                        break;
                    case FLOW_CONTROL:
                        flowControl(queryProviderOutbound.getFlowControl());
                        break;
                    case QUERY_RESPONSE:
                        queryDispatcher.handleResponse(queryProviderOutbound.getQueryResponse(),
                                                       client.get().getClient(),
                                                       false);
                        break;
                    case QUERY_COMPLETE:
                        queryDispatcher.handleComplete(queryProviderOutbound.getQueryComplete().getRequestId(),
                                                       client.get().getClient(),
                                                       false);
                        break;
                    case SUBSCRIPTION_QUERY_RESPONSE:
                        SubscriptionQueryResponse response = queryProviderOutbound.getSubscriptionQueryResponse();
                        eventPublisher.publishEvent(new SubscriptionQueryResponseReceived(response));
                        break;
                    case REQUEST_NOT_SET:
                        break;
                }
            }

            private void flowControl(FlowControl flowControl) {
                client.compareAndSet(null, new ClientIdentification(context,
                                                                    flowControl.getClientId()));
                if (listener.compareAndSet(null, new GrpcQueryDispatcherListener(queryDispatcher,
                                                                                 client.get().toString(),
                                                                                 wrappedQueryProviderInboundObserver,
                                                                                 processingThreads))) {
                    dispatcherListenerSet.add(listener.get());
                }
                listener.get().addPermits(flowControl.getPermits());
            }

            private void checkInitClient(String clientId, String componentName) {
                client.compareAndSet(null, new ClientIdentification(context, clientId));
                queryHandler.compareAndSet(null,
                                           new DirectQueryHandler(wrappedQueryProviderInboundObserver, client.get(),
                                                                  componentName));
            }

            @Override
            protected String sender() {
                return client.toString();
            }

            @Override
            public void onError(Throwable cause) {
                logger.warn("{}: Error on connection from subscriber - {}", client, cause.getMessage());

                cleanup();
            }

            private void cleanup() {
                if (client.get() != null) {
                    eventPublisher.publishEvent(new QueryHandlerDisconnected(context, client.get().getClient()));
                }
                if (listener.get() != null) {
                    dispatcherListenerSet.remove(listener.get());
                    listener.get().cancel();
                }
            }

            @Override
            public void onCompleted() {
                cleanup();

                try {
                    inboundStreamObserver.onCompleted();
                } catch (RuntimeException cause) {
                    logger.warn("{}: Error completing connection to subscriber - {}", client, cause.getMessage());
                }
            }
        };
    }

    @Override
    public void query(QueryRequest request, StreamObserver<QueryResponse> responseObserver) {
        if (logger.isTraceEnabled()) {
            logger.trace("{}: Received query: {}", request.getClientId(), request);
        }
        GrpcQueryResponseConsumer responseConsumer = new GrpcQueryResponseConsumer(responseObserver);
        queryDispatcher.query(new SerializedQuery(contextProvider.getContext(), request),
                              responseConsumer::onNext,
                              result -> responseConsumer.onCompleted());
    }

    @Override
    public StreamObserver<SubscriptionQueryRequest> subscription(
            StreamObserver<SubscriptionQueryResponse> responseObserver) {
        String context = contextProvider.getContext();
        return new SubscriptionQueryRequestTarget(context, responseObserver, eventPublisher);
    }

    public Set<GrpcQueryDispatcherListener> listeners() {
        return dispatcherListenerSet;
    }

    private class GrpcQueryResponseConsumer implements QueryResponseConsumer {

        private final SendingStreamObserver<QueryResponse> responseObserver;

        GrpcQueryResponseConsumer(StreamObserver<QueryResponse> responseObserver) {
            this.responseObserver = new SendingStreamObserver<>(responseObserver);
        }

        @Override
        public void onNext(QueryResponse queryResponse) {
            responseObserver.onNext(queryResponse);
        }

        @Override
        public void onCompleted() {
            responseObserver.onCompleted();
        }
    }
}
