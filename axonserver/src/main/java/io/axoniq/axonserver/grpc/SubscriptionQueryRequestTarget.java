/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.applicationevents.SubscriptionQueryEvents.SubscriptionQueryCanceled;
import io.axoniq.axonserver.applicationevents.SubscriptionQueryEvents.SubscriptionQueryInitialResultRequested;
import io.axoniq.axonserver.applicationevents.SubscriptionQueryEvents.SubscriptionQueryRequested;
import io.axoniq.axonserver.grpc.query.SubscriptionQuery;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryResponse;
import io.axoniq.axonserver.interceptor.DefaultInterceptorContext;
import io.axoniq.axonserver.interceptor.SubscriptionQueryInterceptors;
import io.axoniq.axonserver.message.query.subscription.UpdateHandler;
import io.axoniq.axonserver.message.query.subscription.handler.DirectUpdateHandler;
import io.axoniq.axonserver.util.StreamObserverUtils;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.security.core.Authentication;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * @author Sara Pellegrini
 */
public class SubscriptionQueryRequestTarget extends ReceivingStreamObserver<SubscriptionQueryRequest> {

    private static final Logger logger = LoggerFactory.getLogger(SubscriptionQueryRequestTarget.class);

    private final String context;
    private final SubscriptionQueryInterceptors subscriptionQueryInterceptors;

    private final QueryResponseStreamObserver responseObserver;

    private final ApplicationEventPublisher eventPublisher;

    private final List<SubscriptionQuery> subscriptionQuery;

    private final UpdateHandler updateHandler;

    private final Consumer<Throwable> errorHandler;
    private final DefaultInterceptorContext extensionUnitOfWork;

    private volatile String clientId;

    SubscriptionQueryRequestTarget(
            String context, Authentication authentication,
            StreamObserver<SubscriptionQueryResponse> responseObserver,
            SubscriptionQueryInterceptors subscriptionQueryInterceptors,
            ApplicationEventPublisher eventPublisher) {
        super(LoggerFactory.getLogger(SubscriptionQueryRequestTarget.class));
        this.context = context;
        this.subscriptionQueryInterceptors = subscriptionQueryInterceptors;
        this.extensionUnitOfWork = new DefaultInterceptorContext(context, authentication);
        this.errorHandler = e -> responseObserver.onError(GrpcExceptionBuilder.build(e));
        this.responseObserver = new QueryResponseStreamObserver(new FlowControlledStreamObserver<>(responseObserver,
                                                                                                   errorHandler));
        this.updateHandler = new DirectUpdateHandler(this.responseObserver::onNext);
        this.eventPublisher = eventPublisher;
        this.subscriptionQuery = new ArrayList<>();
    }

    @Override
    protected void consume(SubscriptionQueryRequest message) {
        try {
            message = subscriptionQueryInterceptors.subscriptionQueryRequest(message, extensionUnitOfWork);
            switch (message.getRequestCase()) {
                case SUBSCRIBE:
                    if (clientId == null) {
                        clientId = message.getSubscribe().getQueryRequest().getClientId();
                    }
                    subscriptionQuery.add(message.getSubscribe());
                    eventPublisher.publishEvent(new SubscriptionQueryRequested(context,
                                                                               subscriptionQuery.get(0),
                                                                               updateHandler,
                                                                               errorHandler));

                    break;
                case GET_INITIAL_RESULT:
                    if (subscriptionQuery.isEmpty()) {
                        errorHandler.accept(new IllegalStateException("Initial result asked before subscription"));
                        break;
                    }
                    eventPublisher.publishEvent(new SubscriptionQueryInitialResultRequested(context,
                                                                                            subscriptionQuery.get(0),
                                                                                            updateHandler,
                                                                                            errorHandler));
                    break;
                case FLOW_CONTROL:
                    responseObserver.addPermits(message.getFlowControl().getNumberOfPermits());
                    break;
                case UNSUBSCRIBE:
                    if (!subscriptionQuery.isEmpty()) {
                        unsubscribe(subscriptionQuery.get(0));
                    }
                    break;
                default:
            }
        } catch (Exception e) {
            logger.warn("{}: Exception in consuming SubscriptionQueryRequest", context, e);
            errorHandler.accept(e);
        }
    }

    @Override
    protected String sender() {
        return clientId;
    }

    @Override
    public void onError(Throwable t) {
        if (!subscriptionQuery.isEmpty()) {
            unsubscribe(subscriptionQuery.get(0));
        }
        StreamObserverUtils.error(responseObserver, t);
    }

    @Override
    public void onCompleted() {
        if (!subscriptionQuery.isEmpty()) {
            unsubscribe(subscriptionQuery.get(0));
        }
        StreamObserverUtils.complete(responseObserver);
    }

    private void unsubscribe(SubscriptionQuery cancel) {
        subscriptionQuery.remove(cancel);
        eventPublisher.publishEvent(new SubscriptionQueryCanceled(context, cancel));
    }

    private class QueryResponseStreamObserver implements StreamObserver<SubscriptionQueryResponse> {

        private final FlowControlledStreamObserver<SubscriptionQueryResponse> delegate;

        public QueryResponseStreamObserver(
                FlowControlledStreamObserver<SubscriptionQueryResponse> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void onNext(SubscriptionQueryResponse t) {
            try {
                delegate.onNext(subscriptionQueryInterceptors.subscriptionQueryResponse(t, extensionUnitOfWork));
            } catch (Exception ex) {
                errorHandler.accept(ex);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            delegate.onError(throwable);
        }

        @Override
        public void onCompleted() {
            delegate.onCompleted();
        }

        public void addPermits(long numberOfPermits) {
            delegate.addPermits(numberOfPermits);
        }
    }
}
