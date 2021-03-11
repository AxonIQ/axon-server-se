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
import io.axoniq.axonserver.message.query.subscription.UpdateHandler;
import io.axoniq.axonserver.message.query.subscription.handler.DirectUpdateHandler;
import io.axoniq.axonserver.util.StreamObserverUtils;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * @author Sara Pellegrini
 */
public class SubscriptionQueryRequestTarget extends ReceivingStreamObserver<SubscriptionQueryRequest> {

    private static final Logger logger = LoggerFactory.getLogger(SubscriptionQueryRequestTarget.class);

    private final String context;

    private final FlowControlledStreamObserver<SubscriptionQueryResponse> responseObserver;

    private final ApplicationEventPublisher eventPublisher;

    private final AtomicReference<SubscriptionQuery> subscriptionQuery = new AtomicReference<>();

    private final UpdateHandler updateHandler;

    private final Consumer<Throwable> errorHandler;

    private volatile String clientId;

    SubscriptionQueryRequestTarget(
            String context, StreamObserver<SubscriptionQueryResponse> responseObserver,
            ApplicationEventPublisher eventPublisher) {
        super(LoggerFactory.getLogger(SubscriptionQueryRequestTarget.class));
        this.context = context;
        this.errorHandler = e -> {
            responseObserver.onError(Status.INTERNAL
                                             .withDescription(e.getMessage())
                                             .withCause(e)
                                             .asRuntimeException());
            unsubscribe();
        };
        this.responseObserver = new FlowControlledStreamObserver<>(responseObserver, errorHandler);
        this.updateHandler = new DirectUpdateHandler(this.responseObserver::onNext);
        this.eventPublisher = eventPublisher;
    }

    @Override
    protected void consume(SubscriptionQueryRequest message) {
        try {
            switch (message.getRequestCase()) {
                case SUBSCRIBE:
                    if (clientId == null) {
                        clientId = message.getSubscribe().getQueryRequest().getClientId();
                    }
                    if (subscriptionQuery.compareAndSet(null, message.getSubscribe())) {
                        eventPublisher.publishEvent(new SubscriptionQueryRequested(context,
                                                                                   subscriptionQuery.get(),
                                                                                   updateHandler,
                                                                                   errorHandler));
                    }

                    break;
                case GET_INITIAL_RESULT:
                    if (subscriptionQuery.get() == null) {
                        errorHandler.accept(new IllegalStateException("Initial result asked before subscription"));
                        break;
                    }
                    eventPublisher.publishEvent(new SubscriptionQueryInitialResultRequested(context,
                                                                                            subscriptionQuery.get(),
                                                                                            updateHandler,
                                                                                            errorHandler));
                    break;
                case FLOW_CONTROL:
                    responseObserver.addPermits(message.getFlowControl().getNumberOfPermits());
                    break;
                case UNSUBSCRIBE:
                    unsubscribe();
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
        unsubscribe();
        StreamObserverUtils.error(responseObserver, t);
    }

    @Override
    public void onCompleted() {
        unsubscribe();
        StreamObserverUtils.complete(responseObserver);
    }

    private void unsubscribe() {
        SubscriptionQuery query = this.subscriptionQuery.get();
        if (query != null) {
            this.subscriptionQuery.set(null);
            eventPublisher.publishEvent(new SubscriptionQueryCanceled(context, query));
        }
    }
}
