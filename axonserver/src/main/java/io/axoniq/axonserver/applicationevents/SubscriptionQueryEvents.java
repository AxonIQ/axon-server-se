/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.applicationevents;

import io.axoniq.axonserver.grpc.query.SubscriptionQuery;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryResponse;
import io.axoniq.axonserver.message.query.subscription.UpdateHandler;

import java.util.function.Consumer;

import static io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest.RequestCase.UNSUBSCRIBE;


/**
 * Set of events used in handling of SubscriptionQueries.
 *
 * @author Sara Pellegrini
 */
public class SubscriptionQueryEvents {

    private SubscriptionQueryEvents() {
    }

    public static class ProxiedSubscriptionQueryRequest {

        private final SubscriptionQueryRequest request;
        private final String context;
        private final UpdateHandler handler;
        private final String targetClientStreamId;

        public ProxiedSubscriptionQueryRequest(SubscriptionQueryRequest request, String context,
                                               UpdateHandler handler, String targetClientStreamId) {
            this.request = request;
            this.context = context;
            this.handler = handler;
            this.targetClientStreamId = targetClientStreamId;
        }

        public SubscriptionQueryRequest subscriptionQueryRequest() {
            return request;
        }

        public UpdateHandler handler() {
            return handler;
        }

        public String targetClient() {
            return targetClientStreamId;
        }

        public String context() {
            return context;
        }

        public SubscriptionQuery subscriptionQuery() {
            switch (request.getRequestCase()) {
                case SUBSCRIBE:
                    return request.getSubscribe();
                case GET_INITIAL_RESULT:
                    return request.getGetInitialResult();
                case UNSUBSCRIBE:
                    return request.getUnsubscribe();
                default:
                    return null;
            }
        }

        public boolean isSubscription() {
            return !request.getRequestCase().equals(UNSUBSCRIBE);
        }
    }

    public abstract static class SubscriptionQueryRequestEvent {

        private final String context;

        private final SubscriptionQuery subscription;

        private final UpdateHandler updateHandler;

        private final Consumer<Throwable> errorHandler;


        SubscriptionQueryRequestEvent(String context, SubscriptionQuery subscription,
                                      UpdateHandler updateHandler,
                                      Consumer<Throwable> errorHandler) {
            this.context = context;
            this.subscription = subscription;
            this.updateHandler = updateHandler;
            this.errorHandler = errorHandler;
        }

        public String context() {
            return context;
        }

        public UpdateHandler handler() {
            return updateHandler;
        }

        public SubscriptionQuery subscription() {
            return subscription;
        }

        public String subscriptionId() {
            return subscription.getSubscriptionIdentifier();
        }

        public Consumer<Throwable> errorHandler() {
            return errorHandler;
        }

        public abstract SubscriptionQueryRequest subscriptionQueryRequest();
    }

    public static class SubscriptionQueryStarted extends SubscriptionQueryRequestEvent {

        public SubscriptionQueryStarted(SubscriptionQueryRequested event) {
            this(event.context(), event.subscription(), event.handler(), event.errorHandler());
        }

        public SubscriptionQueryStarted(String context, SubscriptionQuery subscription,
                                        UpdateHandler updateHandler,
                                        Consumer<Throwable> errorHandler) {
            super(context, subscription, updateHandler, errorHandler);
        }

        @Override
        public SubscriptionQueryRequest subscriptionQueryRequest() {
            return SubscriptionQueryRequest.newBuilder()
                                           .setSubscribe(subscription())
                                           .build();
        }
    }

    public static class SubscriptionQueryRequested extends SubscriptionQueryRequestEvent {

        public SubscriptionQueryRequested(String context, SubscriptionQuery subscription,
                                          UpdateHandler updateHandler,
                                          Consumer<Throwable> errorHandler) {
            super(context, subscription, updateHandler, errorHandler);
        }

        @Override
        public SubscriptionQueryRequest subscriptionQueryRequest() {
            return SubscriptionQueryRequest.newBuilder()
                                           .setSubscribe(subscription())
                                           .build();
        }
    }

    public static class SubscriptionQueryInitialResultRequested extends SubscriptionQueryRequestEvent {

        public SubscriptionQueryInitialResultRequested(String context, SubscriptionQuery subscription,
                                                       UpdateHandler updateHandler,
                                                       Consumer<Throwable> errorHandler) {
            super(context, subscription, updateHandler, errorHandler);
        }

        @Override
        public SubscriptionQueryRequest subscriptionQueryRequest() {
            return SubscriptionQueryRequest.newBuilder()
                                           .setGetInitialResult(subscription())
                                           .build();
        }
    }

    public static class SubscriptionQueryCanceled {

        private final String context;

        private final SubscriptionQuery unsubscription;

        public SubscriptionQueryCanceled(String context, SubscriptionQuery cancel) {
            this.context = context;
            this.unsubscription = cancel;
        }


        public String context() {
            return context;
        }

        public SubscriptionQuery unsubscribe() {
            return unsubscription;
        }

        public String subscriptionId() {
            return unsubscription.getSubscriptionIdentifier();
        }
    }

    public static class SubscriptionQueryResponseReceived {

        private final SubscriptionQueryResponse response;
        private final Runnable unknownSubscriptionHandler;

        public SubscriptionQueryResponseReceived(SubscriptionQueryResponse response) {
            this(response, () -> {
            });
        }

        public SubscriptionQueryResponseReceived(SubscriptionQueryResponse response,
                                                 Runnable unknownSubscriptionHandler) {

            this.response = response;
            this.unknownSubscriptionHandler = unknownSubscriptionHandler;
        }

        public SubscriptionQueryResponse response() {
            return response;
        }

        public Runnable unknownSubscriptionHandler() {
            return unknownSubscriptionHandler;
        }

        public String subscriptionId() {
            return response.getSubscriptionIdentifier();
        }
    }
}
